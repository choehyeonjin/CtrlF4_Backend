import os, json, logging, psycopg2
from psycopg2.extras import Json
import boto3
import google.generativeai as genai
from services.utils import trigger_run_tick

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

WORKER_LAMBDA_MAP = {
    "risk": os.getenv("L_RISK", "wk-risks"),
    "qa": os.getenv("L_QA", "wk-qa"),
    "revision": os.getenv("L_REVISION", "wk-revisions"),
    "summarizer": os.getenv("L_SUMMARIZER", "SummarizerWorker"),
}

MAX_ATTEMPT = 2
ANCHOR_RATE_THRESHOLD = 0.4
KPRI_THRESHOLD = 0.3
FAITHFULNESS_THRESHOLD = 0.5

########## DB ##########
def db():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=os.environ.get("PGPORT","5432"),
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
        connect_timeout=5
    )

def upsert_result(cur, run_id: int, payload: dict):
    cur.execute("""
      INSERT INTO run_results (worker_type, run_id, payload)
      VALUES ('verifier', %s, %s::jsonb)
      ON CONFLICT (worker_type, run_id)
      DO UPDATE SET payload = EXCLUDED.payload;
    """, (run_id, Json(payload)))

def get_run_and_results(cur, run_id: int):
    """
    runs 에서 plan/attempt + session/doc 을 같이 가져오고,
    run_results 전체를 dict 로 만들어서 반환.
    """
    cur.execute("""
        SELECT id, plan, attempt, session_id, doc_id
          FROM runs
         WHERE id=%s
    """, (run_id,))
    r = cur.fetchone()
    if not r:
        raise RuntimeError(f"run not found: {run_id}")

    run = {
        "id": r[0],
        "plan": r[1] or [],
        "attempt": r[2] or 0,
        "session_id": r[3],
        "doc_id": r[4],
    }

    cur.execute("SELECT worker_type, payload FROM run_results WHERE run_id=%s", (run_id,))
    results = {}
    for w, p in cur.fetchall():
        results[w] = p if isinstance(p, dict) else json.loads(p)
    return run, results

##########################################
# 1) Deterministic Metric 계산 로직      #
##########################################

def calc_anchor_rate(results: dict) -> float:
    """
    summarizer, risk, qa 결과에서 anchor 매칭 비율 계산
    """
    try:
        sm = results.get("summarizer", {})
        clauses = sm.get("results", {}).get("by_clause", {})
        summarizer_clause_cnt = len(clauses)

        rk = results.get("risk", {})
        risk_items = rk.get("items", [])
        risk_anchors = [r.get("id") for r in risk_items if r.get("id")]

        if summarizer_clause_cnt == 0:
            return 1.0

        rate = len(risk_anchors) / summarizer_clause_cnt
        return round(max(0.0, min(rate, 1.0)), 4)
    except Exception as e:
        log.warning(f"anchorRate calc fail: {e}")
        return 0.5

def calc_kpri(results: dict) -> float:
    """
    QA 답변의 근거 anchoring 존재 여부를 기반으로 계산
    """
    try:
        qa = results.get("qa", {})
        anchors = qa.get("anchors", [])
        if not anchors:
            return 0.3
        return round(min(1.0, len(anchors) / 4), 4)
    except Exception as e:
        log.warning(f"kpri calc fail: {e}")
        return 0.3

def calc_faithfulness(results: dict) -> float:
    """
    summarizer summary와 원문 anchor overlap 기반 계산 (간이)
    """
    try:
        sm = results.get("summarizer", {})
        full = sm.get("results", {}).get("full_document", {}).get("summary", "")
        if not full:
            return 0.3

        if len(full) > 500:
            return 0.85
        return 0.70
    except Exception as e:
        log.warning(f"faithfulness calc fail: {e}")
        return 0.5


##########################################
# 2) LLM 기반 Expert 검증 (Meta-verdict) #
##########################################

genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel(os.environ.get("GEMINI_MODEL","gemini-2.0-flash"))

def llm_verdict(worker_results_with_metrics: dict) -> dict:
    """
    입력: {
      "workers": {...},
      "metrics": {...},
      "metricFlags": {...},
      "suggestedRetryWorkers": [...],
      "attempt": int
    }

    출력 예:
    {
      "verdict": "pass" | "retry",
      "reason": "...",
      "retryWorkers": ["summarizer", "risk"],
      "metricReasons": {
        "anchorRate": "...",
        "kpri": "...",
        "faithfulness": "..."
      },
      "perWorkerFeedback": {
        "summarizer": "...",
        "risk": "...",
        "qa": "...",
        "revision": "..."
      }
    }
    """
    prompt = """
너는 문서 분석 품질을 검증하는 **Verifier Agent**다.

입력은 모든 worker의 결과 + 계산된 metrics + metricFlags + suggestedRetryWorkers 이다.

metrics:
- anchorRate: 계산된 앵커 정합률
- kpri: 키포인트 관련성 지표
- faithfulness: 요약 충실도 지표

metricFlags:
- anchorRateLow: anchorRate < 기준값 인지 여부
- kpriLow: kpri < 기준값 인지 여부
- faithfulnessLow: faithfulness < 기준값 인지 여부

suggestedRetryWorkers:
- 지표 기준에 따라 코드에서 추천한 retry 후보 워커 목록이다.
- 이 중에서 진짜 retry가 필요한 worker만 골라라.

가능한 worker 이름(반드시 이 중에서만 사용):
- "summarizer", "risk", "qa", "revision"

반드시 아래 JSON 형식 **ONLY** 로 답하라:

{
  "verdict": "pass" | "retry",
  "reason": "왜 pass인지 / 왜 retry인지, 어떤 지표가 문제인지 요약",
  "retryWorkers": ["summarizer", "risk", ...],
  "metricReasons": {
    "anchorRate": "현재 값과 기준을 비교해서 왜 retry 또는 pass 인지 설명",
    "kpri": "동일",
    "faithfulness": "동일"
  },
  "perWorkerFeedback": {
    "summarizer": "summarizer를 다시 돌릴 때 어떤 부분을 보완해야 하는지 (없으면 빈 문자열)",
    "risk": "risk worker 관점에서 어떤 위험/앵커 부분을 보완해야 하는지 (없으면 빈 문자열)",
    "qa": "qa worker를 돌린다면 어떤 점을 개선해야 하는지 (없으면 빈 문자열)",
    "revision": "revision worker를 돌린다면 어떤 식으로 수정 문안을 더 보완해야 하는지 (없으면 빈 문자열)"
  }
}
"""
    user = json.dumps(worker_results_with_metrics, ensure_ascii=False)

    res = model.generate_content(prompt + "\n\n입력(JSON):\n" + user)
    txt = res.text or ""

    if txt.startswith("```"):
        txt = txt.strip("`")
        nl = txt.find("\n")
        if nl != -1:
            txt = txt[nl+1:].strip()

    try:
        out = json.loads(txt)
    except Exception as e:
        log.warning(f"llm_verdict JSON parse fail, fallback pass: {e}")
        return {
            "verdict": "pass",
            "reason": "fallback: JSON parse 실패",
            "retryWorkers": [],
            "metricReasons": {},
            "perWorkerFeedback": {}
        }

    # 방어적으로 기본값 보정
    if not isinstance(out.get("retryWorkers"), list):
        out["retryWorkers"] = []
    if not isinstance(out.get("metricReasons"), dict):
        out["metricReasons"] = {}
    if not isinstance(out.get("perWorkerFeedback"), dict):
        out["perWorkerFeedback"] = {}

    return out

##########################################
# 3) auto retry                          #
##########################################

def auto_retry(cur, run: dict, metrics: dict, vout: dict):
    run_id = run["id"]

    if run.get("attempt", 0) >= MAX_ATTEMPT:
        log.warning(f"[auto_retry] attempt limit reached (>= {MAX_ATTEMPT}), skip retry for run {run_id}")
        return None

    session_id = run["session_id"]
    doc_id = run["doc_id"]
    retry_workers = vout.get("retryWorkers", []) or []

    if not retry_workers:
        log.info(f"[verifier] no retryWorkers specified for run {run_id}")
        return None

    cur.execute("""
        UPDATE runs
           SET attempt = COALESCE(attempt, 0) + 1,
               status  = 'running'
         WHERE id = %s
     RETURNING attempt
    """, (run_id,))
    new_attempt_row = cur.fetchone()
    new_attempt = new_attempt_row[0] if new_attempt_row else (run.get("attempt", 0) + 1)

    # ✅ 기존 구조 유지 + 필드만 추가
    retry_info = {
        "from": "verifier",
        "attempt": new_attempt,
        "prevAttempt": run.get("attempt", 0),
        "reason": vout.get("reason"),
        "metrics": metrics,
        # NEW (워커가 써도 되고, 안 써도 됨)
        "metricReasons": vout.get("metricReasons") or {},
        "perWorkerFeedback": vout.get("perWorkerFeedback") or {},
    }

    log.info(f"[verifier] auto-retry run {run_id} attempt={new_attempt} workers={retry_workers}")

    for w in retry_workers:
        cur.execute("""
            INSERT INTO run_results (worker_type, run_id, payload)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (worker_type, run_id)
            DO UPDATE SET payload = EXCLUDED.payload
        """, (
            w,
            run_id,
            json.dumps({"status": "queued", "retryInfo": retry_info}, ensure_ascii=False),
        ))

    cur.execute("""
        INSERT INTO run_results (worker_type, run_id, payload)
        VALUES ('verifier', %s, %s::jsonb)
        ON CONFLICT (worker_type, run_id)
        DO UPDATE SET payload = EXCLUDED.payload
    """, (
        run_id,
        json.dumps({
            "status": "waiting",
            "verdict": "retry",
            **metrics,
            "reason": vout.get("reason"),
            "retryWorkers": retry_workers,
            "attempt": new_attempt,
            "metricReasons": vout.get("metricReasons") or {},
            "perWorkerFeedback": vout.get("perWorkerFeedback") or {},
        }, ensure_ascii=False),
    ))

    return retry_info

##########################################
# 4) Lambda handler                      #
##########################################

def lambda_handler(event, context):
    run_id = None
    try:
        run_id = event.get("runId") or event.get("run_id")
        if not run_id:
            raise ValueError("runId required")

        run_id = int(run_id)
        log.info(f"[verifier] start run_id={run_id}")

        with db() as conn, conn.cursor() as cur:
            # load
            run, results = get_run_and_results(cur, run_id)
            log.info(f"[verifier] run={run}, results_keys={list(results.keys())}")

            # 1) metrics 계산
            anchor_rate = calc_anchor_rate(results)
            kpri = calc_kpri(results)
            faith = calc_faithfulness(results)

            metrics = {
                "anchorRate": anchor_rate,
                "kpri": kpri,
                "faithfulness": faith
            }
            metric_flags = {
                "anchorRateLow": anchor_rate < ANCHOR_RATE_THRESHOLD,
                "kpriLow": kpri < KPRI_THRESHOLD,
                "faithfulnessLow": faith < FAITHFULNESS_THRESHOLD
            }

            log.info(f"[verifier] metrics: {metrics}, flags: {metric_flags}")

            # 2) LLM에 worker_results + metrics 함께 전달
            plan_workers = run["plan"] or []
            if not plan_workers:
                # plan 없으면 run_results 의 키 기반
                plan_workers = [k for k in results.keys() if k not in ("verifier", "__plan__")]

            worker_results = {w: results.get(w) for w in plan_workers}
            merged = {
                "workers": worker_results,
                "metrics": metrics,
                "metricFlags": metric_flags, # [추가] LLM에게 기준 통과 여부를 명시적으로 전달
                "attempt": run.get("attempt", 0),
            }

            vout = llm_verdict(merged)
            log.info(f"[verifier] LLM verdict for run {run_id}: {vout}")

            ##########################################
            # NEW: Safety guard for max retry
            ##########################################
            
            current_attempt = run.get("attempt", 0)

            if current_attempt >= MAX_ATTEMPT:
                log.warning(f"[verifier] run {run_id} reached MAX_ATTEMPT={MAX_ATTEMPT}, force PASS")

                upsert_result(cur, run_id, {
                    "status": "done",
                    "verdict": "pass",
                    **metrics,
                    "reason": f"retry limit {MAX_ATTEMPT} reached - force pass",
                    "retryWorkers": []
                })
                conn.commit()
                trigger_run_tick(run_id)
                return {"ok": True, "verdict": "pass"}

            verdict = vout.get("verdict", "pass")

            # 3) retry이면 auto retry
            if verdict == "retry":
                # DB 내 verifier 결과/attempt/queued 세팅 + retryInfo 구성
                retry_info = auto_retry(cur, run, metrics, vout)
                conn.commit()

                # 커밋 후, 실제로 워커 Lambda 재실행
                if retry_info:
                    retry_workers = vout.get("retryWorkers", []) or []
                    for w in retry_workers:
                        payload = {
                            "runId": run_id,
                            "docId": run["doc_id"],
                            "sessionId": run["session_id"],
                            "worker": w,
                            "inputs": {
                                "retryInfo": retry_info
                            },
                        }
                        fn_name = WORKER_LAMBDA_MAP.get(w, os.getenv(f"L_{w.upper()}", f"wk-{w}"))
                        try:
                            lambda_client.invoke(
                                FunctionName=fn_name,
                                InvocationType="Event",
                                Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                            )
                            log.info(f"[verifier] invoked retry worker={w} fn={fn_name} runId={run_id}")
                        except Exception as e:
                            log.warning(f"[verifier] auto retry invoke fail: worker={w} runId={run_id} err={e}")

                trigger_run_tick(run_id)
                return {"ok": True, "verdict": "retry"}

            # 4) pass → 완료
            upsert_result(cur, run_id, {
                "status": "done",
                "verdict": "pass",
                **metrics,
                "reason": vout.get("reason"),
                "retryWorkers": []
            })
            conn.commit()
            trigger_run_tick(run_id)

            log.info(f"[verifier] PASS run_id={run_id}")
            return {"ok": True, "verdict": "pass"}

    except Exception as e:
        log.exception("verifier failed")
        try:
            if run_id is not None:
                with db() as conn, conn.cursor() as cur:
                    upsert_result(cur, run_id, {"status": "failed", "error": str(e)})
                    conn.commit()
                    trigger_run_tick(run_id)
        except Exception:
            pass
        return {"ok": False, "error": str(e)}
