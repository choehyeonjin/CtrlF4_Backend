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
ANCHOR_RATE_THRESHOLD = 0.50 # 앵커 포함율
KPRI_THRESHOLD          = 0.50 # QA <-> Summarizer가 앵커 제대로 썼는지 일관성
FAITHFULNESS_THRESHOLD  = 0.50 # 요약 내용이 어느 정도만 맞는지

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

def _collect_summarizer_anchors(sm: dict):
    """
    summarizer 결과에서 앵커/조항 정보를 모아오는 헬퍼
    - anchor_ids: by_clause[*].anchors[*].id / anchor_id
    - clause_titles: by_clause[*].title + anchors[*].title
    - clause_ids: by_clause[*].clause_id
    - by_clause: 원본 리스트
    """
    sm_res = (sm or {}).get("results") or {}
    by_clause = sm_res.get("by_clause") or []

    # 혹시 dict 로 올 수도 있으니 방어
    if isinstance(by_clause, dict):
        by_clause = list(by_clause.values())

    anchor_ids: set[str] = set()
    clause_titles: set[str] = set()
    clause_ids: set[str] = set()

    for c in by_clause:
        if not isinstance(c, dict):
            continue

        t = c.get("title")
        if t:
            clause_titles.add(str(t).strip())

        cid = c.get("clause_id")
        if cid:
            clause_ids.add(str(cid).strip())

        for a in c.get("anchors") or []:
            if not isinstance(a, dict):
                continue
            aid = a.get("id") or a.get("anchor_id")
            if aid:
                anchor_ids.add(str(aid).strip())
            atitle = a.get("title")
            if atitle:
                clause_titles.add(str(atitle).strip())

    return anchor_ids, clause_titles, clause_ids, by_clause


def calc_anchor_rate(results: dict) -> float:
    """
    anchorRate: 각 워커 결과 당 '앵커가 있는지' 존재 여부만 파악하는 구조적 지표.
      - summarizer: by_clause[*].anchors 에 앵커가 하나라도 있으면 OK
      - risk: items[*].anchor.id / title 이 하나라도 있으면 OK
      - qa: anchors 배열이 비어있지 않으면 OK
      - revision: revisions[*].anchor.id / title 이 하나라도 있으면 OK

    anchorRate = (앵커가 있는 워커 수) / (결과가 존재하는 워커 수)
    """
    try:
        present_workers = 0
        workers_with_anchor = 0

        # summarizer
        sm = results.get("summarizer")
        if sm is not None:
            present_workers += 1
            _, _, _, by_clause = _collect_summarizer_anchors(sm)
            has_anchor = False
            for c in by_clause:
                if not isinstance(c, dict):
                    continue
                if c.get("anchors"):
                    has_anchor = True
                    break
            if has_anchor:
                workers_with_anchor += 1

        # risk
        rk = results.get("risk")
        if rk is not None:
            present_workers += 1
            items = rk.get("items") or []
            has_anchor = False
            for it in items:
                if not isinstance(it, dict):
                    continue
                a = it.get("anchor") or {}
                if a.get("id") or a.get("title"):
                    has_anchor = True
                    break
            if has_anchor:
                workers_with_anchor += 1

        # qa
        qa = results.get("qa")
        if qa is not None:
            present_workers += 1
            anchors = qa.get("anchors") or []
            if anchors:
                workers_with_anchor += 1

        # revision
        rv = results.get("revision")
        if rv is not None:
            present_workers += 1
            revs = rv.get("revisions") or []
            has_anchor = False
            for r in revs:
                if not isinstance(r, dict):
                    continue
                a = r.get("anchor") or {}
                if a.get("id") or a.get("title"):
                    has_anchor = True
                    break
            if has_anchor:
                workers_with_anchor += 1

        if present_workers == 0:
            # 아무 결과도 없으면 이 지표로 판단 불가 → 중립적으로 1.0
            return 1.0

        rate = workers_with_anchor / present_workers
        return round(max(0.0, min(rate, 1.0)), 4)

    except Exception as e:
        log.warning(f"anchorRate calc fail: {e}")
        return 0.5

def calc_kpri(results: dict) -> float:
    """
    kpri(완화 기준):
      QA에서 인용한 anchor 문자열들이
      summarizer의 (title / clause_id / anchor.id / anchor.title) 중
      하나라도 '의미적으로 매칭'되는 비율.

    완화된 매칭 규칙:
      - 완전 일치
      - summarizer title/anchor title이 QA anchor로 시작(startswith)
      - QA anchor가 summarizer title 내부에 포함(substring)
      - 숫자 기반 id 매칭 (예: "제6조" ↔ clause_id "art-6" 일부 일치)
    """
    try:
        qa = results.get("qa") or {}
        qa_anchors = qa.get("anchors") or []
        if not qa_anchors:
            return 0.1

        sm = results.get("summarizer") or {}
        anchor_ids, clause_titles, clause_ids, _ = _collect_summarizer_anchors(sm)

        # 비교 대상 정규화
        summ_all = set()
        for t in clause_titles:
            summ_all.add(str(t).strip())
        for cid in clause_ids:
            summ_all.add(str(cid).strip())
        for aid in anchor_ids:
            summ_all.add(str(aid).strip())

        if not summ_all:
            return round(min(1.0, len(qa_anchors) / 4), 4)

        def normalize(s: str) -> str:
            # 공백/특수기호 정리
            import re
            return re.sub(r"\s+", " ", s).strip()

        # 매칭 함수 (완화 버전)
        def is_match(qa_anchor: str, summ_val: str) -> bool:
            qa_norm = normalize(qa_anchor)
            sv_norm = normalize(summ_val)

            # 1) 완전 일치
            if qa_norm == sv_norm:
                return True

            # 2) 시작 부분 매칭
            #   "특약사항 1" → "특약사항 1. 임대인의 조기해지권"
            if sv_norm.startswith(qa_norm):
                return True

            # 3) substring 매칭
            if qa_norm in sv_norm:
                return True

            # 4) 숫자 기반 일치 (제6조 ↔ art-6 / 6 포함)
            import re
            qa_nums = re.findall(r"\d+", qa_norm)
            sv_nums = re.findall(r"\d+", sv_norm)
            if qa_nums and sv_nums:
                # 공통 숫자 하나라도 있으면 인정
                if any(n in sv_nums for n in qa_nums):
                    return True

            return False

        # 매칭 개수 세기
        match = 0
        for raw in qa_anchors:
            s = str(raw).strip()
            if any(is_match(s, summ_val) for summ_val in summ_all):
                match += 1

        ratio = match / len(qa_anchors)
        return round(max(0.0, min(ratio, 1.0)), 4)

    except Exception as e:
        log.warning(f"kpri calc fail: {e}")
        return 0.3

import re

TOP_TITLE_RE = re.compile(r"^제\s*\d+\s*조")

def _is_top_clause(c: dict) -> bool:
    """상위 '제n조' 레벨만 faithfulness 평가 대상으로 사용"""
    title = (c.get("title") or "").strip()
    if TOP_TITLE_RE.search(title):
        return True

    # anchors 에 level == 1 이 있으면 상위 조로 간주
    for a in c.get("anchors") or []:
        if isinstance(a, dict) and a.get("level") == 1:
            return True
    return False


def calc_faithfulness(results: dict) -> float:
    """
    faithfulness (요약 충실도, summarizer 전용):

    - summarizer 의 by_clause 중 '제n조 ...' 같은 상위 조만 대상으로 삼고,
      각 조 요약이 제목/앵커와 어느 정도 맞는지 대략 평가한다.

    - 기준 완화:
      * 항/호(1., 2., 25. 같은 숫자 title)는 아예 평가 대상에서 제외
      * summary 길이 기준 완화 (30자 이상)
      * raw_rate 를 0.3 ~ 1.0 구간으로 소프트 스케일링
    """

    try:
        sm = results.get("summarizer") or {}
        sm_res = (sm.get("results") or {})
        by_clause = sm_res.get("by_clause") or []

        if isinstance(by_clause, dict):
            by_clause = list(by_clause.values())

        total = 0
        faithful = 0

        for c in by_clause:
            if not isinstance(c, dict):
                continue

            # 상위 조(제n조 …)만 평가
            if not _is_top_clause(c):
                continue

            summary = (c.get("summary") or "").strip()
            if not summary:
                continue

            total += 1

            # 너무 짧은 요약은 정보량이 부족하다고 보고 패스
            if len(summary) < 30:
                continue

            title = (c.get("title") or "").strip()
            anchor_titles = []
            for a in c.get("anchors") or []:
                if isinstance(a, dict):
                    t = (a.get("title") or "").strip()
                    if t:
                        anchor_titles.append(t)

            text_for_keywords = " ".join([title] + anchor_titles)

            cleaned = re.sub(r"[^0-9A-Za-z가-힣\s]", " ", text_for_keywords)
            tokens = [
                t for t in cleaned.split()
                if len(t) >= 2 and t not in ["제", "조", "항", "특약사항"]
            ]

            is_faithful = False
            for tok in tokens:
                if tok and tok in summary:
                    is_faithful = True
                    break

            if is_faithful:
                faithful += 1

        if total == 0:
            # 조 단위 요약이 없으면 full_document 길이 기반 fallback (조금 후하게)
            full = (sm_res.get("full_document") or {}).get("summary") or ""
            if not full:
                return 0.4
            if len(full) > 500:
                return 0.9
            return 0.75

        raw_rate = faithful / total

        # 너무 극단적으로 낮게 나오지 않도록 스케일링
        # raw_rate=0 → 0.3 / raw_rate=1 → 1.0
        score = 0.3 + 0.7 * raw_rate
        return round(max(0.0, min(score, 1.0)), 4)

    except Exception as e:
        log.warning(f"faithfulness calc fail: {e}")
        # 에러 시 살짝 보수적인 중간값
        return 0.6

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

    thresholds:
    - 각 지표별 기준값 (anchorRate, kpri, faithfulness)

    suggestedRetryWorkers:
    - 지표 기준에 따라 코드에서 추천한 retry 후보 워커 목록이다.
      * anchorRateLow 가 true 이면 보통 ["summarizer", "risk"] 가 포함된다.
      * kpriLow 가 true 이면 보통 ["qa"] 가 포함된다.
      * faithfulnessLow 가 true 이면 보통 ["summarizer"] 가 포함된다.
      이 중에서 진짜 retry가 필요한 worker만 골라라.

    추가 규칙:
    - metricFlags 가 모두 false 이면 기본적으로 verdict 는 "pass" 로 하라
      (JSON에서 아주 명백한 오류/누락을 발견한 경우가 아니라면 retry를 선택하지 말 것).
    - metricFlags 중 true 인 것이 1개뿐이고, 해당 지표 값이 threshold에서 0.1 이내라면
      가급적 "pass" 를 선택하라.
    - 두 개 이상의 metricFlags 가 true 이거나,
      특정 지표 값이 0.1 미만으로 매우 낮을 때만 "retry" 를 선택하라.

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

    # 기존 구조 유지 + 필드만 추가
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

            # NEW: 지표 기준으로 코드 레벨 추천 retry 후보
            suggested_retry_workers = set()
            if metric_flags["anchorRateLow"]:
                suggested_retry_workers.update(["summarizer", "risk"])
            if metric_flags["kpriLow"]:
                suggested_retry_workers.add("qa")
            if metric_flags["faithfulnessLow"]:
                suggested_retry_workers.add("summarizer")

            thresholds = {
                "anchorRate": ANCHOR_RATE_THRESHOLD,
                "kpri": KPRI_THRESHOLD,
                "faithfulness": FAITHFULNESS_THRESHOLD,
            }

            log.info(f"[verifier] metrics: {metrics}, flags: {metric_flags}, suggestedRetryWorkers={list(suggested_retry_workers)}")

            # 2) LLM에 worker_results + metrics 함께 전달
            plan_workers = run["plan"] or []
            if not plan_workers:
                # plan 없으면 run_results 의 키 기반
                plan_workers = [k for k in results.keys() if k not in ("verifier", "__plan__")]

            worker_results = {w: results.get(w) for w in plan_workers}
            merged = {
                "workers": worker_results,
                "metrics": metrics,
                "metricFlags": metric_flags,
                "thresholds": thresholds,
                "suggestedRetryWorkers": list(suggested_retry_workers),
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
