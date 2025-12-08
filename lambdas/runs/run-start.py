import os, json, logging, datetime
import psycopg2, boto3, requests
import jwt
from jwt import InvalidTokenError

class AuthError(Exception):
    pass

def get_auth_user_id(event) -> int:
    """
    Authorization: Bearer <access_token> 헤더에서 user_id(sub) 추출
    - 토큰 타입(type) == 'access' 체크
    """
    headers = (event.get("headers") or {})
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    if not auth.startswith("Bearer "):
        raise AuthError("missing bearer token")

    token = auth.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, os.environ["JWT_SECRET_KEY"], algorithms=["HS256"])
    except jwt.InvalidTokenError as e:
        raise AuthError(f"invalid token: {e}")

    if payload.get("type") != "access":
        raise AuthError("invalid token type")

    return int(payload["sub"])

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
LAMBDA = boto3.client("lambda", region_name=AWS_REGION)

# ---------- CORS ----------
def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Content-Type": "application/json",
    }

# ---------- 공통 유틸 ----------
def need(n: str) -> str:
    v = os.getenv(n)
    if not v:
        raise RuntimeError(f"Missing env {n}")
    return v

def db():
    return psycopg2.connect(
        host=need("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=need("PGDATABASE"),
        user=need("PGUSER"),
        password=need("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5,
    )

# ---------- 스키마 보장 ----------
def ensure_runs(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
      id          SERIAL PRIMARY KEY,
      user_id     INT,
      session_id  INT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      base_run_id INT,
      doc_id      INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      status      VARCHAR(16) NOT NULL DEFAULT 'queued',
      plan        JSONB,
      progress    INT NOT NULL DEFAULT 0,
      attempt     INT NOT NULL DEFAULT 1,
      started_at  TIMESTAMPTZ DEFAULT NOW(),
      finished_at TIMESTAMPTZ
    );
    """)

# ---------- 세션/문서 정보 ----------
def get_session_row(cur, session_id: int):
    cur.execute("""
      SELECT s.id, s.user_id, s.doc_id, s.role, s.answers
      FROM sessions s
      WHERE s.id=%s
      LIMIT 1;
    """, (session_id,))
    r = cur.fetchone()
    if not r:
        return None
    return {
        "id": r[0],
        "user_id": r[1],
        "doc_id": r[2],
        "role": r[3] or "",
        "answers": r[4] or {},
    }

def get_document_name(cur, doc_id: int):
    cur.execute("SELECT name FROM documents WHERE id=%s;", (doc_id,))
    r = cur.fetchone()
    return r[0] if r else None

def get_doc_stats(cur, doc_id: int):
    cur.execute("""
      SELECT COUNT(*) AS chunks,
             COALESCE(
               SUM(
                 CASE
                   WHEN jsonb_typeof(anchors) = 'array' THEN jsonb_array_length(anchors)
                   ELSE 0
                 END
               ), 0
             ) AS anchors_total
      FROM documents_chunks
      WHERE doc_id=%s;
    """, (doc_id,))
    r = cur.fetchone()
    chunks = int(r[0] or 0)
    anchors_total = int(r[1] or 0)

    cur.execute("""
      SELECT meta->>'key'
      FROM documents_chunks
      WHERE doc_id=%s
      ORDER BY chunk_idx
      LIMIT 1;
    """, (doc_id,))
    r = cur.fetchone()
    s3_key = r[0] if r and r[0] else None

    cur.execute("""
      WITH elems AS (
        SELECT jsonb_array_elements(anchors) AS a
        FROM documents_chunks
        WHERE doc_id=%s
        AND jsonb_typeof(anchors) = 'array'
      )
      SELECT DISTINCT a->>'title' AS title
      FROM elems
      WHERE a ? 'title'
      AND a->>'title' IS NOT NULL
      LIMIT 10;
    """, (doc_id,))
    first_titles = [row[0] for row in cur.fetchall()]

    return {
        "chunks": chunks,
        "anchors_total": anchors_total,
        "s3_text_key": s3_key,
        "first_titles": first_titles,
    }

def get_chunk_snippets(cur, doc_id:int, limit:int=6, max_chars:int=200):
    cur.execute("""
      SELECT LEFT(content, %s)
      FROM documents_chunks
      WHERE doc_id=%s
      ORDER BY chunk_idx
      LIMIT %s;
    """, (max_chars, doc_id, limit))
    return [row[0] for row in cur.fetchall() if row and row[0]]

# ---------- 계획 생성 ----------
import google.generativeai as genai

def get_plan_from_gemini(role: str, question: str, focus_list, stats: dict,
                         anchors_sample: list[str], chunk_snippets: list[str]):
    """
    문서 메타데이터를 기반으로 어떤 워커(summarizer, risk, qa, revision)를 실행할지 판단.
    - 최소 기준은 아래 규칙을 항상 만족시키도록 강제한다.

      1) summarizer : 항상 실행
      2) risk       : 항상 실행
      3) qa         : question 과 focus 가 둘 다 비어있지 않을 때만 실행
      4) revision   : 아래 둘 중 하나라도 만족하면 실행
         - role 에 '작성/수정/검토/법무/리스크/컴플라이언스' 등 작성·검토 역할이 포함
         - question 텍스트에 '수정/수정안/고쳐/바꿔/대체/다듬/문구 제안/템플릿/다른 표현' 등
           수정·대체 표현을 요구하는 키워드가 포함
         - focus 는 revision 실행 여부에 영향을 주지 않음
    """
    # focus_list 정규화
    if isinstance(focus_list, str):
        focus_list = [focus_list]
    focus_list = focus_list or []

    model_plan = None

    try:
        # ── 1) Gemini 호출 (참고용) ─────────────────────────
        genai.configure(api_key=need("GEMINI_API_KEY"))
        model = genai.GenerativeModel(os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite"))

        anchors_lines  = "\n".join(f"- {t}" for t in anchors_sample[:10]) or "- (없음)"
        snippets_lines = "\n".join(f"- {s}" for s in chunk_snippets[:10]) or "- (없음)"
        focus_str = ", ".join(focus_list) or "(없음)"

        prompt = f"""
너는 계약서 분석을 담당하는 오케스트레이터 AI다.
입력으로 계약서의 일부 내용(앵커 제목, 조항 일부)과 사용자의 요구(role, question, focus)이 주어진다.
주어진 문서의 성격과 사용자 요구를 고려해, 어떤 워커들을 실행해야 할지 JSON 배열 형식으로만 반환하라.

가능한 워커들은 다음 네 가지이다: 
1. "summarizer" : 문서 전체 및 조항별 요약 
2. "risk"       : 6대 핵심 리스크 탐지 (법률·환불·개인정보·변경·책임·라이선스) 
3. "qa"         : 사용자의 질문(question, focus)에 대한 질의응답 
4. "revision"   : 탐지된 리스크 조항에 대한 수정·대체 문안 제안

### 입력 정보
- 역할(role): {role or "(없음)"}
- 사용자 질문(question): {question or "(없음)"}
- 주요 초점(focus): {focus_str}

### 문서 요약 정보
- 총 청크 수(chunks): {stats.get("chunks", 0)}
- 추출된 조항 수(anchors_total): {stats.get("anchors_total", 0)}
- 대표 조항 제목들:
{anchors_lines}

- 조항 내용 일부(요약 스니펫):
{snippets_lines}

### 최소 판단 기준 (반드시 지켜야 할 규칙)
- "summarizer" 와 "risk" 는 반드시 포함하라.
- question 과 focus 가 둘 다 비어 있으면 "qa" 는 절대로 포함하지 마라.
- question 또는 focus 중 하나라도 비어 있지 않다면 "qa" 를 포함하는 것이 기본이다.
- 다음 중 하나라도 만족하면 반드시 "revision" 을 포함하라:
  - 역할(role)에 "작성", "작성자", "수정", "검토", "법무", "리스크", "컴플라이언스" 등
    문서를 작성/수정/검토하는 역할이 포함된 경우
  - 사용자 질문(question)에 다음과 같은 수정 의도 키워드가 포함된 경우:
    "수정", "수정안", "고쳐", "바꿔", "바꾸", "대체", "다듬", "표현 바꿔",
    "문구 바꿔", "문구 제안", "대체 표현", "대체 문구", "템플릿", "다른 표현",
    "더 안전한 표현"
  - focus 는 "revision" 실행 여부에 영향을 주지 않음
- 문서를 작성하거나 검토하지 않고 단순히 내용을 이해하려는 역할이라면,
  그리고 question 에 수정 관련 키워드가 없다면 "revision" 을 포함하지 않아도 된다.

### 출력 형식 (JSON 배열만 반환)
예:
["summarizer", "risk", "qa"]
""".strip()

        res = model.generate_content(prompt)
        raw = (res.text or "").strip()

        # 코드블럭 제거
        if raw.startswith("```"):
            raw = raw.strip("`").split("\n", 1)[-1].strip()

        # JSON 배열 부분만 추출
        start, end = raw.find("["), raw.rfind("]")
        plan_json = raw[start:end+1] if start != -1 and end != -1 else raw
        parsed = json.loads(plan_json)

        if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
            model_plan = parsed

    except Exception as e:
        log.warning(f"[Gemini fallback] {e}")
        # model_plan 이 없어도 됨. 어차피 아래에서 규칙으로 확정할 것.

    # ── 2) 최소 기준 규칙으로 최종 plan 강제 ────────────────
    q = (question or "").strip()
    role_text = role or ""

    has_focus = any(str(f or "").strip() for f in focus_list)

    # revision 조건 키워드
    revision_keywords_role = [
        "작성", "작성자", "수정", "검토",
        "법무", "리스크", "컴플라이언스","법무팀",
    ]
    revision_keywords_question = [
        "수정", "수정안", "수정해", "수정해줘",
        "고쳐", "고쳐줘", "고쳐서",
        "바꿔", "바꿔줘", "바꾸", "바뀌",
        "대체", "대체해", "대체 표현", "대체 문구",
        "다듬", "다듬어", "표현 바꿔", "문구 바꿔",
        "문구 제안", "다른 표현", "다른 문구",
        "더 안전한 표현", "템플릿", "템플릿으로",
        "나은 표현", "낫게", "오탈자", "오타", "길이",
    ]

    final: list[str] = []

    # 1) summarizer : 항상
    final.append("summarizer")

    # 2) risk : 항상
    final.append("risk")

    # 3) qa : question 과 focus 둘 다 비어 있으면 절대 실행하지 않음
    if q or has_focus:
        final.append("qa")

    # 4) revision : role 또는 question 키워드로 판단
    want_revision = False

    # 4-1) role 기반
    if any(k in role_text for k in revision_keywords_role):
        want_revision = True

    # 4-2) question 텍스트에 수정 의도 키워드가 하나라도 있으면 무조건 실행
    if any(k in q for k in revision_keywords_question):
        want_revision = True

    if want_revision:
        final.append("revision")

    # 중복 제거 + 순서 유지
    seen = set()
    deduped: list[str] = []
    for w in final:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    # 참고로 model_plan은 로그로만 보고 싶으면 여기서 출력 가능
    if model_plan is not None:
        log.info(f"[Gemini plan suggestion] {model_plan} -> final {deduped}")

    return deduped

# ---------- 실행 레코드 ----------
def insert_run(cur, session_row: dict, base_run_id, doc_id: int):
    cur.execute("""
      INSERT INTO runs (user_id, session_id, base_run_id, doc_id, status, progress, attempt, started_at)
      VALUES (%s, %s, %s, %s, 'running', 0, 1, NOW())
      RETURNING id, user_id, session_id, base_run_id, doc_id, status, progress, attempt, started_at, finished_at;
    """, (session_row["user_id"], session_row["id"], base_run_id, doc_id))
    r = cur.fetchone()
    return {
        "id": r[0],
        "user_id": r[1],
        "session_id": r[2],
        "base_run_id": r[3],
        "doc_id": r[4],
        "status": r[5],
        "progress": r[6],
        "attempt": r[7],
        "started_at": r[8],
        "finished_at": r[9],
    }

# ---------- 워커 호출 ----------
def worker_fn_name(worker: str) -> str:
    env_key = f"L_{worker.upper()}"
    return os.getenv(env_key, f"wk-{worker}")

def invoke_worker_async(worker: str, payload: dict):
    fn = worker_fn_name(worker)
    LAMBDA.invoke(
        FunctionName=fn,
        InvocationType="Event",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )

# ---------- plan 저장 + run_results 씨딩 ----------
def _normalize_plan(plan_list: list[str]) -> list[str]:
    """
    - 중복 제거
    - 순서 유지
    - verifier를 항상 마지막에 포함
    """
    seen = set()
    out: list[str] = []
    for w in plan_list or []:
        if isinstance(w, str):
            w = w.strip()
        if not w:
            continue
        if w not in seen:
            seen.add(w)
            out.append(w)
    if "verifier" not in seen:
        out.append("verifier")
    return out


def persist_plan_and_seed_results(run_id: int, plan_list: list[str]):
    """
    - runs.plan 에 계획 저장 (verifier 포함된 정규화 plan)
    - run_results 에 worker별 {"status":"queued"} 미리 INSERT (있으면 건너뜀)
    """
    norm_plan = _normalize_plan(plan_list)

    with db() as conn, conn.cursor() as cur:
        # runs.plan 저장
        cur.execute(
            "UPDATE runs SET plan=%s WHERE id=%s",
            (json.dumps(norm_plan), run_id),
        )

        # run_results 씨드 (plan에 있는 워커 + verifier)
        for w in norm_plan:
            cur.execute(
                """
                INSERT INTO run_results (worker_type, run_id, payload)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (run_id, worker_type) DO NOTHING
                """,
                (w, run_id, json.dumps({"status": "queued"})),
            )
        conn.commit()

    return norm_plan

# ---------- 상태 재계산 트리거 ----------
def trigger_run_tick(run_id: int):
    """
    환경변수 L_RUN_TICK 이 설정되어 있으면, 해당 람다를 Event 모드로 호출.
    (tick 람다는 run_results를 읽고 runs.status/progress/finished_at를 갱신)
    """
    tick = os.getenv("L_RUN_TICK")
    if not tick:
        return
    try:
        LAMBDA.invoke(
            FunctionName=tick,
            InvocationType="Event",
            Payload=json.dumps({"runId": run_id}).encode("utf-8"),
        )
    except Exception as e:
        log.warning(f"run-tick invoke failed: {e}")

# ---------- 핸들러 ----------
def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers()}

    try:
        try:
            user_id = get_auth_user_id(event)
        except AuthError as e:
            return {"statusCode": 401, "headers": cors_headers(),
                    "body": json.dumps({"ok": False, "error": str(e)})}

        # 1) pathParameters.sessionId
        path_params = (event.get("pathParameters") or {}) if isinstance(event, dict) else {}
        session_id_str = path_params.get("sessionId") or path_params.get("session_id")
        if not session_id_str:
            return {"statusCode": 400, "headers": cors_headers(),
                    "body": json.dumps({"ok": False, "error": "missing sessionId in path"})}
        session_id = int(session_id_str)

        # 2) body (override용)
        body = event.get("body") if isinstance(event, dict) else event
        if body and isinstance(body, str):
            try: body = json.loads(body)
            except Exception: body = {}
        if not isinstance(body, dict):
            body = {}

        role_in    = body.get("role")
        answers_in = body.get("answers", {}) or {}
        base_run_id = body.get("baseRunId")

        # reanalyze/기타 옵션 전달용 inputs
        extra_inputs = body.get("inputs", {}) or {}

        # 3) DB 조회
        with db() as conn, conn.cursor() as cur:
            ensure_runs(cur)

            session_row = get_session_row(cur, session_id)
            if not session_row:
                return {"statusCode": 404, "headers": cors_headers(),
                        "body": json.dumps({"ok": False, "error": "session not found"})}

            if session_row["user_id"] != user_id:
                return {"statusCode": 403, "headers": cors_headers(),
                        "body": json.dumps({"ok": False, "error": "user forbidden"})}

            doc_id = int(session_row["doc_id"])

            role = role_in or session_row.get("role") or ""
            s_ans = session_row.get("answers") or {}
            question = (answers_in.get("question")
                        or (s_ans.get("question") if isinstance(s_ans, dict) else "")
                        or "")
            focus = (answers_in.get("focus")
                     or (s_ans.get("focus") if isinstance(s_ans, dict) else [])
                     or [])
            if isinstance(focus, str):
                focus = [focus]

            doc_name = get_document_name(cur, doc_id)
            if not doc_name:
                return {"statusCode": 404, "headers": cors_headers(),
                        "body": json.dumps({"ok": False, "error": "document not found"})}

            stats = get_doc_stats(cur, doc_id)
            s3_key = stats["s3_text_key"] or f"output/{doc_name}.txt"
            chunk_snips = get_chunk_snippets(cur, doc_id, limit=6, max_chars=200)
            anchors_sample = stats["first_titles"]

            # runs INSERT
            run_row = insert_run(cur, session_row, base_run_id, doc_id)
            conn.commit()

        # 4) 계획 생성
        plan_list = get_plan_from_gemini(
            role=role, question=question, focus_list=focus,
            stats=stats, anchors_sample=anchors_sample, chunk_snippets=chunk_snips
        )

        # 4-1) 계획 저장 + 씨딩 (verifier 포함 정규화)
        norm_plan = persist_plan_and_seed_results(run_row["id"], plan_list)

        # 5) 워커 호출
        for worker in norm_plan:
            if worker == "verifier":
                continue

            # extra_inputs + 기본 입력 merge
            merged_inputs = dict(extra_inputs or {})
            merged_inputs.update({
                "s3TextKey": s3_key,
                "role": role,
                "question": question,
                "focus": focus,
            })

            try:
                invoke_worker_async(worker, {
                    "runId": run_row["id"],
                    "sessionId": session_id,
                    "docId": doc_id,
                    "worker": worker,
                    "inputs": merged_inputs,
                })
            except Exception as e:
                log.warning(f"Worker invoke failed for {worker}: {e}")

        # 5-1) 상태 재계산 트리거
        trigger_run_tick(run_row["id"])

        # 6) 응답 (기존 그대로)
        resp = {
            "ok": True,
            "runId": run_row["id"],
            "startedAt": (run_row["started_at"].isoformat() + "Z") if run_row["started_at"] else None,
            "run": {
                "id": run_row["id"],
                "userId": run_row["user_id"],
                "sessionId": run_row["session_id"],
                "baseRunId": run_row["base_run_id"],
                "docId": run_row["doc_id"],
                "status": run_row["status"],
                "progress": run_row["progress"],
                "attempt": run_row["attempt"],
                "startedAt": (run_row["started_at"].isoformat() + "Z") if run_row["started_at"] else None,
                "finishedAt": (run_row["finished_at"].isoformat() + "Z") if run_row["finished_at"] else None,
            },
            "plan": norm_plan,
            "doc": {
                "id": doc_id,
                "name": doc_name,
                "s3TextKey": s3_key,
                "stats": {
                    "chunks": stats["chunks"],
                    "anchorsTotal": stats["anchors_total"],
                    "sampleAnchors": stats["first_titles"],
                }
            },
            "session": {
                "id": session_id,
                "role": role,
                "answers": {"question": question, "focus": focus}
            }
        }
        return {"statusCode": 200, "headers": cors_headers(),
                "body": json.dumps(resp, ensure_ascii=False)}

    except Exception as e:
        log.exception("run-start failed")
        return {"statusCode": 500, "headers": cors_headers(),
                "body": json.dumps({"ok": False, "error": str(e)})}