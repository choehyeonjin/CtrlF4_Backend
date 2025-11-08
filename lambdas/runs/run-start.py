import os, json, logging, datetime
import psycopg2, boto3, requests

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
    문서 메타데이터를 기반으로 어떤 워커(summarizer, risk, qa, revision)를 실행할지 Gemini로 판단
    """
    try:
        genai.configure(api_key=need("GEMINI_API_KEY"))
        model = genai.GenerativeModel(os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite"))

        anchors_lines  = "\n".join(f"- {t}" for t in anchors_sample[:10]) or "- (없음)"
        snippets_lines = "\n".join(f"- {s}" for s in chunk_snippets[:10]) or "- (없음)"

        if isinstance(focus_list, str):
            focus_list = [focus_list]
        focus_str = ", ".join(focus_list or [])

        prompt = f"""
너는 계약서 분석을 담당하는 오케스트레이터 AI다.
입력으로 계약서의 일부 내용(앵커 제목, 조항 일부)과 사용자의 요구(role, question, focus)이 주어진다.
주어진 문서의 성격과 사용자 요구를 고려해, 어떤 워커들을 실행해야 할지 JSON 배열 형식으로만 반환하라.

가능한 워커들은 다음 네 가지이다: 
1. "summarizer" : 문서 전체 및 조항별 요약 
2. "risk" : 6대 핵심 리스크 탐지 (법률·환불·개인정보·변경·책임·라이선스) 
3. "qa" : 사용자의 질문(question, focus)에 대한 질의응답 
4. "revision" : 탐지된 리스크 조항에 대한 수정·대체 문안 제안

### 입력 정보
- 역할(role): {role or "(없음)"}
- 사용자 질문(question): {question or "(없음)"}
- 주요 초점(focus): {focus_str or "(없음)"}

### 문서 요약 정보
- 총 청크 수(chunks): {stats.get("chunks", 0)}
- 추출된 조항 수(anchors_total): {stats.get("anchors_total", 0)}
- 대표 조항 제목들:
{anchors_lines}

- 조항 내용 일부(요약 스니펫):
{snippets_lines}

### 최소 판단 기준
- 문서가 서비스 약관, 계약서, 이용정책 등 “법률 문서”라면 summarizer와 risk를 반드시 포함하라. 
- 사용자가 질문(question)이나 focus를 입력했으면 qa를 포함하라. 
- “법무팀”, “검토”, “변호사” 등의 문서를 작성하는 역할(role)이 포함되어 있으면 revision을 포함하라.
- 문서가 단순 설명문이나 공지문 수준이라면 summarizer만 포함하라.

### 출력 형식 (JSON 배열만 반환)
예:
["summarizer", "risk", "qa"]
""".strip()

        # 1️⃣ Gemini 호출
        res = model.generate_content(prompt)
        raw = (res.text or "").strip()

        # 2️⃣ 코드블록 제거
        if raw.startswith("```"):
            raw = raw.strip("`").split("\n", 1)[-1].strip()

        # 3️⃣ JSON 파싱
        start, end = raw.find("["), raw.rfind("]")
        plan_json = raw[start:end+1] if start != -1 and end != -1 else raw
        plan = json.loads(plan_json)

        # 4️⃣ 유효성 확인
        if isinstance(plan, list) and all(isinstance(x, str) for x in plan):
            return plan

        raise ValueError("invalid plan format")

    except Exception as e:
        log.warning(f"[Gemini fallback] {e}")
        # ✅ 폴백 기본 계획
        plan = ["summarizer", "risk"]
        if (question and str(question).strip()) or (focus_list and len(focus_list) > 0):
            plan.append("qa")
        if role and any(k in role for k in ["법무", "변호", "검토", "리스크", "컴플라이언스"]):
            plan.append("revision")
        return plan

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
def persist_plan_and_seed_results(run_id: int, plan_list: list[str]):
    """
    - runs.plan 에 계획 저장
    - run_results 에 worker별 {"status":"queued"} 미리 INSERT (있으면 건너뜀)
    """
    with db() as conn, conn.cursor() as cur:
        # runs.plan 저장
        cur.execute("UPDATE runs SET plan=%s WHERE id=%s", (json.dumps(plan_list), run_id))
        # run_results 씨드
        for w in plan_list:
            cur.execute("""
                INSERT INTO run_results (worker_type, run_id, payload)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (run_id, worker_type) DO NOTHING
            """, (w, run_id, json.dumps({"status":"queued"})))
        conn.commit()

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

        # 3) DB 조회
        with db() as conn, conn.cursor() as cur:
            ensure_runs(cur)

            session_row = get_session_row(cur, session_id)
            if not session_row:
                return {"statusCode": 404, "headers": cors_headers(),
                        "body": json.dumps({"ok": False, "error": "session not found"})}

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

        # 4-1) 계획 저장 + 씨딩
        persist_plan_and_seed_results(run_row["id"], plan_list)

        # 5) 워커 호출
        for worker in plan_list:
            try:
                invoke_worker_async(worker, {
                    "runId": run_row["id"],
                    "sessionId": session_id,
                    "docId": doc_id,
                    "worker": worker,
                    "inputs": {
                        "s3TextKey": s3_key,
                        "role": role,
                        "question": question,
                        "focus": focus,
                    }
                })
            except Exception as e:
                log.warning(f"Worker invoke failed for {worker}: {e}")

        # 5-1) 상태 재계산 트리거
        trigger_run_tick(run_row["id"])

        # 6) 응답
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
            "plan": plan_list,
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
