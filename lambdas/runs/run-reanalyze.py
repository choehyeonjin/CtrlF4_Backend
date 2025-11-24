# lambda_function.py  (run-reanalyze)
# -*- coding: utf-8 -*-
import os, json, logging
import psycopg2, boto3, jwt
from jwt import InvalidTokenError

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
LAMBDA = boto3.client("lambda", region_name=AWS_REGION)
L_RUN_START = os.getenv("L_RUN_START", "run-start")

class AuthError(Exception):
    pass

# ---------- CORS ----------
def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Content-Type": "application/json",
    }

# ---------- ENV / DB ----------
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

# ---------- Auth ----------
def get_auth_user_id(event) -> int:
    headers = (event.get("headers") or {})
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    if not auth.startswith("Bearer "):
        raise AuthError("missing bearer token")

    token = auth.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, os.environ["JWT_SECRET_KEY"], algorithms=["HS256"])
    except InvalidTokenError as e:
        raise AuthError(f"invalid token: {e}")

    if payload.get("type") != "access":
        raise AuthError("invalid token type")

    return int(payload["sub"])

# ---------- helpers ----------
def _clip(s: str, n: int) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[:n] + " ..."

def load_run_and_session(cur, run_id: int):
    """
    runs + sessions + documents join 해서 기본 정보 가져오기
    """
    cur.execute("""
        SELECT
          r.id, r.user_id, r.session_id, r.doc_id, r.status,
          s.role, s.answers,
          s.user_id AS session_user_id
        FROM runs r
        JOIN sessions s ON s.id = r.session_id
        WHERE r.id = %s
        LIMIT 1;
    """, (run_id,))
    row = cur.fetchone()
    if not row:
        return None

    answers = row[6]
    if isinstance(answers, str):
        try:
            answers = json.loads(answers)
        except Exception:
            answers = {}

    return {
        "id": row[0],
        "user_id": row[1],
        "session_id": row[2],
        "doc_id": row[3],
        "status": row[4],
        "session_role": row[5] or "",
        "session_answers": answers or {},
        "session_user_id": row[7],
    }

def build_prev_summary(cur, prev_run_id: int) -> dict:
    """
    이전 run의 run_results를 읽어서 간단한 요약 구조 + 텍스트 생성
    """
    cur.execute("SELECT status FROM runs WHERE id=%s", (prev_run_id,))
    r = cur.fetchone()
    status = r[0] if r else None

    cur.execute("SELECT worker_type, payload FROM run_results WHERE run_id=%s", (prev_run_id,))
    results = {}
    for w, p in cur.fetchall():
        if isinstance(p, dict):
            results[w] = p
        else:
            try:
                results[w] = json.loads(p)
            except Exception:
                results[w] = {}

    sm = results.get("summarizer") or {}
    rk = results.get("risk") or {}
    qa = results.get("qa") or {}
    rv = results.get("revision") or {}

    summary_text = (
        sm.get("results", {})
          .get("full_document", {})
          .get("summary", "") or ""
    )
    qa_answer = qa.get("answer", "") or ""
    risk_items = rk.get("items") or []
    rev_items  = rv.get("revisions") or []

    risk_cnt = len(risk_items) if isinstance(risk_items, list) else 0
    rev_cnt  = len(rev_items) if isinstance(rev_items, list) else 0

    lines = [f"- 이전 run ID: {prev_run_id}"]
    if status:
        lines.append(f"- 상태: {status}")
    if risk_cnt:
        lines.append(f"- 탐지된 리스크 항목 수: {risk_cnt}개")
    if rev_cnt:
        lines.append(f"- 제안된 수정안 개수: {rev_cnt}개")
    if qa_answer:
        lines.append(f"- Q&A 답변 요약: {_clip(qa_answer, 300)}")
    if summary_text:
        lines.append(f"- 전체 요약: {_clip(summary_text, 400)}")

    text = "\n".join(lines) if lines else ""

    return {
        "prevRunId": prev_run_id,
        "status": status,
        "riskCount": risk_cnt,
        "revisionCount": rev_cnt,
        "qaAnswer": qa_answer,
        "summary": summary_text,
        "text": text,
    }

# ---------- Lambda handler ----------
def lambda_handler(event, context):
    # CORS preflight
    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers()}

    try:
        try:
            user_id = get_auth_user_id(event)
        except AuthError as e:
            return {
                "statusCode": 401,
                "headers": cors_headers(),
                "body": json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            }

        # pathParameters.runId
        path_params = (event.get("pathParameters") or {}) if isinstance(event, dict) else {}
        run_id_str = path_params.get("runId") or path_params.get("run_id")
        if not run_id_str:
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"ok": False, "error": "missing runId in path"}, ensure_ascii=False),
            }
        prev_run_id = int(run_id_str)

        # Body 파싱
        body = event.get("body") if isinstance(event, dict) else event
        if isinstance(body, str) and body.strip():
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        if not isinstance(body, dict):
            body = {}

        # 명세 상: body.inputs.role/question/focus (optional)
        inputs = body.get("inputs", {}) or {}
        role_in     = inputs.get("role")
        question_in = inputs.get("question")
        focus_in    = inputs.get("focus")

        with db() as conn, conn.cursor() as cur:
            run_row = load_run_and_session(cur, prev_run_id)
            if not run_row:
                return {
                    "statusCode": 404,
                    "headers": cors_headers(),
                    "body": json.dumps({"ok": False, "error": "run not found"}, ensure_ascii=False),
                }

            # 소유자 검사 (runs.user_id 우선, 없으면 sessions.user_id)
            owner_id = run_row["user_id"] or run_row["session_user_id"]
            if owner_id != user_id:
                return {
                    "statusCode": 403,
                    "headers": cors_headers(),
                    "body": json.dumps({"ok": False, "error": "user forbidden"}, ensure_ascii=False),
                }

            # status 체크: completed 가 아니면 재분석 불가
            if run_row["status"] != "completed":
                return {
                    "statusCode": 409,
                    "headers": cors_headers(),
                    "body": json.dumps(
                        {"ok": False, "error": "run is not completed; reanalyze allowed only after report"},
                        ensure_ascii=False,
                    ),
                }

            session_id = run_row["session_id"]
            doc_id     = run_row["doc_id"]

            # 기존 session role/answers 기반으로 기본값 계산
            s_role = run_row["session_role"] or ""
            s_ans  = run_row["session_answers"] or {}
            base_role     = role_in or s_role
            base_question = question_in or s_ans.get("question") or ""
            base_focus    = focus_in or s_ans.get("focus") or []
            if isinstance(base_focus, str):
                base_focus = [base_focus]

            # 이전 run 결과 요약 생성
            prev_summary = build_prev_summary(cur, prev_run_id)
            prev_summary_text = prev_summary.get("text") or ""

        # 이제 run-start 람다를 동기 호출해서 새 run 생성
        run_start_event = {
            "httpMethod": "POST",
            "headers": event.get("headers") or {},
            "pathParameters": {"sessionId": str(session_id)},
            "body": json.dumps(
                {
                    "role": base_role,
                    "answers": {
                        "question": base_question,
                        "focus": base_focus,
                    },
                    # 새 run 의 base_run_id
                    "baseRunId": prev_run_id,
                    # 워커들에 전달할 재분석 컨텍스트
                    "inputs": {
                        "reanalyze": True,
                        "prevRunId": prev_run_id,
                        "prevSummary": prev_summary,
                        "prevSummaryText": prev_summary_text,
                    },
                },
                ensure_ascii=False,
            ),
        }

        invoke_resp = LAMBDA.invoke(
            FunctionName=L_RUN_START,
            InvocationType="RequestResponse",
            Payload=json.dumps(run_start_event, ensure_ascii=False).encode("utf-8"),
        )
        raw_payload = invoke_resp["Payload"].read().decode("utf-8")
        run_start_result = json.loads(raw_payload)

        status_code = run_start_result.get("statusCode", 500)
        if status_code != 200:
            # run-start 쪽 에러 그대로 전달
            return {
                "statusCode": status_code,
                "headers": cors_headers(),
                "body": run_start_result.get("body", json.dumps({"ok": False, "error": "run-start failed"})),
            }

        body_str = run_start_result.get("body") or "{}"
        try:
            body_json = json.loads(body_str)
        except Exception:
            body_json = {}

        new_run_id = body_json.get("runId")
        new_run_status = (body_json.get("run") or {}).get("status", "running")

        resp = {
            "ok": True,
            "runId": new_run_id,
            "prevRunId": prev_run_id,
            "sessionId": session_id,
            "docId": doc_id,
            "status": new_run_status,
            "createdAt": body_json.get("startedAt"),
            "message": f"기존 분석 결과(runId={prev_run_id})를 바탕으로 재분석을 다시 시작했습니다.",
        }
        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps(resp, ensure_ascii=False),
        }

    except Exception as e:
        log.exception("run-reanalyze failed")
        return {
            "statusCode": 500,
            "headers": cors_headers(),
            "body": json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
        }