import os, json, logging
from typing import Any, Dict, List, Optional
import psycopg2

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# ENV & DB
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

# HTTP helpers (CORS)
CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json",
}
def _resp(code: int, body: Dict[str, Any]):
    return {"statusCode": code, "headers": CORS, "body": json.dumps(body, ensure_ascii=False)}

# Parse helpers
def _get_path_param(event: Dict[str, Any], key: str) -> Optional[str]:
    p = event.get("pathParameters") or {}
    return p.get(key)

def _get_query(event: Dict[str, Any]) -> Dict[str, str]:
    return (event.get("queryStringParameters") or {}) if isinstance(event, dict) else {}

def _as_list(csv: Optional[str]) -> Optional[List[str]]:
    if csv is None:
        return None
    items = [x.strip() for x in csv.split(",") if x.strip()]
    return items or None

# Core queries
def fetch_run_meta(cur, run_id: int) -> Optional[Dict[str, Any]]:
    cur.execute("""
      SELECT id, user_id, session_id, base_run_id, doc_id, status, progress, attempt,
             started_at, finished_at
      FROM runs
      WHERE id=%s
      LIMIT 1
    """, (run_id,))
    r = cur.fetchone()
    if not r: return None
    def ts(x): return (x.isoformat() + "Z") if x else None
    return {
        "id": r[0], "userId": r[1], "sessionId": r[2], "baseRunId": r[3], "docId": r[4],
        "status": r[5], "progress": r[6], "attempt": r[7],
        "startedAt": ts(r[8]), "finishedAt": ts(r[9]),
    }

def fetch_run_results(cur, run_id: int, workers: Optional[List[str]]) -> Dict[str, Any]:
    if workers:
        sql = """
          SELECT worker_type, payload
          FROM run_results
          WHERE run_id=%s AND worker_type = ANY(%s)
          ORDER BY worker_type
        """
        cur.execute(sql, (run_id, workers))
    else:
        cur.execute("""
          SELECT worker_type, payload
          FROM run_results
          WHERE run_id=%s
          ORDER BY worker_type
        """, (run_id,))
    out: Dict[str, Any] = {}
    for w, payload in cur.fetchall():
        if isinstance(payload, str):
            try: payload = json.loads(payload)
            except Exception: pass
        out[w] = payload
    return out

def fetch_doc_and_session(cur, run_meta: Dict[str, Any]) -> Dict[str, Any]:
    # 문서 이름
    doc_name = None
    cur.execute("SELECT name FROM documents WHERE id=%s;", (run_meta["docId"],))
    r = cur.fetchone()
    if r: doc_name = r[0]

    # 세션 요약
    session = None
    cur.execute("SELECT role, answers FROM sessions WHERE id=%s;", (run_meta["sessionId"],))
    r = cur.fetchone()
    if r:
        role, answers = r[0], r[1]
        if isinstance(answers, str):
            try: answers = json.loads(answers)
            except Exception: answers = {}
        session = {"role": role or "", "answers": answers or {}}

    return {"doc": {"id": run_meta["docId"], "name": doc_name}, "session": session}

# Lambda handler
def lambda_handler(event, context):
    try:
        # 프리플라이트 허용
        if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
            return {"statusCode": 200, "headers": CORS}

        # 1) 입력
        run_id_raw = _get_path_param(event, "runId")
        if not run_id_raw:
            return _resp(400, {"ok": False, "error": "missing runId"})
        try:
            run_id = int(run_id_raw)
        except:
            return _resp(400, {"ok": False, "error": "invalid runId"})

        q = _get_query(event)
        workers_filter = _as_list(q.get("worker"))   # 예: ?worker=qa,risk

        # 2) DB 조회
        with db() as conn, conn.cursor() as cur:
            run_meta = fetch_run_meta(cur, run_id)
            if not run_meta:
                return _resp(404, {"ok": False, "error": "run not found"})

            results = fetch_run_results(cur, run_id, workers_filter)
            extras  = fetch_doc_and_session(cur, run_meta)

        # 3) 응답
        body = {
            "ok": True,
            "run": run_meta,
            "doc": extras.get("doc"),
            "session": extras.get("session"),
            "results": results,
            "availableWorkers": sorted(list(results.keys())),
        }
        return _resp(200, body)

    except Exception as e:
        log.exception("run-results failed")
        return _resp(500, {"ok": False, "error": f"{type(e).__name__}: {e}"})