import os, json, logging, psycopg2

logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)

# ---------- DB ----------
def need_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def db():
    return psycopg2.connect(
        host=need_env("PGHOST"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=need_env("PGDATABASE"),
        user=need_env("PGUSER"),
        password=need_env("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5
    )

STATUS2PROG = {
    "uploaded": 1,
    "preprocessed": 2,
    "embeded": 3,
    "probed": 4,
    "error": 5,
}

def get_document(cur, doc_id: int):
    cur.execute("""
        SELECT id, name, status, pages, file_count, predicted_role, predicted_type,
               uploaded_at, updated_at
        FROM documents
        WHERE id=%s;
    """, (doc_id,))
    return cur.fetchone()

# ---------- Handler ----------
def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        doc_id = path_params.get("docId")
        if not doc_id:
            return {"statusCode": 400, "body": json.dumps({"ok": False, "error": "missing docId"})}
        doc_id = int(doc_id)

        with db() as conn:
            with conn.cursor() as cur:
                row = get_document(cur, doc_id)
                if not row:
                    return {"statusCode": 404, "body": json.dumps({"ok": False, "error": "document not found"})}
                _id, name, status, pages, file_count, predicted_role, predicted_type, uploaded_at, updated_at = row

        status = (status or "").lower()
        progress = STATUS2PROG.get(status, 0)

        body = {
            "docId": doc_id,
            "name": name,
            "status": status or "unknown",
            "progress": progress
        }

        return {"statusCode": 200, "body": json.dumps(body, ensure_ascii=False)}
    except Exception as e:
        log.exception("doc-status failed")
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": f"{type(e).__name__}: {str(e)}"})}
