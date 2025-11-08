import os, json, logging, psycopg2

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# -------------------- CORS --------------------
def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Content-Type": "application/json"
    }

# -------------------- DB --------------------
def need_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def db():
    return psycopg2.connect(
        host=need_env("PGHOST"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=need_env("PGDATABASE"),
        user=need_env("PGUSER"),
        password=need_env("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5,
    )

# -------------------- Sessions Schema --------------------
def _has_col(cur, table: str, col: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s
        LIMIT 1;
    """, (table, col))
    return cur.fetchone() is not None

def _col_type(cur, table: str, col: str):
    cur.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s;
    """, (table, col))
    r = cur.fetchone()
    return r[0] if r else None

def _ensure_updated_at_trigger(cur, table: str):
    cur.execute("""CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
        BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;""")
    cur.execute(f"""
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='{table}_touch_updated_at') THEN
          CREATE TRIGGER {table}_touch_updated_at
          BEFORE UPDATE ON {table}
          FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
        END IF;
      END$$;
    """)

def ensure_sessions_schema(cur):
    cur.execute("""
      CREATE TABLE IF NOT EXISTS sessions (
        id         SERIAL PRIMARY KEY,
        user_id    INT,
        doc_id     INT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        role       VARCHAR(64),
        answers    JSON,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    """)
    _ensure_updated_at_trigger(cur, "sessions")

    if not _has_col(cur, "sessions", "role"):
        cur.execute("ALTER TABLE sessions ADD COLUMN role VARCHAR(64);")

    if not _has_col(cur, "sessions", "answers"):
        cur.execute("ALTER TABLE sessions ADD COLUMN answers JSON;")
    else:
        dt = _col_type(cur, "sessions", "answers")
        if dt == "jsonb":
            cur.execute("ALTER TABLE sessions ALTER COLUMN answers TYPE JSON USING answers::json;")

    if _has_col(cur, "sessions", "predicted_role"):
        cur.execute("""
          UPDATE sessions
             SET role = COALESCE(role, predicted_role::text)
           WHERE role IS NULL OR role = '';
        """)
        cur.execute("ALTER TABLE sessions DROP COLUMN predicted_role;")

    if _has_col(cur, "sessions", "intent"):
        cur.execute("""
          UPDATE sessions
             SET answers = COALESCE(answers, (intent)::json);
        """)
        cur.execute("ALTER TABLE sessions DROP COLUMN intent;")

# -------------------- Helpers --------------------
def parse_body(event):
    b = event.get("body")
    if isinstance(b, str):
        try:
            return json.loads(b)
        except Exception:
            return {}
    return b or {}

# -------------------- Handler --------------------
def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers()}

    try:
        body = parse_body(event)
        doc_id = body.get("docId")
        role   = (body.get("role") or "").strip()[:64]
        answers = body.get("answers") or {}

        if not isinstance(doc_id, int):
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"ok": False, "error": "docId(int) required"})
            }

        if not isinstance(answers, dict):
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"ok": False, "error": "answers must be object"})
            }

        with db() as conn, conn.cursor() as cur:
            ensure_sessions_schema(cur)

            cur.execute("SELECT 1 FROM documents WHERE id=%s;", (doc_id,))
            if cur.fetchone() is None:
                return {
                    "statusCode": 404,
                    "headers": cors_headers(),
                    "body": json.dumps({"ok": False, "error": "document not found"})
                }

            cur.execute("""
              INSERT INTO sessions (user_id, doc_id, role, answers)
              VALUES (NULL, %s, %s, %s::json)
              RETURNING id;
            """, (doc_id, role, json.dumps(answers)))
            sid = cur.fetchone()[0]
            conn.commit()

        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps({
                "ok": True,
                "sessionId": sid,
                "docId": doc_id,
                "role": role,
                "answers": answers
            }, ensure_ascii=False)
        }

    except Exception as e:
        log.exception("sessions-create failed")
        return {
            "statusCode": 500,
            "headers": cors_headers(),
            "body": json.dumps({"ok": False, "error": f"{type(e).__name__}: {str(e)}"})
        }
