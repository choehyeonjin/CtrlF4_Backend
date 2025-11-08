import os, json, logging, io, boto3, psycopg2
from typing import Any, Dict

# Logging Setup
logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)

# Global Config
BUCKET = os.environ.get("BUCKET", "ctrlf4seoul")
MAX_HEAD_BYTES = int(os.environ.get("MAX_HEAD_BYTES", "4096"))
S3 = boto3.client("s3")

# Utilities
def resp(status: int, body: dict):
    """공통 응답 함수 (CORS + JSON + statusCode)"""
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, ensure_ascii=False)
    }

def need(n):
    v = os.getenv(n)
    if not v:
        raise RuntimeError(f"Missing {n}")
    return v

def db():
    return psycopg2.connect(
        host=need("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=need("PGDATABASE"),
        user=need("PGUSER"),
        password=need("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5
    )

# DB Operations
def get_doc(cur, doc_id: int):
    cur.execute("""
        SELECT id, name, status, pages, predicted_role, predicted_type
        FROM documents WHERE id=%s;
    """, (doc_id,))
    return cur.fetchone()

def set_status(cur, doc_id: int, status: str, predicted_role=None, predicted_type=None, pages=None):
    cur.execute("""
        UPDATE documents SET
            status=%s,
            predicted_role = COALESCE(%s::text[], predicted_role),
            predicted_type = COALESCE(%s, predicted_type),
            pages          = COALESCE(%s, pages),
            updated_at     = NOW()
        WHERE id=%s;
    """, (status, predicted_role, predicted_type, pages, doc_id))

def read_txt_head(bucket, key, n):
    obj = S3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{n-1}")
    return obj["Body"].read().decode("utf-8", errors="ignore")

# Gemini API
import google.generativeai as genai

def get_gemini():
    genai.configure(api_key=need("GEMINI_API_KEY"))
    return genai.GenerativeModel(os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite"))

def analyze(text: str) -> Dict[str, Any]:
    model = get_gemini()
    prompt = (
        "아래는 계약서의 일부입니다. JSON만 반환: "
        '{ "type":"<문서유형>", "predictedRole":["<역할>", "..."] }\n\n' + (text[:1000] if text else "")
    )

    try:
        res = model.generate_content(prompt)
        raw = (res.text or "").strip()
        if raw.startswith("```"):
            raw = raw.strip("`").split("\n", 1)[-1].strip()
        data = json.loads(raw)
    except Exception as e:
        log.warning(f"[Gemini parsing error] {e}")
        data = {"type": "알수없음", "predictedRole": []}

    pr = data.get("predictedRole") or []
    if isinstance(pr, str):
        pr = [pr]

    return {"type": data.get("type", "알수없음"), "predictedRole": pr}

# Lambda Handler
def lambda_handler(event, context):
    try:
        # 1️⃣ Parse parameters
        path = event.get("pathParameters") or {}
        qsp = event.get("queryStringParameters") or {}
        try:
            doc_id = int(path.get("docId"))
        except:
            return resp(400, {"ok": False, "error": "missing docId"})

        force = (qsp.get("force", "false").lower() == "true")

        # 2️⃣ Get document from DB
        with db() as conn, conn.cursor() as cur:
            row = get_doc(cur, doc_id)
            if not row:
                return resp(404, {"ok": False, "error": "document not found"})

            _, name, status, pages_in_db, existed_roles, existed_type = row

            if not force and (
                (existed_roles and len(existed_roles) > 0)
                or existed_type
                or (status and status.lower() == "probed")
            ):
                return resp(200, {
                    "ok": True,
                    "skipped": True,
                    "reason": "already_probed",
                    "docId": doc_id,
                    "probed": {"type": existed_type, "pages": pages_in_db},
                    "predictedRole": existed_roles or [],
                    "status": status or "probed"
                })

        # 3️⃣ Read head of document from S3
        txt_key = f"output/{name}.txt"
        try:
            head = read_txt_head(BUCKET, txt_key, MAX_HEAD_BYTES)
        except Exception as e:
            log.error(f"S3 read failed: {e}")
            return resp(500, {"ok": False, "error": f"cannot read S3 object: {txt_key}", "details": str(e)})

        # 4️⃣ Analyze with Gemini
        a = analyze(head)
        dtype, roles = a["type"], a["predictedRole"]
        pages = pages_in_db

        # 5️⃣ Update DB status
        with db() as conn, conn.cursor() as cur:
            set_status(cur, doc_id, "probed",
                       predicted_role=roles,
                       predicted_type=dtype,
                       pages=pages)
            conn.commit()

        # 6️⃣ Success Response
        return resp(200, {
            "ok": True,
            "skipped": False,
            "docId": doc_id,
            "probed": {"type": dtype, "pages": pages},
            "predictedRole": roles,
            "status": "probed"
        })

    except Exception as e:
        log.exception("Unhandled error")
        return resp(500, {"ok": False, "error": str(e)})
