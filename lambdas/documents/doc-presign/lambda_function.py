import os, json, base64, uuid
import boto3
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

# ===========================================
# Feature Flags / Env
# ===========================================
USE_DB = str(os.getenv("USE_DB", "0")).strip().strip('"') == "1"

pg = None
if USE_DB:
    try:
        import psycopg2
        pg = psycopg2
    except Exception:
        USE_DB = False  # psycopg2 없으면 자동 OFF

s3 = boto3.client("s3")

# ===========================================
# CORS 설정
# ===========================================

CORS = {
    "Access-Control-Allow-Origin": os.getenv("CORS_ALLOW_ORIGIN", "*"),
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": os.getenv(
        "CORS_ALLOW_HEADERS",
        "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent"
    ),
    "Access-Control-Allow-Methods": os.getenv(
        "CORS_ALLOW_METHODS",
        "GET,POST,OPTIONS"
    ),
    "Content-Type": "application/json",
}


# ===========================================
# 공통 응답 함수
# ===========================================
def _resp(code: int, body):
    if isinstance(body, (dict, list)):
        body_txt = json.dumps(body, ensure_ascii=False)
    else:
        body_txt = str(body)
    return {
        "statusCode": code,
        "headers": CORS,
        "body": body_txt
    }


# ===========================================
# event parser (REST API + HTTP API 모두 호환)
# ===========================================
def _parse_event(event):
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            event = {}

    print("DEBUG RAW EVENT:", json.dumps(event))

    # method
    method = (
        event.get("httpMethod") or
        event.get("requestContext", {}).get("http", {}).get("method") or
        "POST"
    ).upper()

    # path
    path = event.get("path") or event.get("rawPath") or "/"

    # body
    body_raw = event.get("body")
    if body_raw and event.get("isBase64Encoded"):
        try:
            body_raw = base64.b64decode(body_raw).decode("utf-8", errors="ignore")
        except Exception as e:
            print("DEBUG BASE64 DECODE ERROR:", e)

    try:
        body = json.loads(body_raw) if isinstance(body_raw, str) and body_raw else (body_raw or {})
    except Exception:
        body = {}

    qs = event.get("queryStringParameters") or {}

    print("DEBUG PARSED:", {"method": method, "path": path, "body": body, "qs": qs})
    return method, path, body, qs


# ===========================================
# DB connection
# ===========================================
def _db_conn():
    return pg.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        port=int(os.environ.get("PGPORT", "5432")),
    )


# ===========================================
# S3 presign 함수
# ===========================================
def _presign_put(bucket: str, key: str, expires: int, content_type: str | None):
    params = {
        "Bucket": bucket,
        "Key": key,
        "ACL": "private",
    }
    if content_type:
        params["ContentType"] = content_type

    print("DEBUG presign PUT params:", params)
    return s3.generate_presigned_url("put_object", Params=params, ExpiresIn=expires)


def _presign_get(bucket: str, key: str, expires: int):
    params = {
        "Bucket": bucket,
        "Key": key
    }
    print("DEBUG presign GET params:", params)
    return s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires)


# ===========================================
# 메인 핸들러
# ===========================================
def lambda_handler(event, context):
    method, path, body, qs = _parse_event(event)
    print(event.get("headers"))
    # 인증
    try:
        user_id = get_auth_user_id(event)
    except AuthError as e:
        return _resp(401, {"error": str(e)})

    # ----------------------------
    # OPTIONS (CORS preflight)
    # ----------------------------
    if method == "OPTIONS":
        return _resp(200, "")

    # ----------------------------
    # Health Check
    # ----------------------------
    if method == "GET" and (path == "/" or path.endswith("/health")):
        return _resp(200, {"ok": True, "useDb": USE_DB})

    # ----------------------------
    # Only POST allowed
    # ----------------------------
    if method != "POST":
        return _resp(405, {"error": f"method {method} not allowed"})

    # 공통 입력 파싱
    bucket = body.get("bucket") or os.getenv("BUCKET") or os.getenv("DEFAULT_BUCKET")
    key = body.get("key")
    op = (body.get("operation") or os.getenv("DEFAULT_OPERATION", "put")).lower()
    expires = int(body.get("expiresIn") or os.getenv("DEFAULT_EXPIRES", "900"))
    content_type = body.get("contentType")
    file_name = body.get("fileName")

    print("DEBUG INPUT:", {
        "bucket": bucket,
        "key": key,
        "op": op,
        "expires": expires,
        "content_type": content_type,
        "file_name": file_name,
        "user_id": user_id
    })

    # -------------------------------------------------------------
    # 파일명 중복 방지
    # -------------------------------------------------------------
    if file_name:
        base, ext = os.path.splitext(file_name)
        unique_suffix = uuid.uuid4().hex[:8]
        file_name = f"{base}_{unique_suffix}{ext}"

    # ============================================================
    # DB MODE
    # ============================================================
    if USE_DB:
        if not file_name:
            return _resp(400, {"error": "fileName required (when USE_DB=1)"})
        if not bucket:
            return _resp(400, {"error": "bucket required"})

        try:
            conn = _db_conn(); conn.autocommit = True
            cur = conn.cursor()

            # Insert metadata
            cur.execute("""
                INSERT INTO documents (name, status, uploaded_at, updated_at, user_id)
                VALUES (%s, 'uploaded', NOW(), NOW(), %s)
                RETURNING id
            """, (file_name, user_id))

            doc_id = cur.fetchone()[0]

            # S3 key
            key = f"uploads/{file_name}"

            # create presign URL
            if op == "get":
                url = _presign_get(bucket, key, expires)
            else:
                url = _presign_put(bucket, key, expires, content_type)

            cur.execute("""
                UPDATE documents
                   SET updated_at = NOW()
                 WHERE id=%s
            """, (doc_id,))

            cur.close(); conn.close()

            return _resp(200, {
                "docId": doc_id,
                "bucket": bucket,
                "key": key,
                "fileName": file_name,
                "url": url
            })

        except Exception as e:
            print("DB ERROR:", e)
            return _resp(500, {"error": f"DB error: {e}"})


    # ============================================================
    # S3 DIRECT MODE
    # ============================================================
    if not bucket or not key:
        return _resp(400, {"error": "bucket and key required (or set USE_DB=1 and send fileName)"})

    try:
        if op == "get":
            url = _presign_get(bucket, key, expires)
        else:
            url = _presign_put(bucket, key, expires, content_type)

        return _resp(200, {"url": url, "bucket": bucket, "key": key})

    except Exception as e:
        print("S3 ERROR:", e)
        return _resp(500, {"error": f"S3 error: {e}"})
