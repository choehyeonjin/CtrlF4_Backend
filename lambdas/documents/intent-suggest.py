import os, json, logging, psycopg2
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

# ---------- 공통 Response (CORS 포함) ----------
def response(status: int, body: dict):
    """응답 헬퍼 - 모든 응답에 CORS 헤더 포함"""
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With, Accept, Origin, User-Agent",
            "Access-Control-Allow-Credentials": "true",
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, ensure_ascii=False)
    }

# ---------- DB ----------
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

# ---------- Query ----------
def get_predicted_role(cur, doc_id: int):
    cur.execute("SELECT predicted_role FROM documents WHERE id=%s;", (doc_id,))
    row = cur.fetchone()
    return row[0] if row else None  # text[] or None

def get_doc_owner(cur, doc_id: int):
    cur.execute("SELECT user_id FROM documents WHERE id=%s;", (doc_id,))
    r = cur.fetchone()
    return r[0] if r else None

def as_list(x):
    if isinstance(x, list):
        return x
    if x in (None, ""):
        return []
    return [x]

# ---------- Handler ----------
def lambda_handler(event, context):
    try:
        # OPTIONS 프리플라이트 요청 허용
        if event.get("httpMethod", "").upper() == "OPTIONS":
            return response(200, {"ok": True, "message": "CORS preflight success"})

        try:
            user_id = get_auth_user_id(event)
        except AuthError as e:
            return response(401, {"ok": False, "error": str(e)})

        path = event.get("pathParameters") or {}
        doc_id_str = path.get("docId")
        if not doc_id_str:
            return response(400, {"ok": False, "error": "missing docId"})

        doc_id = int(doc_id_str)

        with db() as conn, conn.cursor() as cur:
            owner_id = get_doc_owner(cur, doc_id)
            if owner_id is None:
                return response(404, {"ok": False, "error": "document not found"})
            if owner_id != user_id:
                return response(403, {"ok": False, "error": "user forbidden"})

            predicted = get_predicted_role(cur, doc_id)
            predicted_role = as_list(predicted)

        body = {
            "ok": True,
            "predictedRole": predicted_role,
            "questions": [
                {"key": "role", "text": "당신의 역할/직책은 무엇인가요?"},
                {"key": "question", "text": "이 문서에서 궁금한 점이 있나요?"},
                {"key": "focus", "text": "분석에서 집중하고 싶은 조항 키워드는 무엇인가요?"}
            ],
        }
        return response(200, body)

    except Exception as e:
        log.exception("intent-suggest failed")
        return response(500, {"ok": False, "error": f"{type(e).__name__}: {str(e)}"})
