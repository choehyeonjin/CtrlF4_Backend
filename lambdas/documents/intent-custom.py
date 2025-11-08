import os, json, logging
logging.getLogger().setLevel(os.environ.get("LOG_LEVEL","INFO").upper())
log = logging.getLogger(__name__)

def parse_body(event):
    b = event.get("body")
    if isinstance(b, str):
        try:
            return json.loads(b)
        except:
            return {}
    return b or {}

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    s = str(x).strip()
    return [s] if s else []

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With,Accept,Origin,User-Agent",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Content-Type": "application/json"
    }

def lambda_handler(event, context):
    # OPTIONS 프리플라이트 요청 처리
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers()}

    try:
        doc_id = int((event.get("pathParameters") or {}).get("docId"))
    except:
        return {
            "statusCode": 400,
            "headers": cors_headers(),
            "body": json.dumps({"ok": False, "error": "missing docId"})
        }

    body = parse_body(event)
    raw = body.get("answers") or {}

    role = (raw.get("role") or "").strip()[:64]  # VARCHAR(64)
    question = (raw.get("question") or "").strip()
    focus = as_list(raw.get("focus"))

    normalized = {
        "role": role,
        "answers": {"question": question, "focus": focus}
    }

    return {
        "statusCode": 200,
        "headers": cors_headers(),
        "body": json.dumps(
            {"ok": True, "docId": doc_id, **normalized},
            ensure_ascii=False
        )
    }
