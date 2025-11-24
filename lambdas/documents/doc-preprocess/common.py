import json, base64

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json",
}

def resp(code, payload):
    return {"statusCode": code, "headers": CORS_HEADERS, "body": json.dumps(payload, ensure_ascii=False)}

def parse_body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8", errors="ignore")
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}
