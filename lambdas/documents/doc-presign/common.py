# common.py
import json, base64

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",              # 개발 단계: *
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json",
}

def resp(code: int, payload: dict):
    return {"statusCode": code, "headers": CORS_HEADERS, "body": json.dumps(payload, ensure_ascii=False)}

def parse_body(event):
    """API GW 프록시 통합에서 body를 안전하게 파싱"""
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8", errors="ignore")
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}
