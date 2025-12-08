import os, json, base64, io, urllib.parse, time
import boto3

# ====== 환경설정 ======
USE_DB = os.getenv("USE_DB", "0") == "1"
USE_FITZ = True
USE_DOCX = True

fitz = None
docx = None
pg = None

# ====== 라이브러리 로드 ======
try:
    import fitz
except Exception:
    fitz = None

try:
    import docx
    print("✅ python-docx import OK:", docx.__version__)
except Exception as e:
    print("❌ python-docx import 실패:", e)
    docx = None

if USE_DB:
    try:
        import psycopg2
        pg = psycopg2
    except Exception:
        USE_DB = False

# ====== AWS S3 클라이언트 ======
s3 = boto3.client("s3")

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Methods": "*",
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json",
}

# ====== 응답 헬퍼 ======
def _resp(code, payload):
    return {
        "statusCode": code,
        "headers": CORS,
        "body": json.dumps(payload, ensure_ascii=False)
    }

def _parse_body(event):
    if isinstance(event, dict) and "body" not in event:
        return event
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except:
            event = {}
    raw = event.get("body")
    if raw and event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8", errors="ignore")
        except:
            pass
    try:
        return json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
    except:
        return event

def _extract_text_and_pages(file_bytes, s3_key):
    ext = os.path.splitext(s3_key)[1].lower()

    # PDF
    if ext == ".pdf":
        if fitz is None or not USE_FITZ:
            return "[stub] pdf skipped", 1
        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            pages = doc.page_count

            # 전체 페이지 텍스트 추출 (기존: min(pages, 5))
            texts = []
            for i in range(pages):
                page = doc.load_page(i)
                texts.append(page.get_text("text"))

            full_text = "\n\n".join(texts).strip()
            return full_text or "[empty pdf]", pages
        except Exception as e:
            print("❌ PDF 추출 실패:", e)
            return "[pdf error]", 0

    # DOCX
    if ext == ".docx":
        if docx is None or not USE_DOCX:
            return "[stub] docx skipped", 0
        try:
            document = docx.Document(io.BytesIO(file_bytes))
            texts = [p.text.strip() for p in document.paragraphs if p.text.strip()]
            return "\n".join(texts) or "[empty docx]", len(texts)
        except Exception as e:
            print("❌ DOCX 추출 실패:", e)
            return "[docx error]", 0

    return f"[{ext} not supported]", 0


# ==========================================================
#                     MAIN HANDLER
# ==========================================================
def lambda_handler(event, context):
    print("=== [doc-preprocess] 시작 ===")
    print("USE_DB =", USE_DB)

    try:
        # 1) API 또는 S3 이벤트 구분
        if "Records" in event and "s3" in event["Records"][0]:
            record = event["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            s3_key = urllib.parse.unquote(record["s3"]["object"]["key"])
            doc_id = 1001
            print(f"[S3 Event] bucket={bucket}, key={s3_key}")
        else:
            body = _parse_body(event)
            bucket = os.getenv("BUCKET", "ctrlf4seoul")
            s3_key = urllib.parse.unquote(body.get("s3Key", "uploads/test.pdf"))
            doc_id = body.get("docId", 1001)
            print(f"[API Event] bucket={bucket}, key={s3_key}")

        # 2) S3 다운로드
        print(f"🔍 S3 객체 요청: s3://{bucket}/{s3_key}")
        time.sleep(0.3)

        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            pdf_bytes = obj["Body"].read()
            print(f"✅ S3 다운로드 완료 ({len(pdf_bytes)} bytes)")
        except Exception as e:
            print("⚠️ S3 로드 실패:", e)
            pdf_bytes = b""

        # 3) 텍스트 추출
        text_out, pages = _extract_text_and_pages(pdf_bytes, s3_key)

        # 4) 결과 S3 업로드
        base_name = os.path.basename(s3_key)
        out_key = f"output/{base_name}.txt"
        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=text_out.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
        print(f"✅ 결과 업로드 완료 → s3://{bucket}/{out_key}")

        # 5) ❌ DB 업데이트 제거
        print("ℹ️ DB 저장 단계는 건너뜀 (요청에 따라 비활성화)")

        # 6) 최종 응답
        return _resp(200, {
            "ok": True,
            "docId": int(doc_id),
            "pages": pages,
            "outputKey": out_key,
            "bucket": bucket,
            "message": "Preprocess 완료 (DB 저장 없음)"
        })

    except Exception as e:
        print("❌ 전체 실패:", e)
        return _resp(500, {"ok": False, "error": str(e)})
