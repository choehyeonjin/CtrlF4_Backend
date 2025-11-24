import os, json, base64, io, urllib.parse, time
import boto3

# ====== 환경설정 ======
USE_DB = os.getenv("USE_DB", "0") == "1"
USE_FITZ = True   # PDF 추출용
USE_DOCX = True   # DOCX 추출용

fitz = None
docx = None
pg = None

# ====== 라이브러리 로드 ======
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import docx  # python-docx
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
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json",
}


# ====== 헬퍼 함수 ======
def _resp(code, payload):
    """API Gateway 응답"""
    return {
        "statusCode": code,
        "headers": CORS,
        "body": json.dumps(payload, ensure_ascii=False)
    }


def _parse_body(event):
    """API Gateway event body 파싱"""
    if isinstance(event, dict) and "body" not in event:
        return event
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            event = {}
    raw = event.get("body")
    if raw and event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8", errors="ignore")
        except Exception:
            pass
    try:
        return json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
    except Exception:
        return event


def _db_conn():
    """PostgreSQL 연결"""
    return pg.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        port=int(os.environ.get("PGPORT", "5432")),
        connect_timeout=3
    )


def _extract_text_and_pages(file_bytes: bytes, s3_key: str):
    """PDF / DOCX 텍스트 자동 추출"""
    ext = os.path.splitext(s3_key)[1].lower()

    # --- PDF ---
    if ext == ".pdf":
        if fitz is None or not USE_FITZ:
            return "[stub] pdf extraction skipped", 1
        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            pages = doc.page_count
            texts = [doc.load_page(i).get_text("text") for i in range(min(pages, 5))]
            return "\n\n".join(texts).strip() or "[empty pdf]", pages
        except Exception as e:
            print("❌ PDF 추출 실패:", e)
            return "[pdf error]", 0

    # --- DOCX ---
    elif ext == ".docx":
        if docx is None or not USE_DOCX:
            return "[stub] docx extraction skipped", 0
        try:
            document = docx.Document(io.BytesIO(file_bytes))
            texts = [p.text.strip() for p in document.paragraphs if p.text.strip()]
            text_joined = "\n".join(texts)
            return text_joined or "[empty docx]", len(texts)
        except Exception as e:
            print("❌ DOCX 추출 실패:", e)
            return "[docx error]", 0

    # --- Unknown ---
    else:
        return f"[{ext} not supported]", 0


# ====== 메인 핸들러 ======
def lambda_handler(event, context):
    print("=== [doc-preprocess] 시작 ===")
    USE_DB = os.getenv("USE_DB", "0") == "1"
    print("USE_DB =", USE_DB)

    try:
        # --- (1) 이벤트 타입 구분 (S3 트리거 or API 호출) ---
        if "Records" in event and "s3" in event["Records"][0]:
            # ✅ S3 업로드 트리거
            record = event["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            s3_key = urllib.parse.unquote(record["s3"]["object"]["key"])
            doc_id = 1001  # 기본값
            print(f"[S3 Event] bucket={bucket}, key={s3_key}")
        else:
            # ✅ API Gateway 직접 호출
            body = _parse_body(event)
            bucket = os.getenv("BUCKET", "ctrlf4seoul")
            s3_key = urllib.parse.unquote(body.get("s3Key", "uploads/test1.pdf"))
            doc_id = body.get("docId", 1001)
            print(f"[API Event] bucket={bucket}, key={s3_key}")

        # --- (2) S3 파일 다운로드 ---
        pdf_bytes = None
        print(f"🔍 S3 객체 요청: s3://{bucket}/{s3_key}")
        time.sleep(0.5)  # S3 업로드 직후 안정화 대기 (optional)
        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            pdf_bytes = obj["Body"].read()
            print(f"✅ S3 다운로드 완료 ({len(pdf_bytes)} bytes)")
        except Exception as e:
            print("⚠️ S3 로드 실패:", e)
            pdf_bytes = b""

        # --- (3) 텍스트 추출 ---
        text_out, pages = _extract_text_and_pages(pdf_bytes or b"", s3_key)

        # --- (4) 결과 S3 업로드 ---
        base_name = os.path.basename(s3_key)
        out_key = f"output/{base_name}.txt"
        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=text_out.encode("utf-8"),
            ContentType="text/plain; charset=utf-8"
        )
        print(f"✅ 결과 업로드 완료 → s3://{bucket}/{out_key}")

        # --- (5) DB 업데이트 (선택) ---
        if USE_DB:
            try:
                conn = _db_conn()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO documents (id, name, status)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, updated_at=NOW()
                """, (int(doc_id), os.path.basename(s3_key), "preprocessed"))
                conn.commit()
                cur.close()
                conn.close()
                print("✅ DB 업데이트 성공")
            except Exception as e:
                print("⚠️ DB 업데이트 실패:", e)

        # --- (6) 최종 응답 ---
        return _resp(200, {
            "ok": True,
            "docId": int(doc_id),
            "pages": pages,
            "outputKey": out_key,
            "useDB": USE_DB,
            "bucket": bucket,
            "message": "Preprocess 완료"
        })

    except Exception as e:
        print("❌ 전체 실패:", e)
        return _resp(500, {"ok": False, "error": str(e)})
