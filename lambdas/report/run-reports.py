import os
import json
import uuid
import re
import boto3
import psycopg2
import subprocess
import tempfile
from jinja2 import Environment, FileSystemLoader
import unicodedata

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=boto3.session.Config(
        signature_version="s3v4",
        s3={"addressing_style": "virtual"}
    )
)

RESULT_BUCKET = os.environ["RESULT_BUCKET"]
REPORT_BUCKET = os.environ["REPORT_BUCKET"]

DB_HOST = os.environ["PGHOST"]
DB_USER = os.environ["PGUSER"]
DB_PASSWORD = os.environ["PGPASSWORD"]
DB_NAME = os.environ["PGDATABASE"]
DB_PORT = os.environ.get("PGPORT", 5432)

def db_connect():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )


# ---------------------------
# 파일명 → 사람이 읽기 좋은 제목
# ---------------------------
def humanize_filename(filename: str) -> str:
    if not filename:
        return "문서"

    filename = unicodedata.normalize("NFC", filename)

    name = os.path.splitext(filename)[0]
    name = re.sub(r'[_-][0-9a-fA-F]{6,}$', '', name)
    name = name.replace("_", " ").replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()

    return name


# ---------------------------
# run_id → 파일명 조회
# ---------------------------
def get_filename_of_run(run_id):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT d.name
        FROM runs r
        JOIN documents d ON r.doc_id = d.id
        WHERE r.id = %s
    """, (run_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


# ---------------------------
# run_results 로드
# ---------------------------
def load_run_results_from_db(run_id):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT worker_type, payload
        FROM run_results
        WHERE run_id = %s
    """, (run_id,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    combined = {}

    for worker_type, payload in rows:
        combined[worker_type] = payload  # 그대로 저장

    return combined


# ---------------------------
# HTML 렌더링
# ---------------------------
env = Environment(
    loader=FileSystemLoader(os.path.join(os.getcwd(), "templates")),
    autoescape=True
)

def render_html(title, summaryLine, results):
    template = env.get_template("report.html")

    # --- summarizer ---

    # ─────────────────────────
    # 1) summary 루트 찾기
    #    - worker_type 이 "summary" 가 아닐 수도 있어서 여러 키 시도
    # ─────────────────────────
    summary_root = (
        results.get("summary")
        or results.get("summarizer")
        or results.get("summarizer_v2")
        or results.get("summary_worker")
        or {}
    )

    # 디버깅용 (CloudWatch에서 한번 확인해보면 좋음)
    print("[DEBUG] SUMMARY_ROOT KEYS:", type(summary_root), getattr(summary_root, "keys", lambda: None)())
    try:
        print("[DEBUG] SUMMARY_ROOT RAW:", json.dumps(summary_root, ensure_ascii=False)[:1000])
    except Exception:
        pass

    summary_text = ""

    # ─────────────────────────
    # 2) dict 인 경우 여러 케이스 커버
    #    - { "results": { "full_document": { "summary": ... } } }
    #    - { "full_document": { "summary": ... } }
    #    - { "summary": "...", "anchors": ... }
    # ─────────────────────────
    if isinstance(summary_root, dict):
        # a) results 밑에 있을 수도 있고, 아니면 바로 있을 수도 있음
        container = summary_root.get("results") or summary_root

        # b) full_document 안에 있을 수도 있음
        full_doc = container.get("full_document") or {}

        summary_text = (
            full_doc.get("summary")              # 1순위: full_document.summary
            or container.get("summary")          # 2순위: summary 바로 있음
            or container.get("full_summary")     # 3순위: full_summary 같은 키
            or ""
        )

    # ─────────────────────────
    # 3) summary_root 자체가 문자열인 경우
    # ─────────────────────────
    elif isinstance(summary_root, str):
        summary_text = summary_root

    # 안전 장치
    summary_text = str(summary_text or "")

    summary_lines = [
        s.strip()
        for s in summary_text.split("\n")
        if s.strip()
    ]

    print("[DEBUG] SUMMARY_TEXT:", summary_text[:200])
    print("[DEBUG] SUMMARY_LINES:", summary_lines)

    # --- risk ---
    risk_items = (results.get("risk") or {}).get("items", [])

    # --- QA ---
    qa_raw = results.get("qa") or {}

    if isinstance(qa_raw, dict):
        if "pairs" in qa_raw and isinstance(qa_raw["pairs"], list):
            qa_pairs = qa_raw["pairs"]
        elif "question" in qa_raw and "answer" in qa_raw:
            qa_pairs = [qa_raw]
        else:
            qa_pairs = []
    else:
        qa_pairs = []


    # --- Revision (★ 추가) ---
    revisions = (results.get("revision") or {}).get("revisions", [])

    return template.render(
        title=title,
        summaryLine=summaryLine,
        summary=summary_lines,
        risk=risk_items,
        qa=qa_pairs,
        revisions=revisions   # ★ 템플릿에 전달
    )



# ---------------------------
# HTML → PDF
# ---------------------------
def html_to_pdf(html):
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
        f.write(html.encode("utf-8"))
        f.flush()
        html_path = f.name

    output_fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
    os.close(output_fd)

    cmd = ["wkhtmltopdf", "--encoding", "UTF-8", html_path, pdf_path]
    subprocess.run(cmd, check=True)

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    os.remove(html_path)
    os.remove(pdf_path)

    return pdf_bytes


# ---------------------------
# presigned URL
# ---------------------------
def create_presigned(key):
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": REPORT_BUCKET, "Key": key},
        ExpiresIn=3600
    )


# ---------------------------
# Lambda Handler
# ---------------------------
def handler(event, context):
    print("EVENT:", event)
    
    raw_body = event.get("body")

    if raw_body:
        if isinstance(raw_body, str):
            try:
                body = json.loads(raw_body)
            except:
                body = {}
        else:
            body = raw_body
    else:
        # Proxy가 아닌 REST Integration일 경우 body 없이 파라미터가 최상위에 들어옴
        body = event

    # runId 우선순위: body → path → 직접 event
    run_id = (
        body.get("runId")
        or (event.get("pathParameters") or {}).get("runId")
        or event.get("runId")
    )

    summaryLine = (
        body.get("summaryLine")
        or event.get("summaryLine")
        )

    if not run_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"msg": "runId required"})
        }

    # 파일명 → 제목 자동 생성
    filename = get_filename_of_run(run_id)
    pretty_title = humanize_filename(filename)
    title = f"{pretty_title} 분석 보고서"

    # run_results 전체 로딩
    results = load_run_results_from_db(run_id)

    # HTML 생성
    html = render_html(title, summaryLine, results)

    # PDF 생성
    pdf_bytes = html_to_pdf(html)

    # S3 저장
    s3_key = f"reports/{uuid.uuid4()}.pdf"
    s3.put_object(
        Bucket=REPORT_BUCKET,
        Key=s3_key,
        Body=pdf_bytes,
        ContentType="application/pdf"
    )

    # DB 보고서 기록
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO reports (run_id, s3_key, title, summary_line)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (run_id, s3_key, title, summaryLine))
    report_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    # PRESIGNED URL
    download_url = create_presigned(s3_key)


    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Credentials": "true",
        },
        "body": json.dumps({
            "reportId": report_id,
            "download": download_url
        })
    }
