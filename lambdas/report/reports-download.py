import os
import json
import boto3
import psycopg2

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=boto3.session.Config(signature_version="s3v4")
)

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

def lambda_handler(event, context):
    print("EVENT:", event)

    # body 파싱 (POST 전용)
    body = event.get("body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except:
            body = {}
    if body is None:
        body = {}

    # GET/POST 공통 path params
    path_params = event.get("pathParameters") or {}
    report_id = path_params.get("reportId")

    # path → body 순으로 찾기
    report_id = path_params.get("reportId") or body.get("reportId")

    if not report_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"msg": "reportId required"})
        }

    # DB 조회
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT s3_key FROM reports WHERE id = %s", (report_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {
            "statusCode": 404,
            "body": json.dumps({"msg": "report not found"})
        }

    s3_key = row[0]

    # presigned URL 생성
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": REPORT_BUCKET, "Key": s3_key},
        ExpiresIn=3600
    )

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps({
            "reportId": report_id,
            "url": url
        })
    }
