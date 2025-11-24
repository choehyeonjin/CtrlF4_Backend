import os, json, logging, re, time
from typing import List, Dict, Any, Tuple
import boto3, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
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

logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)

# --------- ENV / CONST ----------
BUCKET        = os.environ.get("BUCKET", "ctrlf4seoul")
OPENAI_SDK    = os.environ.get("OPENAI_SDK", "v1")
EMBED_MODEL   = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
DIM           = int(os.environ.get("PGVECTOR_DIM", "1536"))
CHUNK_SIZE    = int(os.environ.get("CHUNK_SIZE", "1500"))
CHUNK_OVERLAP = int(os.environ.get("CHUNK_OVERLAP", "200"))
EMBED_BATCH   = int(os.environ.get("EMBED_BATCH", "64"))

S3 = boto3.client("s3")

# --------- Response Helper (CORS 적용) ----------
def response(status: int, body: dict):
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

def need(n):
    v = os.getenv(n)
    if not v:
        raise RuntimeError(f"Missing env {n}")
    return v

def db():
    return psycopg2.connect(
        host=need("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "ctrlf4"),
        user=os.getenv("PGUSER", "ctrlf4"),
        password=need("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5
    )

# --------- DB helpers ----------
def get_doc_name(cur, doc_id: int) -> str | None:
    cur.execute("SELECT name FROM documents WHERE id=%s;", (doc_id,))
    r = cur.fetchone()
    return r[0] if r else None

def set_status(cur, doc_id: int, status: str):
    cur.execute("UPDATE documents SET status=%s, updated_at=NOW() WHERE id=%s;", (status, doc_id))

def has_chunks(cur, doc_id: int) -> bool:
    cur.execute("SELECT EXISTS (SELECT 1 FROM documents_chunks WHERE doc_id=%s);", (doc_id,))
    return cur.fetchone()[0]

def ensure_anchors_column(cur):
    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='documents_chunks' AND column_name='anchors'
        LIMIT 1;
    """)
    if cur.fetchone() is None:
        log.info("ADD COLUMN anchors jsonb")
        cur.execute("ALTER TABLE public.documents_chunks ADD COLUMN anchors jsonb;")

def get_chunk_row(cur, doc_id: int, idx: int) -> dict | None:
    cur.execute("""
        SELECT doc_id, chunk_idx, content, embedding::text AS embedding, meta, anchors
        FROM documents_chunks
        WHERE doc_id=%s AND chunk_idx=%s
        LIMIT 1;
    """, (doc_id, idx))
    r = cur.fetchone()
    if not r:
        return None
    return {
        "doc_id": r[0],
        "chunk_idx": r[1],
        "content": r[2],
        "embedding": r[3],
        "meta": r[4],
        "anchors": r[5],
    }

# --------- IO / chunking ----------
def read_text(bucket: str, key: str) -> str:
    obj = S3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="ignore")

def chunk_text(txt: str, size: int, overlap: int) -> List[Tuple[int, int, str]]:
    out = []
    i = 0
    n = len(txt)
    while i < n:
        j = min(n, i + size)
        out.append((i, j, txt[i:j]))
        if j >= n:
            break
        i += size - overlap
    return out

# --------- Anchor extraction ----------
RE_ARTICLE = re.compile(r'(제\s*\d+\s*조(?:\s*\([^)]+\))?)', re.M)
RE_SECTION = re.compile(r'(제\s*\d+\s*항|[①-⑳]|[1-9][0-9]?[)\.])', re.M)
RE_ITEM    = re.compile(r'([0-9]+[\.]|[(][0-9]+[)])', re.M)

def _find_spans(pattern: re.Pattern, text: str) -> List[Tuple[str, int, int]]:
    items = []
    for m in pattern.finditer(text):
        title = m.group(0).strip()
        s = m.start()
        items.append((title, s, -1))
    for i in range(len(items)):
        title, s, _ = items[i]
        e = items[i + 1][1] if i + 1 < len(items) else len(text)
        items[i] = (title, s, e)
    return items

def extract_anchors(full_text: str) -> List[Dict[str, Any]]:
    A: List[Dict[str, Any]] = []
    arts = _find_spans(RE_ARTICLE, full_text)
    for ai, (atitle, astart, aend) in enumerate(arts, start=1):
        art_id = f"art-{ai}"
        A.append({
            "anchor_id": art_id,
            "level": 1,
            "title": atitle,
            "path": atitle,
            "span": {"start": astart, "end": aend}
        })
        sec_block = full_text[astart:aend]
        secs = _find_spans(RE_SECTION, sec_block)
        for si, (stitle, sstart_rel, ssend_rel) in enumerate(secs, start=1):
            sstart = astart + sstart_rel
            ssend = astart + ssend_rel
            sec_id = f"{art_id}-sec-{si}"
            A.append({
                "anchor_id": sec_id,
                "level": 2,
                "title": stitle,
                "path": f"{atitle}>제{si}항",
                "span": {"start": sstart, "end": ssend}
            })
            item_block = full_text[sstart:ssend]
            items = _find_spans(RE_ITEM, item_block)
            for ii, (ititle, istart_rel, iend_rel) in enumerate(items, start=1):
                istart = sstart + istart_rel
                iend = sstart + iend_rel
                item_id = f"{sec_id}-item-{ii}"
                A.append({
                    "anchor_id": item_id,
                    "level": 3,
                    "title": ititle,
                    "path": f"{atitle}>제{si}항>{ii}호",
                    "span": {"start": istart, "end": iend}
                })
    return A

def anchors_for_chunk(anchors_doclevel: List[Dict[str, Any]], cs: int, ce: int) -> Dict[str, Any]:
    items = []
    for a in anchors_doclevel:
        s = a["span"]["start"]
        e = a["span"]["end"]
        inter_s, inter_e = max(s, cs), min(e, ce)
        if inter_s < inter_e:
            items.append({
                "id": a["anchor_id"],
                "level": a.get("level", 1),
                "title": a["title"],
                "path": a.get("path"),
                "docSpan": {"start": s, "end": e},
                "chunkSpan": {"start": inter_s - cs, "end": inter_e - cs}
            })
    return {"count": len(items), "items": items} if items else {"count": 0, "items": []}

# --------- S3 key resolver (랜덤 파일명 대응) ----------
import time

def resolve_text_key(bucket: str, doc_name: str) -> str:
    """S3 내 output 파일 자동 탐색 (랜덤명 + 업로드 지연 대응)"""
    base = os.path.basename(doc_name)
    stem, ext = os.path.splitext(base)

    for attempt in range(3):  # 최대 3회 재시도
        try:
            # --- 1) 정밀 매칭 후보 ---
            candidates = [
                f"output/{base}.txt",
                f"output/{stem}.txt",
                f"output/{stem}{ext}.txt",  # 예: stem이 '계약서_595a00b3' + .pdf
            ]
            for key in candidates:
                try:
                    S3.head_object(Bucket=bucket, Key=key)
                    print(f"exact match found: {key}")
                    return key
                except Exception:
                    continue

            # --- 2) prefix 탐색 (랜덤명 + 최신 버전 선택) ---
            prefix = f"output/{stem}"
            if not prefix.endswith(ext):
                prefix += ext  # 확장자 누락 방지

            best_key = None
            best_time = datetime.fromtimestamp(0, tz=timezone.utc)
            token = None

            while True:
                resp = S3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=token
                ) if token else S3.list_objects_v2(Bucket=bucket, Prefix=prefix)

                for obj in resp.get("Contents", []):
                    k = obj["Key"]
                    if k.endswith(".txt"):
                        lm = obj.get("LastModified", datetime.fromtimestamp(0, tz=timezone.utc))
                        if lm > best_time:
                            best_time = lm
                            best_key = k

                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                else:
                    break

            if best_key:
                print(f"Found latest S3 key: {best_key}")
                return best_key

            raise FileNotFoundError(f"S3 파일을 찾지 못했습니다 (prefix={prefix})")

        except FileNotFoundError as e:
            if attempt < 2:
                print(f"[{attempt+1}/3] 파일 아직 S3에서 안 보임, 5초 후 재시도...")
                time.sleep(5)
            else:
                raise e

    base = os.path.basename(doc_name)
    stem, ext = os.path.splitext(base)
    candidates = [
        f"output/{base}.txt",
        f"output/{stem}.txt",
    ]
    for key in candidates:
        try:
            S3.head_object(Bucket=bucket, Key=key)
            print(f"exact match found: {key}")
            return key
        except Exception:
            continue

    prefix = f"output/{stem}"
    best_key = None
    best_time = datetime.fromtimestamp(0, tz=timezone.utc)
    token = None

    while True:
        resp = S3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token) if token else S3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".txt"):
                lm = obj.get("LastModified", datetime.fromtimestamp(0, tz=timezone.utc))
                if lm > best_time:
                    best_time = lm
                    best_key = k
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    if best_key:
        print(f"Found latest S3 key: {best_key}")
        return best_key
    else:
        raise FileNotFoundError(f"S3 파일을 찾지 못했습니다 (prefix={prefix})")

# --------- Embedding ----------
_openai = None
def get_openai():
    global _openai
    if _openai:
        return _openai
    api_key = need("OPENAI_API_KEY")
    if OPENAI_SDK == "v1":
        from openai import OpenAI
        _openai = ("v1", OpenAI(api_key=api_key))
    else:
        import openai as v0
        v0.api_key = api_key
        _openai = ("v0", v0)
    return _openai

def embed_texts(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []
    sdk, client = get_openai()
    out = []
    if sdk == "v1":
        for i in range(0, len(texts), EMBED_BATCH):
            r = client.embeddings.create(model=EMBED_MODEL, input=texts[i:i + EMBED_BATCH])
            out.extend([d.embedding for d in r.data])
    else:
        for i in range(0, len(texts), EMBED_BATCH):
            r = client.Embedding.create(model=EMBED_MODEL, input=texts[i:i + EMBED_BATCH])
            out.extend([d["embedding"] for d in r["data"]])
    if out and len(out[0]) != DIM:
        raise RuntimeError(f"dim mismatch {len(out[0])}!={DIM}")
    return out

# --------- Upsert ----------
def upsert_chunks(cur, doc_id: int, chunks, embs, meta, A):
    ensure_anchors_column(cur)
    rows = []
    for idx, (gs, ge, content) in enumerate(chunks):
        per_chunk = anchors_for_chunk(A, gs, ge)
        rows.append((
            doc_id, idx, content, embs[idx],
            json.dumps(meta),
            json.dumps(per_chunk)
        ))
    sql = """
    INSERT INTO documents_chunks (doc_id, chunk_idx, content, embedding, meta, anchors)
    VALUES %s
    ON CONFLICT (doc_id, chunk_idx) DO UPDATE
      SET content=EXCLUDED.content,
          embedding=EXCLUDED.embedding,
          meta=EXCLUDED.meta,
          anchors=EXCLUDED.anchors;
    """
    execute_values(cur, sql, rows, template="(%s,%s,%s,%s::vector,%s::jsonb,%s::jsonb)")

def get_doc_owner_and_name(cur, doc_id: int):
    cur.execute("SELECT user_id, name FROM documents WHERE id=%s;", (doc_id,))
    r = cur.fetchone()
    if not r:
        return None, None
    return r[0], r[1]

# --------- Handler ----------
def lambda_handler(event, context):
    try:
        if event.get("httpMethod", "").upper() == "OPTIONS":
            return response(200, {"ok": True, "message": "CORS preflight success"})

        try:
            user_id = get_auth_user_id(event)
        except AuthError as e:
            return response(401, {"ok": False, "error": str(e)})

        path = event.get("pathParameters") or {}
        qsp = event.get("queryStringParameters") or {}
        if not path.get("docId"):
            return response(400, {"ok": False, "error": "missing docId"})

        doc_id = int(path["docId"])
        force = (qsp.get("force", "false").lower() == "true") if qsp else False

        # row 모드
        if qsp and qsp.get("row") is not None:
            try:
                idx = int(qsp["row"])
            except Exception:
                return response(400, {"ok": False, "error": "invalid row"})
            with db() as conn, conn.cursor() as cur:
                ensure_anchors_column(cur)
                name = get_doc_name(cur, doc_id)
                if not name:
                    return response(404, {"ok": False, "error": "document not found"})
                row = get_chunk_row(cur, doc_id, idx)
                if not row:
                    return response(404, {"ok": False, "error": "chunk not found"})
                return response(200, {"ok": True, **row})

        # 임베딩 수행
        with db() as conn, conn.cursor() as cur:
            owner_id, name = get_doc_owner_and_name(cur, doc_id)
            if not name:
                return response(404, {"ok": False, "error": "document not found"})
            if owner_id != user_id:
                return response(403, {"ok": False, "error": "forbidden"})
                
            if has_chunks(cur, doc_id) and not force:
                return response(200, {
                    "ok": True, "skipped": True, "reason": "chunks_exist",
                    "docId": doc_id, "docName": name
                })

        # 랜덤 파일명 대응 로직 적용
        text_key = resolve_text_key(BUCKET, name)

        full = read_text(BUCKET, text_key)
        anchors = extract_anchors(full)
        chunks = chunk_text(full, CHUNK_SIZE, CHUNK_OVERLAP)
        embs = embed_texts([c[2] for c in chunks])
        meta = {"bucket": BUCKET, "key": text_key, "model": EMBED_MODEL, "dim": DIM}

        with db() as conn, conn.cursor() as cur:
            upsert_chunks(cur, doc_id, chunks, embs, meta, anchors)
            set_status(cur, doc_id, "embeded")
            conn.commit()

        return response(200, {
            "ok": True, "skipped": False,
            "docId": doc_id, "docName": name,
            "status": "embeded", "chunks": len(chunks),
            "anchorsTotal": len([a for a in anchors if a.get("level") == 1])
        })

    except Exception as e:
        log.exception("doc-embedding failed")
        return response(500, {"ok": False, "error": f"{type(e).__name__}: {str(e)}"})
