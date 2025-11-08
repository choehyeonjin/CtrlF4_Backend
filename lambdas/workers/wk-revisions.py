import os, json, logging, math
from typing import Any, Dict, List, Optional, Tuple
import boto3, psycopg2, requests
from psycopg2.extras import Json
from services.utils import trigger_run_tick

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3 = boto3.client("s3", region_name=AWS_REGION)

# 공통: ENV & DB
def need(n: str) -> str:
    v = os.getenv(n)
    if not v:
        raise RuntimeError(f"Missing env {n}")
    return v

def db():
    return psycopg2.connect(
        host=need("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=need("PGDATABASE"),
        user=need("PGUSER"),
        password=need("PGPASSWORD"),
        sslmode="require",
        connect_timeout=5,
    )
# 스키마 보장
def ensure_run_results(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS run_results (
      worker_type VARCHAR(16) NOT NULL,
      run_id      INT NOT NULL,
      payload     JSONB,
      PRIMARY KEY (worker_type, run_id)
    );
    """)

# 입력 파싱 (API GW/직접호출 모두 OK)
# - pathParameters.runId 허용
# - body.runId/sessionId/docId/inputs 허용
# - runId만 오면 runs에서 sessionId/docId 보강
def get_run(cur, run_id: int):
    cur.execute("SELECT id, session_id, doc_id FROM runs WHERE id=%s LIMIT 1", (run_id,))
    r = cur.fetchone()
    return None if not r else {"id": r[0], "session_id": r[1], "doc_id": r[2]}

def parse_event(event: Dict[str, Any]) -> Tuple[int, int, int, Dict[str, Any]]:
    # Lambda.invoke 직접 호출용 방어
    if isinstance(event, dict) and "runId" in event:
        body = event
    else:
        body = event.get("body") if isinstance(event, dict) else event
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        body = body or {}

    path = event.get("pathParameters") or {}
    run_id = body.get("runId") or body.get("run_id") or path.get("runId") or path.get("run_id")
    if run_id is None:
        raise ValueError("runId required (path /runs/{runId}/revision or body.runId)")
    run_id = int(run_id)

    session_id = body.get("sessionId") or body.get("session_id")
    doc_id     = body.get("docId") or body.get("doc_id")
    inputs     = (body.get("inputs") or {}) if isinstance(body, dict) else {}

    with db() as conn, conn.cursor() as cur:
        if session_id is None or doc_id is None:
            r = get_run(cur, run_id)
            if not r:
                raise RuntimeError(f"run not found: {run_id}")
            session_id = session_id or r["session_id"]
            doc_id     = doc_id or r["doc_id"]

    return int(run_id), int(session_id), int(doc_id), (inputs or {})


# 세션/문서/청크
def get_session(cur, session_id: int) -> Optional[Dict[str, Any]]:
    cur.execute("SELECT id, user_id, doc_id, role, answers FROM sessions WHERE id=%s LIMIT 1", (session_id,))
    r = cur.fetchone()
    if not r: return None
    answers = r[4]
    if isinstance(answers, str):
        try: answers = json.loads(answers)
        except Exception: answers = {}
    return {"id": r[0], "user_id": r[1], "doc_id": r[2], "role": r[3] or "", "answers": answers or {}}

def get_doc_name(cur, doc_id: int) -> Optional[str]:
    cur.execute("SELECT name FROM documents WHERE id=%s;", (doc_id,))
    r = cur.fetchone()
    return r[0] if r else None

def fetch_chunks(cur, doc_id: int) -> List[Dict[str, Any]]:
    """
    documents_chunks.anchors 스키마: {"count": N, "items":[...]}
    """
    cur.execute("""
      SELECT chunk_idx, content, anchors
        FROM documents_chunks
       WHERE doc_id=%s
       ORDER BY chunk_idx ASC
    """, (doc_id,))
    out = []
    for idx, content, anchors in cur.fetchall():
        items = []
        if anchors:
            if isinstance(anchors, str):
                try:
                    anchors = json.loads(anchors)
                except Exception:
                    anchors = {}
            if isinstance(anchors, dict):
                items = anchors.get("items", []) or []
        out.append({"chunk_idx": idx, "content": content or "", "anchors": items})
    return out

# 섹션 조립 (앵커별 문맥)
def build_sections_by_anchor(chunks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    sections: Dict[str, Dict[str, Any]] = {}
    for ch in chunks:
        text = ch["content"] or ""
        for a in (ch["anchors"] or []):
            key = a.get("id") or a.get("anchor_id") or a.get("title") or f"unk-{len(sections)+1}"
            title = a.get("title") or key
            level = a.get("level") or 1
            docspan = a.get("docSpan") or a.get("span")
            cspan   = a.get("chunkSpan") or a.get("chunk_span")

            piece = text
            if isinstance(cspan, dict):
                try:
                    s = max(0, int(cspan.get("start", 0)))
                    e = min(len(text), int(cspan.get("end", len(text))))
                    if 0 <= s < e <= len(text):
                        piece = text[s:e]
                except Exception:
                    pass

            sec = sections.setdefault(key, {
                "id": key, "title": title, "level": level, "docSpan": docspan, "texts": []
            })
            if not sec["texts"] or sec["texts"][-1] != piece:
                sec["texts"].append(piece)

    if not sections:
        whole = "".join([c["content"] for c in chunks])
        sections["__document__"] = {"id": "__document__", "title": "문서 전체", "level": 0, "docSpan": None, "texts": [whole]}

    for k in list(sections.keys()):
        sections[k]["text"] = "\n".join(sections[k].pop("texts"))

    return sections

# Gemini
def call_gemini(prompt: str) -> str:
    api_key = need("GEMINI_API_KEY")
    model   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    url     = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    r = requests.post(f"{url}?key={api_key}", json=payload, timeout=int(os.getenv("GEMINI_TIMEOUT","120")))
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"]

# 프롬프트 (I-4.1 ~ I-4.3 범위 반영)
# - 위험 조항/분량/오타/반복표현 감지 & 대체표현
# - role/type(세션/문서 맥락) 반영 템플릿
# - 사용자 요청 기반 custom 수정
def _clip(t: str, n: int) -> str:
    t = t or ""
    return t if len(t) <= n else t[:n] + " ..."

def _score_section(sec: Dict[str, Any], focus: List[str]) -> float:
    text  = (sec.get("title") or "") + "\n" + (sec.get("text") or "")
    hits  = sum(1 for f in (focus or []) if f and f in text)
    level = int(sec.get("level") or 1)
    length = len(sec.get("text") or "")
    return hits*10 + (3 - min(3, level)) + min(5.0, math.log10(max(10, length)))

def make_prompt(doc_name: str,
                sections: Dict[str, Dict[str, Any]],
                role: str, question: str, focus: List[str],
                hard_limit: int = 14) -> str:
    ranked = sorted(sections.values(), key=lambda s: _score_section(s, focus), reverse=True)[:hard_limit]

    sec_lines = []
    for i, sec in enumerate(ranked, 1):
        sec_lines.append(f"[{i}] {sec.get('title')}\n{_clip(sec.get('text'), 1800)}")

    return f"""
너는 한국어 계약/약관 편집 전문가인 **Revision Agent**다. 아래 섹션들을 검토하여 다음을 수행하라.

[작업 범위]
1) (I-4.1) 위험/불균형 조항, 과도한 분량, 오타·문장 오류·중복표현을 찾아 **대체 표현**을 제안.
2) (I-4.2) 제공된 role/type 맥락을 반영한 **수정안 템플릿**을 생성. (변수/괄호 자리표시자 활용)
3) (I-4.3) 사용자의 요청(question/focus)을 반영한 **맞춤 수정**도 함께 제안.

[사용자 맥락]
- role: {role or "(미지정)"}
- question: {question or "(없음)"}
- focus: {", ".join(focus) if focus else "(없음)"}

[문서] {doc_name}
[검토 섹션 샘플]
{chr(10).join(sec_lines)}

[출력 형식: JSON만]
{{
  "doc": "{doc_name}",
  "count": <number>,
  "revisions": [
    {{
      "anchor": {{
        "id": "<anchor_id or null>",
        "title": "<조항 제목 or '문서 전체'>"
      }},
      "issues": {{
        "risk": "<위험/불균형 요약 또는 empty>",
        "length": "<장황/중복/가독성 문제 요약 또는 empty>",
        "typo": "<오타/문장 오류 요약 또는 empty>"
      }},
      "severity": "<low|medium|high>",
      "original_excerpt": "<원문 일부(<=240자)>",
      "suggestions": {{
        "safe_alternative": "<안전하고 균형 잡힌 대체 문안>",
        "persona_template": "<role/type 반영 템플릿(변수 자리지정)>",
        "user_custom": "<question/focus 반영 맞춤 수정안>"
      }},
      "tags": ["risk:refund", "privacy", "style:length", "style:typo"]
    }}
    ...
  ]
}}

주의:
- JSON 이외 텍스트 출력 금지.
- 원문 인용 240자 이내. 제안문은 간결하고 명확하게.
""".strip()

# runs_results 저장
def upsert_result(cur, run_id: int, payload: Dict[str, Any]):
    cur.execute("""
      INSERT INTO run_results (worker_type, run_id, payload)
      VALUES ('revision', %s, %s)
      ON CONFLICT (worker_type, run_id)
      DO UPDATE SET payload = EXCLUDED.payload;
    """, (run_id, Json(payload)))

# 핸들러
def lambda_handler(event, context):
    try:
        run_id, session_id, doc_id, inputs = parse_event(event)

        # 시작: running
        with db() as conn, conn.cursor() as cur:
            ensure_run_results(cur)
            upsert_result(cur, run_id, {"status": "running"})
            conn.commit()

        role_in     = inputs.get("role")
        question_in = inputs.get("question")
        focus_in    = inputs.get("focus", [])

        with db() as conn, conn.cursor() as cur:
            ensure_run_results(cur)

            session = get_session(cur, session_id)
            if not session:
                return {"statusCode": 404, "body": json.dumps({"ok": False, "error": "session not found"})}

            s_ans = session.get("answers") or {}
            role = role_in or session.get("role") or ""
            question = question_in or (s_ans.get("question") if isinstance(s_ans, dict) else "")
            focus = (focus_in if isinstance(focus_in, list) else []) or (s_ans.get("focus") if isinstance(s_ans, dict) else []) or []

            doc_name = get_doc_name(cur, doc_id) or f"doc-{doc_id}"
            chunks   = fetch_chunks(cur, doc_id)
            sections = build_sections_by_anchor(chunks)

            prompt = make_prompt(doc_name, sections, role, question, focus)

        # Gemini 호출
        raw = call_gemini(prompt)

        # json 파싱
        parsed = try_parse_json(raw)
        if not parsed:
            parsed = {"doc": doc_name, "count": 0, "revisions": [], "raw": _clip(raw, 1500)}

        # 성공: done
        log.info(f"wk-revisions done: {run_id} {doc_id} {len(parsed.get('revisions') or [])}")
        with db() as conn, conn.cursor() as cur:
            upsert_result(cur, run_id, {"status": "done", **parsed})
            conn.commit()
            trigger_run_tick(run_id)

        return {"statusCode": 200, "body": json.dumps({"ok": True, "worker": "revision"})}

    except Exception as e:
        # 실패: failed
        log.exception("wk-revisions failed")
        try:
            # 가능하면 runId 재추출
            try:
                rid, *_ = parse_event(event)
                run_id_fallback = rid
            except Exception:
                run_id_fallback = None

            if run_id_fallback is not None:
                with db() as conn, conn.cursor() as cur:
                    upsert_result(cur, run_id_fallback, {"status": "failed", "error": str(e)})
                    conn.commit()
                    trigger_run_tick(run_id)
        except Exception:
            pass

        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)})}

def try_parse_json(t: str) -> Optional[Dict[str, Any]]:
    try:
        s = (t or "").strip()
        if s.startswith("```"):
            s = s.strip("`")
            nl = s.find("\n")
            if nl != -1:
                s = s[nl+1:]
        # JSON 배열을 통째로 주는 경우도 방어
        if s and s[0] in "[{":
            return json.loads(s)
        return None
    except Exception:
        return None
