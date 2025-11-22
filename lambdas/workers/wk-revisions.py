# lambda_function.py  (wk-revisions)
# -*- coding: utf-8 -*-
import os, json, time, logging, psycopg2, requests
from typing import Dict, Any, List, Optional

import google.generativeai as genai
from psycopg2.extras import Json
from services.utils import trigger_run_tick

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")

# ==== Gemini ====
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel(GEMINI_MODEL)

# ==== DB 연결 ====
def db():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
        connect_timeout=5,
    )

# ==== run_results upsert ====
def upsert_result(cur, run_id: int, payload: dict):
    cur.execute("""
      INSERT INTO run_results (worker_type, run_id, payload)
      VALUES ('revision', %s, %s::jsonb)
      ON CONFLICT (worker_type, run_id)
      DO UPDATE SET payload = EXCLUDED.payload;
    """, (run_id, Json(payload)))

# ==== helper: 문서/세션/청크 ====
def get_run(cur, run_id: int) -> Optional[Dict[str, Any]]:
    cur.execute("SELECT id, session_id, doc_id FROM runs WHERE id=%s LIMIT 1", (run_id,))
    r = cur.fetchone()
    return None if not r else {"id": r[0], "session_id": r[1], "doc_id": r[2]}

def get_session(cur, session_id: int) -> Optional[Dict[str, Any]]:
    cur.execute("SELECT id, user_id, doc_id, role, answers FROM sessions WHERE id=%s LIMIT 1", (session_id,))
    r = cur.fetchone()
    if not r:
        return None
    answers = r[4]
    if isinstance(answers, str):
        try:
            answers = json.loads(answers)
        except Exception:
            answers = {}
    return {
        "id": r[0],
        "user_id": r[1],
        "doc_id": r[2],
        "role": r[3] or "",
        "answers": answers or {},
    }

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

def build_sections_by_anchor(chunks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    sections: Dict[str, Dict[str, Any]] = {}
    for ch in chunks:
        text = ch["content"] or ""
        for a in (ch["anchors"] or []):
            if not isinstance(a, dict):
                continue
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
                "id": key,
                "title": title,
                "level": level,
                "docSpan": docspan,
                "texts": [],
            })
            if not sec["texts"] or sec["texts"][-1] != piece:
                sec["texts"].append(piece)

    if not sections:
        whole = "".join([c["content"] for c in chunks])
        sections["__document__"] = {
            "id": "__document__",
            "title": "문서 전체",
            "level": 0,
            "docSpan": None,
            "texts": [whole],
        }

    for k in list(sections.keys()):
        sections[k]["text"] = "\n".join(sections[k].pop("texts"))

    return sections

# ==== 입력 파싱 (runId / sessionId / docId / inputs) ====
def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(event, dict) and "body" in event:
        raw = event.get("body")
        if isinstance(raw, str) and raw.strip():
            try:
                body = json.loads(raw)
            except Exception:
                body = {}
        elif isinstance(raw, dict):
            body = raw
        else:
            body = {}
    else:
        body = event if isinstance(event, dict) else {}

    run_id     = body.get("runId")     or body.get("run_id")
    session_id = body.get("sessionId") or body.get("session_id")
    doc_id     = body.get("docId")     or body.get("doc_id")
    inputs     = body.get("inputs", {}) or {}

    if not run_id:
        raise ValueError("runId required")
    run_id = int(run_id)

    if not (session_id and doc_id):
        # runs 테이블에서 보강
        with db() as conn, conn.cursor() as cur:
            r = get_run(cur, run_id)
            if not r:
                raise RuntimeError(f"run not found: {run_id}")
            if not session_id:
                session_id = r["session_id"]
            if not doc_id:
                doc_id = r["doc_id"]

    return {
        "run_id": run_id,
        "session_id": int(session_id),
        "doc_id": int(doc_id),
        "inputs": inputs,
    }

# ==== 프롬프트 생성 (Revision Agent) ====
def _clip(t: str, n: int) -> str:
    t = t or ""
    return t if len(t) <= n else t[:n] + " ..."

def _score_section(sec: Dict[str, Any], focus: List[str]) -> float:
    text  = (sec.get("title") or "") + "\n" + (sec.get("text") or "")
    hits  = sum(1 for f in (focus or []) if f and f in text)
    level = int(sec.get("level") or 1)
    length = len(sec.get("text") or "")
    import math
    return hits*10 + (3 - min(3, level)) + min(5.0, math.log10(max(10, length)))

def make_prompt_for_revision(
    doc_name: str,
    sections: Dict[str, Dict[str, Any]],
    role: str,
    question: str,
    focus: List[str],
    retry_info: Dict[str, Any] | None = None,
    reanalyze_text: str | None = None,
    hard_limit: int = 14,
) -> str:
    ranked = sorted(sections.values(), key=lambda s: _score_section(s, focus), reverse=True)[:hard_limit]

    sec_lines = []
    for i, sec in enumerate(ranked, 1):
        sec_lines.append(f"[{i}] {sec.get('title')}\n{_clip(sec.get('text'), 1800)}")

    vf_block = ""
    if retry_info:
        reason  = retry_info.get("reason")
        metrics = retry_info.get("metrics") or {}
        attempt = retry_info.get("attempt")
        vf_block = f"""
[검증(Verifier) 피드백]
- attempt: {attempt}
- reason: {reason or "N/A"}
- metrics: anchorRate={metrics.get("anchorRate")}, kpri={metrics.get("kpri")}, faithfulness={metrics.get("faithfulness")}

위 피드백을 충분히 반영하여, 위험/불균형/가독성 문제를 더 명확하게 짚고,
수정 문안(safe_alternative / persona_template / user_custom)을 한 단계 더 개선하라.
        """.strip()
        
    re_block = ""
    if reanalyze_text:
        re_block = f"""
[이전 사용자 분석 요약]
아래는 직전 분석(run)의 핵심 요약이다. 이 내용을 참고하되,
이전 제안에서 아쉬웠던 부분(누락된 위험, 애매한 표현 등)을 보완해
더 나은 수정 문안을 제시하라.

{reanalyze_text}
        """.strip()

    return f"""
너는 한국어 계약/약관 편집 전문가인 **Revision Agent**다.
목표는 문서 속 위험·불균형·장황·오탈자를 찾아서, **실제 계약서에 바로 넣을 수 있는 수정 문안**을 만들어 주는 것이다.

[작업 범위]

1) **위험/불균형 조항 수정**
   - 환불·요금 청구, 손해배상·책임 제한·면책, 개인정보 처리, 서비스/콘텐츠 변경권,
     이용 제한·계정 정지, 해지·분쟁·소송 제한과 관련된 위험하거나 일방적인 조항을 민감하게 탐지하라.
   - 탐지된 경우, 각 조항마다
       (a) 무엇이 왜 위험/불균형인지 (문제점·근거),
       (b) 어떤 방향으로 고쳐야 하는지,
       (c) 실제로 사용할 수 있는 **수정 문구(대체 표현)** 
     를 제시하라.
   - 출력에서 6대 리스크를 이름으로 나열할 필요는 없지만,
     위 영역에 해당하는 위험을 놓치지 말고 **수정 제안에 자연스럽게 반영**하라.

2) **분량/가독성/오탈자 중심 리라이팅**
   - 의미는 유지하되, 너무 길고 반복적인 표현은 짧고 명확하게 정리하라.
   - 오탈자, 조사 오류, 어색한 어순·중복 표현을 자연스러운 문장으로 고쳐라.
   - 한 조항 안에서 용어·표현 방식이 들쭉날쭉하면 일관되는 스타일로 통일하라.

3) **사용자 맥락 반영 (role / question / focus)**
   - role, question, focus가 있으면, 그 관점에서 특히 신경 쓰이는 부분을 우선적으로 다루고,
     이에 맞는 **맞춤형 수정 문구**를 함께 제안하라.
   - 예: “법무팀 검토용”, “세입자 보호 관점”, “위약금 줄이기” 등.

[사용자 맥락]
- role: {role or "(미지정)"}
- question: {question or "(없음)"}
- focus: {", ".join(focus) if focus else "(없음)"}

{vf_block}

{re_block}

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
        "title": "<조항 제목 또는 '문서 전체'>"
      }},
      "issues": {{
        "risk": "<해당 부분에 어떤 위험/불균형이 있는지, 특히 환불·책임·개인정보·변경·이용제한·해지/분쟁과 관련된 경우 이유를 함께 설명. 없으면 빈 문자열>",
        "length": "<장황함·중복·가독성 문제 요약. 없으면 빈 문자열>",
        "typo": "<오탈자·문장 오류·어색한 표현 요약. 없으면 빈 문자열>"
      }},
      "severity": "<low|medium|high>",
      "original_excerpt": "<수정 대상이 되는 원문 문장/문단을 그대로 포함. 필요한 범위 전체를 넣되, 문맥상 꼭 필요한 부분 위주로 작성>",
      "suggestions": {{
        "safe_alternative": "<위 위험/불균형과 표현 문제를 모두 반영한 대체 문안. 실제 계약서에 바로 넣을 수 있는 형태로 작성>",
        "persona_template": "<role/type에 따라 변수만 바꿔 쓸 수 있는 템플릿 문안(예: [당사자], [기간], [금액] 등 자리 표시자 활용)>",
        "user_custom": "<question/focus를 반영한 추가 수정안이나 변형안. 필요 없으면 간단히 빈 문자열 또는 짧은 메모>"
      }},
      "tags": [
        "risk",
        "style:length",
        "style:typo",
        "rewrite"
      ]
    }}
    ...
  ]
}}

주의:
- 반드시 JSON만 출력할 것.
- “issues.risk/length/typo”는 실제 문제가 있는 경우에만 구체적으로 적고, 없으면 빈 문자열로 두어라.
- "original_excerpt"에는 수정 대상이 되는 원문을 포함하라.
- 제안 문구는 하나의 계약 조항으로 바로 사용할 수 있을 정도로 명확하고 완결된 문장으로 작성하라.
""".strip()

def _sanitize_control_chars(s: str) -> str:
    """
    JSON 문자열 안에 들어간 비허용 control char(\n, \r, \t, 그 외 ASCII<32)를
    \\n / \\r / \\t / \\u00XX 형태로 이스케이프해서 json.loads가 먹을 수 있게 만든다.
    따옴표/백슬래시 처리도 같이 고려.
    """
    out = []
    in_str = False
    escape = False

    for ch in s:
        if escape:
            # 기존 이스케이프 그대로 통과
            out.append(ch)
            escape = False
        elif ch == "\\":
            out.append(ch)
            escape = True
        elif ch == '"':
            out.append(ch)
            in_str = not in_str
        elif in_str and ord(ch) < 32:
            # 문자열 내부의 제어문자들만 이스케이프
            if ch == "\n":
                out.append("\\n")
            elif ch == "\r":
                out.append("\\r")
            elif ch == "\t":
                out.append("\\t")
            else:
                out.append("\\u%04x" % ord(ch))
        else:
            out.append(ch)

    return "".join(out)

# ==== Gemini 호출 ====
def call_gemini_json(prompt: str) -> Dict[str, Any]:
    res = gemini_model.generate_content(
        prompt,
        generation_config=genai.types.GenerationConfig(
            response_mime_type="application/json"
        )
    )
    txt = res.text or ""
    s = txt.strip()

    # fenced code block 방어
    if s.startswith("```"):
        s = s.strip("`")
        nl = s.find("\n")
        if nl != -1:
            s = s[nl+1:].strip()

    # 1차 시도
    try:
        return json.loads(s)
    except Exception as e1:
        log.warning(f"revision JSON parse failed (1st): {e1}")

    # 2차 시도: control char sanitize 후 재시도
    try:
        s2 = _sanitize_control_chars(s)
        return json.loads(s2)
    except Exception as e2:
        log.warning(f"revision JSON parse failed (2nd): {e2}")

    # 둘 다 안 되면 최소 raw는 남겨두기
    return {
        "doc": "",
        "count": 0,
        "revisions": [],
        "raw": s[:1500],
    }

# ==== Lambda handler ====
def lambda_handler(event, context):
    t0 = time.time()
    run_id = None

    try:
        params = parse_event(event)
        run_id     = params["run_id"]
        session_id = params["session_id"]
        doc_id     = params["doc_id"]
        inputs     = params["inputs"]

        log.info(f"[wk-revisions] start run_id={run_id}, doc_id={doc_id}, session_id={session_id}, inputs={inputs}")

        with db() as conn, conn.cursor() as cur:
            # running 상태 기록
            upsert_result(cur, run_id, {"status": "running"})
            conn.commit()

            # 세션/문서/청크 로드
            session = get_session(cur, session_id)
            if not session:
                raise RuntimeError(f"session not found: {session_id}")

            s_ans = session.get("answers") or {}
            role = (inputs.get("role") or session.get("role") or s_ans.get("role") or "")
            question = (inputs.get("question") or s_ans.get("question") or "")
            focus = inputs.get("focus") or s_ans.get("focus") or []
            if isinstance(focus, str):
                focus = [focus]
            retry_info = inputs.get("retryInfo") or None
            reanalyze_text = inputs.get("prevSummaryText")

            doc_name = get_doc_name(cur, doc_id) or f"doc-{doc_id}"
            chunks   = fetch_chunks(cur, doc_id)
            sections = build_sections_by_anchor(chunks)

            prompt = make_prompt_for_revision(
                doc_name,
                sections,
                role,
                question,
                focus,
                retry_info=retry_info,
                reanalyze_text=reanalyze_text,
            )

        # LLM 호출
        out = call_gemini_json(prompt)

        doc_val = out.get("doc") or doc_name
        revs    = out.get("revisions") or []
        if not isinstance(revs, list):
            revs = []

        cnt = out.get("count")
        if not isinstance(cnt, int):
            cnt = len(revs)

        runtime_ms = int((time.time() - t0) * 1000)

        # 결과 저장
        with db() as conn, conn.cursor() as cur:
            upsert_result(cur, run_id, {
                "status": "done",
                "count": cnt,
                "revisions": revs
            })
            conn.commit()
            trigger_run_tick(run_id)

        log.info(f"[wk-revisions] done run_id={run_id}, revisions={len(revs)}, runtimeMs={runtime_ms}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "worker": "revision", "count": cnt}, ensure_ascii=False)
        }

    except Exception as e:
        log.exception("[wk-revisions] failed")
        try:
            if run_id is not None:
                with db() as conn, conn.cursor() as cur:
                    upsert_result(cur, run_id, {"status": "failed", "error": str(e)})
                    conn.commit()
                    trigger_run_tick(run_id)
        except Exception:
            pass

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
        }
