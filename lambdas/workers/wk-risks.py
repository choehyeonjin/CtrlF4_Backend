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
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-3-pro-preview")
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel(GEMINI_MODEL)

# ==== DB 연결 ====
def db():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=os.environ.get("PGPORT","5432"),
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
        connect_timeout=5,
    )

# ==== run_results upsert ====
def upsert_result(cur, run_id: int, payload: dict):
    cur.execute("""
      INSERT INTO run_results (worker_type, run_id, payload)
      VALUES ('risk', %s, %s::jsonb)
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

# ==== 프롬프트 생성 ====
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

def make_prompt_for_risk(
    doc_name: str,
    sections: Dict[str, Dict[str, Any]],
    role: str,
    question: str,
    focus: List[str],
    retry_info: Dict[str, Any] | None = None,
    reanalyze_text: str | None = None,
    hard_limit: int | None = None,
) -> str:
    ranked = sorted(sections.values(), key=lambda s: _score_section(s, focus), reverse=True)[:hard_limit]

    sec_lines = []
    for i, sec in enumerate(ranked, 1):
        sec_lines.append(
            f"[{i}] {sec.get('title')}\n(anchor_id={sec.get('id')})\n{_clip(sec.get('text'), 1800)}"
        )

    vf_block = ""
    if retry_info:
        reason = retry_info.get("reason")
        metrics = retry_info.get("metrics") or {}
        attempt = retry_info.get("attempt")
        vf_block = f"""
[검증(Verifier) 피드백]
- attempt: {attempt}
- reason: {reason or "N/A"}
- metrics: anchorRate={metrics.get("anchorRate")}, kpri={metrics.get("kpri")}, faithfulness={metrics.get("faithfulness")}

이 피드백을 고려하여, **위험 조항을 놓치지 않는 것(리콜 극대화)**를 최우선 목표로 삼아라.
조금 과해 보이더라도 애매한 조항은 severity="low"로라도 포함하고, anchor(조항 단위) 연결을 명확히 하라.
""".strip()

    re_block = ""
    if reanalyze_text:
        re_block = f"""
[이전 사용자 분석 요약]
아래는 직전 분석(run)의 핵심 요약이다. 이 내용을 참고하되,
놓친 위험이 없는지, 과도하거나 부족한 판단이 없는지 다시 점검하라.

{reanalyze_text}
        """.strip()

    focus_str = ", ".join(focus) if focus else "(없음)"

    return f"""
(주의: 이 분석은 법률 자문이 아니며, 계약서의 독소조항(리스크)을 탐지하기 위한 보조 도구일 뿐이다.)

너는 한국어 계약/약관 전문 변호사 역할의 **Risk Detector Agent**다.

이번 작업의 **가장 중요한 목표는 "위험 조항을 최대한 놓치지 않는 것(리콜 극대화)"**이다.
조금 과할지라도, 잠재적 위험 가능성이 있는 조항은 일단 모두 잡아내고,
정말 문제가 없다고 확신되는 조항만 제외하라.

[독소조항 체크리스트]
아래 유형이 하나라도 보이면 반드시 리스크로 등록하라. 여러 유형이 섞여 있으면 각각 분리해도 좋다.
목표는 아래 문서에서 **6대 핵심 리스크**를 중심으로 위험 조항을 찾아내고,
각 위험을 관련 조항(앵커)에 정확히 연결하는 것이다.

1) 요금·환불·자동결제
   - 자동 갱신, 자동 결제, 선급금/보증금/MG의 환불 불가, 일방적 요금 변경, 위약금·지연손해금 과다
2) 책임상한·간접손해·면책
   - "어떠한 경우에도 책임지지 않는다", "간접·특별·결과적 손해 배제", 손해배상 상한(최근 n개월 요금 등),
     과도하게 좁은 보증, 이용자에게 과도한 면책·배상 책임을 지우는 조항
3) 개인정보·정보보호·데이터 이용
   - 개인정보/데이터를 광범위하게 이용·제공·2차 활용·AI 학습에 사용하는 조항,
     삭제/파기·보호 의무가 약하거나 책임이 제한된 조항
4) 서비스/콘텐츠 변경·정지권
   - 사업자 임의로 서비스/콘텐츠를 변경·중단할 수 있고, 이용자는 사실상 구제 수단이 없는 조항
5) 이용제한·계정정지
   - 모호한 기준으로 계정 정지/이용 제한을 할 수 있고, 통지·이의제기·복구 절차가 부족한 조항
6) 해지·분쟁·소송제한
   - 한쪽만 편한 해지권(편의해지), 과도한 위약벌, 매우 짧은 청구 기한(예: 3~6개월),
     특정 법원·중재기관만을 강제, 집단소송·소송 권리 제한
7) 지식재산·개선결과물·저작인격권
   - 2차적저작물·개선 결과물(IP)이 일방 당사자에게 전부 귀속,
     저작인격권 포기, 과도한 수정·검수 요구 권한
8) 기타
   - 위 항목에 딱 맞지 않더라도, 상대방에게 명백히 불리하거나
     추후 분쟁 시 큰 리스크가 될 수 있는 조항은 모두 "기타"로라도 포함하라.

[사용자 맥락]
- role: {role or "(미지정)"}
- question: {question or "(없음)"}
- focus: {focus_str}

{vf_block}

{re_block}

[분석 방법 지침]

1. 먼저 위 체크리스트를 기준으로, 독소조항이 될 수 있는 부분이 있는지 조항별로 훑어보라.
2. 애매하더라도 "상대방에게 불리한 구조"라면 일단 리스크로 등록하고 severity="low"로 표시하라.
3. 같은 조항(같은 anchor_id) 안에서 서로 다른 유형의 리스크가 섞여 있으면,
   하나로 합치지 말고 각각 별도의 item으로 분리하라.
4. 각 item에는 반드시 가장 관련성이 높은 anchor_id를 하나만 지정하라.
   - anchor_id는 아래 [검토 섹션]의 (anchor_id=...) 값 중 하나여야 한다.
   - 문단이 여러 조항에 걸쳐 있어도, 핵심 내용과 가장 직접적으로 연결된 anchor_id를 선택하라.
5. 전체적으로 **최소 10개 이상** 리스크 후보를 적극적으로 찾아보고,
   문서 특성상 리스크가 많은 경우 15~25개까지도 허용된다.

[입력 문서] {doc_name}
아래는 조항(anchor)별로 정리된 텍스트이다.
각 블록의 (anchor_id=...) 를 사용하여, 어떤 조항에서 위험이 나오는지 정확히 표시하라.

[검토 섹션]
{chr(10).join(sec_lines)}

[출력 형식: JSON만]
아래 형식의 JSON 객체로만 답하라.

{{
  "doc": "{doc_name}",
  "items": [
    {{
      "id": "<risk_id 같은 고유 ID, 예: R1, R2 ...>",
      "anchor": {{
        "id": "<위험이 발견된 anchor_id (위 섹션의 id)>",
        "title": "<anchor 제목 또는 조항명>"
      }},
      "riskType": "<6대 리스크 중 하나 또는 '기타'>",
      "severity": "<low|medium|high>",
      "reason": "<이 조항이 왜 위험한지, 어떤 상황에서 문제가 되는지 구체적으로 설명>",
      "original_excerpt": "<위험이 드러나는 원문 문장/문단을 그대로 포함>",
      "tags": ["risk", "anchor"]
    }}
    ...
  ]
}}

주의:
- 반드시 JSON만 출력할 것. 한국어 설명은 JSON 값 안에서만 사용.
- 같은 anchor_id 안에 여러 위험이 있으면 items를 여러 개 만들어도 된다.
- "id" 필드는 run 내부에서만 유일하면 된다 (예: "R1", "R2"...).
- "original_excerpt"에는 실제 조항의 문장을 그대로 넣어라.
""".strip()

# ==== control char sanitize ====
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
            # 기존 이스케이프 시퀀스는 그대로 통과
            out.append(ch)
            escape = False
        elif ch == "\\":
            out.append(ch)
            escape = True
        elif ch == '"':
            out.append(ch)
            in_str = not in_str
        elif in_str and ord(ch) < 32:
            # 문자열 내부의 제어문자만 이스케이프
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
            response_mime_type="application/json",
            temperature=0.2,
            top_p=0.8
        )
    )
    txt = res.text or ""
    s = txt.strip()

    # fenced code block 제거
    if s.startswith("```"):
        s = s.strip("`")
        nl = s.find("\n")
        if nl != -1:
            s = s[nl+1:].strip()

    # 1차 시도
    try:
        return json.loads(s)
    except Exception as e1:
        log.warning(f"risk-detector JSON parse failed (1st): {e1}")

    # 2차 시도: control char sanitize 후 재시도
    try:
        s2 = _sanitize_control_chars(s)
        return json.loads(s2)
    except Exception as e2:
        log.warning(f"risk-detector JSON parse failed (2nd): {e2}")

    # 둘 다 안 되면 최소 raw는 남겨두기
    return {"doc": "", "items": [], "raw": s[:1500]}

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

        log.info(f"[wk-risks] start run_id={run_id}, doc_id={doc_id}, session_id={session_id}, inputs={inputs}")

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

            prompt = make_prompt_for_risk(
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
        items = out.get("items") or []
        # id 필드 없는 경우 anchor 기반으로 최소한 채워주기
        for idx, it in enumerate(items, 1):
            if not it.get("id"):
                it["id"] = f"R{idx}"

        runtime_ms = int((time.time() - t0) * 1000)

        # 결과 저장
        payload = {
            "status": "done",
            "items": items
        }
        if out.get("raw"):
            payload["raw"] = out["raw"]

        with db() as conn, conn.cursor() as cur:
            upsert_result(cur, run_id, payload)
            conn.commit()
            trigger_run_tick(run_id)

        log.info(f"[wk-risks] done run_id={run_id}, items={len(items)}, runtimeMs={runtime_ms}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "worker": "risk", "count": len(items)} , ensure_ascii=False)
        }

    except Exception as e:
        log.exception("[wk-risks] failed")
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
