# -*- coding: utf-8 -*-
import os
import json
import boto3
import psycopg2
import google.generativeai as genai
from typing import Any, Dict
from services.utils import trigger_run_tick
import logging

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# ───────────────────────────────
# 환경 변수
# ───────────────────────────────
BUCKET = os.environ["BUCKET_NAME"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
L_RUN_TICK = os.getenv("L_RUN_TICK")

s3 = boto3.client("s3", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# Gemini 설정
genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

# 임베딩 설정
EMBED_MODEL   = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
OPENAI_SDK    = os.environ.get("OPENAI_SDK", "v1")
PGVECTOR_DIM  = int(os.environ.get("PGVECTOR_DIM", "1536"))
_openai_client = None

# ───────────────────────────────
# DB 연결 및 상태 갱신 함수
# ───────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
        connect_timeout=5,
    )

def upsert_status(cur, worker: str, run_id: int, payload: dict):
    cur.execute(
        """
        INSERT INTO run_results (worker_type, run_id, payload)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (run_id, worker_type) DO UPDATE
        SET payload = EXCLUDED.payload
        """,
        (worker, run_id, json.dumps(payload, ensure_ascii=False)),
    )

def get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    api_key = os.environ["OPENAI_API_KEY"]
    if OPENAI_SDK == "v1":
        from openai import OpenAI
        _openai_client = ("v1", OpenAI(api_key=api_key))
    else:
        import openai as v0
        v0.api_key = api_key
        _openai_client = ("v0", v0)
    return _openai_client

def embed_question(text: str) -> list[float]:
    text = (text or "").strip()
    if not text:
        # 비어 있으면 그냥 0 벡터 리턴 (실제로는 거의 안타게)
        return [0.0] * PGVECTOR_DIM

    sdk, client = get_openai_client()
    if sdk == "v1":
        r = client.embeddings.create(model=EMBED_MODEL, input=[text])
        emb = r.data[0].embedding
    else:
        r = client.Embedding.create(model=EMBED_MODEL, input=[text])
        emb = r["data"][0]["embedding"]

    if len(emb) != PGVECTOR_DIM:
        raise RuntimeError(f"QA embedding dim mismatch: {len(emb)} != {PGVECTOR_DIM}")
    return emb

# ───────────────────────────────
# documents_chunks 에서 상위 K개 청크 + 앵커 조회
# ───────────────────────────────
def get_top_chunks_for_qa(cur, doc_id: int, q_vec: list[float], k: int = 8):
    """
    pgvector similarity (embedding <-> q_vec) 기반으로
    상위 K개 관련 청크와 그 앵커 정보를 가져온다.
    """
    # q_vec 을 그대로 vector 로 캐스팅해서 사용 (doc-embedding 때와 동일 패턴)
    cur.execute(
        """
        SELECT chunk_idx, content, anchors
        FROM documents_chunks
        WHERE doc_id = %s
        ORDER BY embedding <-> %s::vector
        LIMIT %s;
        """,
        (doc_id, q_vec, k),
    )
    rows = cur.fetchall()
    out = []
    for idx, content, anchors in rows:
        # anchors: {"count":N,"items":[...]} 형태
        if isinstance(anchors, str):
            try:
                anchors = json.loads(anchors)
            except Exception:
                anchors = {}
        if anchors is None:
            anchors = {}
        out.append(
            {
                "chunk_idx": idx,
                "content": content or "",
                "anchors": anchors or {"count": 0, "items": []},
            }
        )
    return out

def build_context_from_chunks(chunks_for_qa: list[dict], max_chars_per_chunk: int = 1200) -> str:
    """
    LLM 입력용 컨텍스트 문자열 구성:
    - 청크별 index, anchor 제목들, 내용 일부를 포함
    - 너무 길어지지 않도록 청크당 최대 글자수 제한
    """
    blocks: list[str] = []
    for ch in chunks_for_qa:
        idx = ch["chunk_idx"]
        text = ch["content"] or ""
        anchors = ch.get("anchors") or {}
        items = anchors.get("items") or []

        titles = []
        for a in items:
            if isinstance(a, dict):
                t = (a.get("title") or "").strip()
                if t:
                    titles.append(t)
        titles_str = ", ".join(titles) if titles else "(관련 앵커 없음)"

        snippet = text[:max_chars_per_chunk]
        blocks.append(
            f"[chunk {idx}] anchors: {titles_str}\n{snippet}"
        )

    return "\n\n".join(blocks) if blocks else "(관련 청크를 찾지 못함)"

# ───────────────────────────────
# 질문 파싱 헬퍼 (수정 요청 부분 제거)
# ───────────────────────────────
def extract_pure_question(original_question: str) -> str:
    """
    question 안에서 '오탈자/표현 수정/대체 표현/문구 다듬기' 같은
    '문장 수정·대체 요청' 부분은 모두 무시하고,
    실제로 문서 내용에 대해 궁금해하는 질의 부분만 뽑는다.

    - 실패하거나 비어 있으면 원래 질문을 그대로 반환.
    """
    if not original_question:
        return ""

    # revision 성향 키워드가 하나라도 있으면 파싱 시도
    revision_like_keywords = [
        "오탈자", "오타",
        "수정해줘", "수정해 주고", "수정해주고", "수정해", "수정해라", "수정",
        "고쳐줘", "고쳐 주고", "고쳐주고", "고쳐", "고치", "고쳐라",
        "바꿔줘", "바꿔 주고", "바꿔주고", "바꿔", "바꾸", "변경해줘", "변경해",
        "대체", "대체해줘", "대체해", "대체 표현", "대체 문구",
        "표현 다듬", "문구 다듬", "다듬어", "다듬어줘",
        "문구 제안", "문구 추천", "템플릿", "다른 표현", "다른 문구",
    ]

    if not any(k in original_question for k in revision_like_keywords):
        # 수정 관련 키워드 없으면 그냥 원문 그대로 사용
        return original_question

    splitter_prompt = f"""
당신은 한국어 계약서 QA 시스템의 전처리 모듈이다.

다음 질문에서:
- '오탈자 수정해줘', '문구를 다듬어줘', '대체 표현을 제안해줘', '수정해주고' 등
  **위험 조항/문장/표현을 수정·대체해 달라는 요청 부분은 모두 제거**하고,
- 사용자가 실제로 문서 내용에 대해 **정보를 묻는 부분(질문)**만 자연스럽게 한 문장으로 다시 써라.

예시:
- 입력: "오탈자 고쳐주고, 계약 종료 시 기밀정보는 어떻게 처리해야 하는가?"
  출력: "계약 종료 시 기밀정보는 어떻게 처리해야 하는가?"

- 입력: "위험 조항 수정해주고, 위약금은 어느 정도까지 청구할 수 있는지 알려줘."
  출력: "위약금은 어느 정도까지 청구할 수 있는지 알려줘."

- 입력: "이 조항을 더 부드러운 표현으로 바꿔줘."
  출력: ""   (정보 질의가 없으므로 빈 문자열)

출력은 반드시 JSON 형식으로만:
{{ "pure_question": "<정보 질의만 남긴 질문(없으면 빈 문자열)>" }}

[원본 질문]
{original_question}
""".strip()

    try:
        res = model.generate_content(
            splitter_prompt,
            generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json"
            ),
        )
        data = json.loads(res.text)
        pure_q = (data.get("pure_question") or "").strip()
        if pure_q:
            return pure_q
        # 비어 있으면 원래 질문 그대로 사용
        return original_question
    except Exception:
        # 파싱 실패 시에도 안전하게 원래 질문 사용
        return original_question

# ───────────────────────────────
# 이벤트 파싱
# ───────────────────────────────
def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    body = None
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

    run_id = body.get("runId") or body.get("run_id")
    doc_id = body.get("docId") or body.get("doc_id")
    session_id = body.get("sessionId") or body.get("session_id")
    inputs = body.get("inputs", {}) or {}

    if not all([run_id, doc_id, session_id]):
        raise ValueError("Missing required parameters: runId, docId, sessionId")

    return {
        "run_id": int(run_id),
        "doc_id": int(doc_id),
        "session_id": int(session_id),
        "inputs": inputs,
    }

# ───────────────────────────────
# Lambda Handler
# ───────────────────────────────
def lambda_handler(event, context):
    conn = None
    cur = None
    try:
        params = parse_event(event)
        run_id = params["run_id"]
        doc_id = params["doc_id"]
        session_id = params["session_id"]
        inputs = params["inputs"]

        conn = get_connection()
        cur = conn.cursor()

        upsert_status(cur, "qa", run_id, {"status": "running"})
        conn.commit()

        # 1️⃣ 문서 이름
        cur.execute("SELECT name FROM documents WHERE id=%s;", (doc_id,))
        row = cur.fetchone()
        if not row:
            upsert_status(cur, "qa", run_id, {"status": "failed", "error": f"Document not found: {doc_id}"})
            conn.commit()
            return {"ok": False, "error": f"Document not found: {doc_id}"}
        doc_name = row[0]

        s3_key = (inputs or {}).get("s3TextKey") or f"output/{doc_name}.txt"

        # 2️⃣ 세션 정보
        cur.execute("SELECT role, answers FROM sessions WHERE id=%s;", (session_id,))
        fetched = cur.fetchone()
        if not fetched:
            upsert_status(cur, "qa", run_id, {"status": "failed", "error": f"Session not found: {session_id}"})
            conn.commit()
            return {"ok": False, "error": f"Session not found: {session_id}"}

        role_db, answers = fetched
        if isinstance(answers, str):
            try:
                answers = json.loads(answers)
            except Exception:
                answers = {}

        role = (inputs.get("role") if inputs else None) or role_db or answers.get("role")
        question_raw = (inputs.get("question") if inputs else None) or answers.get("question")
        focus = (inputs.get("focus") if inputs else None) or answers.get("focus", [])
        if isinstance(focus, str):
            focus = [focus]
        focus_joined = ", ".join(map(str, focus)) if focus else "없음"

        # ─────────────────────────────
        # ① question 이 있으면: 수정요청 부분 제거 + 순수 질의만 사용
        # ② question 이 없고 focus 만 있으면: focus 기반으로 "가짜 질문" 생성
        # ─────────────────────────────
        if question_raw and question_raw.strip():
            # 질문이 있는 경우: revision 관련 표현 제거 후 순수 질의만 사용
            pure_question = extract_pure_question(question_raw)
            question_for_qa = pure_question.strip() or question_raw.strip()
        elif focus:
            # 질문은 없고 focus 만 있는 경우:
            # → focus 를 이용해 "문서에서 이 키워드들에 대해 중요한 내용을 설명해 달라"는 형태의 질의 생성
            question_for_qa = f"다음 키워드에 대해 이 계약서에서 사용자에게 중요한 내용과 위험 요소를 설명하라: {', '.join(map(str, focus))}"
        else:
            # 이 경우는 사실상 plan 단계에서 QA가 실행되지 않아야 하지만,
            # 혹시 몰라서 완전 빈 문자열로 두기
            question_for_qa = ""

        # Verifier 재시도 피드백
        retry_info = (inputs or {}).get("retryInfo") or {}
        vf_reason = retry_info.get("reason")
        vf_metrics = retry_info.get("metrics") or {}
        vf_attempt = retry_info.get("attempt")

        verifier_block = ""
        if vf_reason or vf_metrics:
            verifier_block = f"""
[이전 시도에 대한 Verifier 피드백]
- attempt: {vf_attempt}
- reason: {vf_reason or "N/A"}
- metrics: anchorRate={vf_metrics.get("anchorRate")}, kpri={vf_metrics.get("kpri")}, faithfulness={vf_metrics.get("faithfulness")}

위 피드백을 반영하여, 보다 정확하고 근거가 분명한 답변을 다시 작성하라.
가능하면 앵커(관련 조항 제목/번호)를 더 명확히 제시하라.
""".strip()

        # 재분석 컨텍스트
        reanalyze_text = (inputs or {}).get("prevSummaryText")
        reanalyze_block = ""
        if reanalyze_text:
            reanalyze_block = f"""
[이전 사용자 분석 요약]
아래는 직전 분석(run)의 핵심 요약이다. 이 내용을 참고하되,
이전 답변에서 충분히 다루지 못했던 부분이 없는지 다시 점검하라.

{reanalyze_text}
""".strip()

        # 3️⃣ 관련 청크 + 앵커 조회 (pgvector RAG)
        #    - 문서 전체를 S3 에서 다 읽어오는 대신,
        #      question_for_qa 임베딩 → documents_chunks 상위 K개만 LLM에 제공
        q_vec = embed_question(question_for_qa)
        top_chunks = get_top_chunks_for_qa(cur, doc_id, q_vec, k=24)  # k 8 → 24로 늘리기
        context_text = build_context_from_chunks(top_chunks, max_chars_per_chunk=800)  # 청크당 800자로 줄여서 전체 길이 조절

        logger.info("[QA] doc_id=%s question=%r", doc_id, question_for_qa)
        logger.info("[QA] retrieved chunks = %d", len(top_chunks))
        for ch in top_chunks:
            anchors = ch.get("anchors") or {}
            items = anchors.get("items") or []
            titles = [ (a.get("title") or "").strip() for a in items if isinstance(a, dict) ]
            logger.info(
                "[QA] chunk_idx=%s anchors=%s snippet=%r",
                ch["chunk_idx"],
                titles,
                (ch["content"] or "")[:200].replace("\n", " ")
            )

        # 3️⃣ S3 문서 로드
        # obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
        # text = obj["Body"].read().decode("utf-8")

        # 4️⃣ Gemini 프롬프트 구성 (질문은 question_for_qa 사용)
        prompt = f"""
당신은 계약서 분석 전문가이자 QA Agent입니다.
아래 '관련 청크들'을 기반으로 사용자의 질문을 직접 분석하고,
가능한 한 실제 조항 제목/번호(앵커)를 함께 제시하세요.

[응답 규칙 매우 중요]
1. 사용자의 질문이 모호하더라도 되묻거나 추가 질문을 하지 않는다.
2. 질문이 한 문장에 여러 요구를 섞어 말하더라도,
   그 중에서 "문서 내용에 대한 정보 질의" 부분만을 대상으로 답변한다.
   예를 들어, "오탈자 수정해주고, 계약 종료 시 기밀정보는 어떻게 처리해야 하는가?"
   라는 질문이 오면, "계약 종료 시 기밀정보는 어떻게 처리해야 하는가?"에 대해서만 답변하라.
3. 문장/표현 수정, 오탈자/문구 다듬기, 대체 표현/템플릿 제안 요청은 모두 무시하고,
   실제로 문서에 어떤 내용이 규정되어 있는지에 대해서만 답변한다.
4. 자연스러운 대화식 답변 금지. 항상 '분석 결과'만 JSON으로 출력한다.
5. 문서에 없는 내용을 허구로 생성하지 않는다.
6. 관련 조항이 여러 개면 모두 anchors에 넣는다.
7. 아래 [관련 청크들]은 문서 전체가 아니라, 질문과 가장 유사한 일부 조각들이다.
+    답변은 이 컨텍스트 안에서만 근거를 찾되,
+    컨텍스트에 해당 내용이 보이지 않는 경우에는
+    - "이 컨텍스트 범위 내에서는 해당 내용을 확인할 수 없다"고 말하고,
+    - 문서 전체에 없다고 단정하지 말아라.

[사용자 역할]
{role}

[질문 (정보 질의만 추려진 버전)]
{question_for_qa}

[집중 키워드]
{focus_joined}

{verifier_block if verifier_block else ""}

{reanalyze_block if reanalyze_block else ""}

[관련 청크들 (상위 검색 결과)]
{context_text}

JSON 형식으로만 답변하세요.
{{
  "answer": "요약된 답변 내용",
  "anchors": ["제15조 (해지 및 환불)", "제12조 (요금 반환)"]
}}
""".strip()

        # 5️⃣ Gemini API 호출
        result = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json"
            )
        )

        parsed = json.loads(result.text)

        print("[RESULT] doc_name =", doc_name)
        print("[RESULT] original_question =", question_raw)
        print("[RESULT] question_for_qa =", question_for_qa)
        print("[RESULT] gemini_answer =", parsed.get("answer"))
        print("[RESULT] gemini_anchors =", parsed.get("anchors"))

        # 6️⃣ 결과 저장
        payload = {
            "status": "done",
            "ok": True,
            "worker": "qa",
            "role": role,
            # 🔧 실제 QA에 사용한 질문과 원본 질문 둘 다 저장
            "question": question_for_qa,
            "originalQuestion": question_raw,
            "focus": focus,
            "answer": parsed.get("answer"),
            "anchors": parsed.get("anchors", []),
        }
        upsert_status(cur, "qa", run_id, payload)
        conn.commit()
        trigger_run_tick(run_id)

        return {"ok": True, "worker": "qa", "runId": run_id}

    except Exception as e:
        try:
            if conn:
                if not cur:
                    cur = conn.cursor()
                rid = None
                try:
                    rid = parse_event(event)["run_id"]
                except Exception:
                    pass
                if rid is not None:
                    upsert_status(cur, "qa", rid, {"status": "failed", "error": str(e)})
                    conn.commit()
                    trigger_run_tick(rid)
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
