import logging
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict

from database import db_manager
from services.utils import trigger_run_tick

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self):
        self.db_manager = db_manager
    
    def load_document_data(self, doc_id: int, session_id: int, inputs: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        logger.info(f"Loading data for doc_id={doc_id}, session_id={session_id}, inputs={inputs}")
        try:
            chunks = self.db_manager.fetch_document_chunks(doc_id)
            if not chunks:
                raise ValueError(f"No chunks found for doc_id {doc_id}")
            
            context = self.db_manager.fetch_session_answers(session_id)
            if not context:
                raise ValueError(f"No context found for session_id {session_id}")

            # inputs 에서 role/question/focus/retryInfo 덮어쓰기 & 머지
            role_in = (inputs or {}).get("role")
            question_in = (inputs or {}).get("question")
            focus_in = (inputs or {}).get("focus")
            retry_info = (inputs or {}).get("retryInfo")

            # 재분석 요약도 컨텍스트로 내려주기
            prev_summary_text = (inputs or {}).get("prevSummaryText")
            if not prev_summary_text:
                prev_summary = (inputs or {}).get("prevSummary")
                if isinstance(prev_summary, dict):
                    prev_summary_text = prev_summary.get("text")

            if role_in:
                context["role"] = role_in
            if question_in:
                context["question"] = question_in
            if focus_in is not None:
                context["focus"] = focus_in
            if retry_info:
                context["retryInfo"] = retry_info
            if prev_summary_text:
                context["prevSummaryText"] = prev_summary_text

            logger.info(f"Loaded {len(chunks)} chunks, context={context}")
            return chunks, context
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def group_chunks_by_clause(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        anchors 입력의 다양한 스키마를 방어적으로 처리:
        - list[dict]  (권장)
        - dict{ items: [...] }
        - None / 잘못된 타입은 스킵
        또한 anchor key 이름 변형(id/anchor_id/key, chunk_span/chunkSpan)도 수용
        """
        clause_groups = defaultdict(lambda: {
            'clause_id': None,
            'title': None,
            'content': '',
            'anchors': []
        })
        
        for chunk in (chunks or []):
            content = chunk.get('content') or ""
            anchors = chunk.get('anchors') or []
            if isinstance(anchors, dict):
                anchors = anchors.get('items', []) or []
            if not isinstance(anchors, list):
                anchors = []

            for anchor in anchors:
                if not isinstance(anchor, dict):
                    # 문자열/기타 타입인 경우 스킵
                    continue

                anchor_id = anchor.get('id') or anchor.get('anchor_id') or anchor.get('key')
                if not anchor_id:
                    continue

                title = anchor.get('title') or str(anchor_id)
                chunk_span = anchor.get('chunk_span') or anchor.get('chunkSpan') or {}

                clause_groups[anchor_id]['clause_id'] = anchor_id
                clause_groups[anchor_id]['title'] = title

                # 안전한 슬라이싱
                try:
                    start_pos = int(chunk_span.get('start', 0))
                    end_pos = int(chunk_span.get('end', len(content)))
                except Exception:
                    start_pos, end_pos = 0, len(content)

                start_pos = max(0, min(start_pos, len(content)))
                end_pos = max(start_pos, min(end_pos, len(content)))

                piece = content[start_pos:end_pos] if content else ""
                if clause_groups[anchor_id]['content']:
                    clause_groups[anchor_id]['content'] += '\n\n' + piece
                else:
                    clause_groups[anchor_id]['content'] = piece

                clause_groups[anchor_id]['anchors'].append(anchor)
        
        clause_data = []
        for clause_id, group_data in clause_groups.items():
            # 동일 anchor_id 중복 제거
            unique_anchors = []
            seen = set()
            for a in group_data['anchors']:
                aid = a.get('id') or a.get('anchor_id') or a.get('key')
                if aid and aid not in seen:
                    unique_anchors.append(a)
                    seen.add(aid)
            clause_data.append({
                'clause_id': clause_id,
                'title': group_data['title'],
                'content': group_data['content'],
                'anchors': unique_anchors
            })
        
        # clause_id가 문자열/숫자 혼재 가능 → 문자열로 정렬 안정화
        clause_data.sort(key=lambda x: str(x.get('clause_id')))
        return clause_data
    
    def save_results(self, run_id: int, summaries: Dict[str, Any]) -> None:
        logger.info(f"Saving results for run_id={run_id}")
        try:
            self.db_manager.upsert_run_result(
                worker_type="summarizer",
                run_id=run_id,
                payload=summaries
            )
            logger.info("Results saved successfully")
            trigger_run_tick(run_id)
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            trigger_run_tick(run_id)
            raise
