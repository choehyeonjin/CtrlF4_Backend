import logging
from typing import Dict, Any, List

from config import MAX_CHUNK_SIZE, LAMBDA_TIMEOUT_BUFFER

logger = logging.getLogger(__name__)


class SummarizationService:
    def __init__(self, llm_service):
        self.llm_service = llm_service

    # ----------------------------------------------------------------------
    # 1) 조항 단위 요약
    # ----------------------------------------------------------------------
    def summarize_clause_with_persona(
        self,
        chunk: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            clause_id = chunk.get("clause_id")
            title = chunk.get("title")
            content = chunk.get("content") or ""
            anchors = chunk.get("anchors") or []

            role = context.get("role")
            answer = context.get("answer")
            retry_info = context.get("retryInfo")
            reanalyze_text = context.get("prevSummaryText")

            summary = self.llm_service.summarize_content_with_persona(
                content,
                role,
                answer,
                retry_info=retry_info,
                reanalyze_text=reanalyze_text,
            )

            return {
                "clause_id": clause_id,
                "title": title,
                "summary": summary,
                "anchors": anchors,
            }

        except Exception as e:
            logger.error(
                f"Error summarizing clause {chunk.get('clause_id', 'unknown')}: {e}"
            )
            return {
                "clause_id": chunk.get("clause_id", "unknown"),
                "title": chunk.get("title", ""),
                "summary": f"Error summarizing this clause: {str(e)}",
                "anchors": chunk.get("anchors", []),
            }

    # ----------------------------------------------------------------------
    # 2) 전체 조항 반복 요약
    # ----------------------------------------------------------------------
    def summarize_clauses_with_persona(
        self,
        chunks_by_clause: List[Dict[str, Any]],
        context: Dict[str, Any],
        lambda_context: Any,
    ) -> List[Dict[str, Any]]:
        summaries = []

        for chunk in chunks_by_clause:
            if self._check_timeout(lambda_context):
                logger.warning("Timeout imminent, stopping summarization.")
                break

            summaries.append(self.summarize_clause_with_persona(chunk, context))

        return summaries

    # ----------------------------------------------------------------------
    # 3) 전체 문서 요약 (Stuff / Refine / Map-Reduce)
    # ----------------------------------------------------------------------
    def create_full_document_summary_with_persona(
        self,
        chunks_by_clause: List[Dict[str, Any]],
        clause_summaries: List[Dict[str, Any]],
        context: Dict[str, Any],
        lambda_context: Any,
    ) -> Dict[str, Any]:
        full_content = "\n\n".join(
            [chunk.get("content") or "" for chunk in chunks_by_clause]
        )
        role = context.get("role")
        answer = context.get("answer")
        retry_info = context.get("retryInfo")
        reanalyze_text = context.get("prevSummaryText")

        try:
            # Strategy 1: Stuff
            if len(full_content) < MAX_CHUNK_SIZE:
                logger.info("Using 'Stuff' strategy")
                summary = self.llm_service.summarize_content_with_persona(
                    full_content,
                    role,
                    answer,
                    max_tokens=2000,
                    summary_type="detailed",
                    retry_info=retry_info,
                    reanalyze_text=reanalyze_text,
                )
                return {"summary": summary}

            # Strategy 2: Refine
            combined_summaries = "\n\n".join(
                [cs.get("summary") or "" for cs in clause_summaries]
            )
            if len(combined_summaries) < MAX_CHUNK_SIZE:
                logger.info("Using 'Refine' strategy")
                summary = self.llm_service.summarize_content_with_persona(
                    combined_summaries,
                    role,
                    answer,
                    max_tokens=4000,
                    summary_type="detailed",
                    retry_info=retry_info,
                    reanalyze_text=reanalyze_text,
                )
                return {"summary": summary}

            # Strategy 3: Map-Reduce
            logger.info("Using 'Map-Reduce' strategy")
            inter = self._create_intermediate_summaries(
                clause_summaries,
                role,
                answer,
                lambda_context,
                retry_info=retry_info,
                reanalyze_text=reanalyze_text,
            )
            if not inter:
                raise ValueError("No intermediate summaries were created.")

            summary = self.llm_service.summarize_content_with_persona(
                "\n\n".join(inter),
                role,
                answer,
                max_tokens=4000,
                summary_type="detailed",
                retry_info=retry_info,
                reanalyze_text=reanalyze_text,
            )
            return {"summary": summary}

        except Exception as e:
            logger.error(f"Error in full document summary creation: {e}")
            return {"summary": f"Error creating full summary: {str(e)}"}

    # ----------------------------------------------------------------------
    # 4) Map-Reduce 중간 요약 생성
    # ----------------------------------------------------------------------
    def _create_intermediate_summaries(
        self,
        clause_summaries: List[Dict[str, Any]],
        role: str,
        answer: Dict[str, Any],
        lambda_context: Any,
        retry_info: Dict[str, Any] | None = None,
        reanalyze_text: str | None = None,
    ) -> List[str]:
        intermediate_summaries = []
        current_batch = []
        current_length = 0

        for cs in clause_summaries:
            summary_text = cs.get("summary") or ""
            if current_length + len(summary_text) > MAX_CHUNK_SIZE:
                if self._check_timeout(lambda_context):
                    logger.warning("Timeout imminent during intermediate summaries.")
                    break

                batch_text = "\n\n".join(current_batch)
                inter_summary = self.llm_service.summarize_content_with_persona(
                    batch_text,
                    role,
                    answer,
                    max_tokens=2000,
                    summary_type="brief",
                    retry_info=retry_info,
                    reanalyze_text=reanalyze_text,
                )
                intermediate_summaries.append(inter_summary)

                current_batch = [summary_text]
                current_length = len(summary_text)
            else:
                current_batch.append(summary_text)
                current_length += len(summary_text)

        # 마지막 배치 처리
        if current_batch and not self._check_timeout(lambda_context):
            batch_text = "\n\n".join(current_batch)
            inter_summary = self.llm_service.summarize_content_with_persona(
                batch_text,
                role,
                answer,
                max_tokens=2000,
                summary_type="brief",
                retry_info=retry_info,
                reanalyze_text=reanalyze_text,
            )
            intermediate_summaries.append(inter_summary)

        return intermediate_summaries

    # ----------------------------------------------------------------------
    # 5) Lambda 타임아웃 체크
    # ----------------------------------------------------------------------
    def _check_timeout(self, lambda_context: Any) -> bool:
        if not lambda_context:
            return False

        remaining = lambda_context.get_remaining_time_in_millis() / 1000.0
        if remaining < LAMBDA_TIMEOUT_BUFFER:
            logger.warning(f"Approaching timeout: {remaining:.2f}s remaining")
            return True

        return False
