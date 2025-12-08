# -*- coding: utf-8 -*-
import os
import json
import time
import logging
from typing import Dict, Any

import boto3
from database import db_manager  # run_results 업서트에 사용

from services.llm_service import LLMService
from services.validation_service import ValidationService
from services.summarization_service import SummarizationService
from services.data_service import DataService
from services.utils import trigger_run_tick

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
L_RUN_TICK = os.getenv("L_RUN_TICK")
_lambda = boto3.client("lambda", region_name=AWS_REGION)

def _upsert(worker: str, run_id: int, payload: dict):
    db_manager.upsert_run_result(worker_type=worker, run_id=run_id, payload=payload)

def _parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    API GW(body 문자열/객체)와 내부 invoke(최상위 dict) 모두 지원.
    camelCase/snake_case 키 모두 허용.
    """
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
    doc_id     = body.get("docId")     or body.get("doc_id")
    session_id = body.get("sessionId") or body.get("session_id")
    inputs     = body.get("inputs", {}) or {}

    if not all([run_id, doc_id, session_id]):
        raise ValueError("Missing required parameters: runId, docId, sessionId")

    return {
        "run_id": int(run_id),
        "doc_id": int(doc_id),
        "session_id": int(session_id),
        "inputs": inputs,
    }

class SummarizerWorker:
    def __init__(self):
        self.llm_service = LLMService()
        self.validation_service = ValidationService(self.llm_service)
        self.summarization_service = SummarizationService(self.llm_service)
        self.data_service = DataService()
        self.start_time = time.time()

    def process(self, doc_id: int, run_id: int, session_id: int, inputs: Dict[str, Any], lambda_context: Any):
        logger.info(f"Starting SummarizerWorker processing: doc_id={doc_id}, run_id={run_id}, session_id={session_id}")

        # run_results: running
        _upsert("summarizer", run_id, {"status": "running"})

        try:
            # 1) 문서/세션 로드 (inputs 안의 retryInfo 포함)
            chunks, context = self.data_service.load_document_data(doc_id, session_id, inputs)

            # 2) 조항별 그룹
            clause_data = self.data_service.group_chunks_by_clause(chunks)

            # 3) 조항별 요약 → 전체 요약
            clause_summaries = self.summarization_service.summarize_clauses_with_persona(
                clause_data, context, lambda_context
            )
            full_summary = self.summarization_service.create_full_document_summary_with_persona(
                clause_data, clause_summaries, context, lambda_context
            )

            results_payload = {
                "full_document": {"summary": full_summary.get("summary")},
                "by_clause": clause_summaries,
            }

            # 4) 결과 저장 (표준화된 업서트)
            _upsert("summarizer", run_id, {
                "status": "done",
                "ok": True,
                "worker": "summarizer",
                "results": results_payload,
                "runtimeSec": round(time.time() - self.start_time, 2),
            })
            logger.info("SummarizerWorker completed successfully")
            # 상태 집계 트리거(선택)

        except Exception as e:
            logger.error(f"SummarizerWorker failed: {e}")
            _upsert("summarizer", run_id, {"status": "failed", "error": str(e)})
            raise

def lambda_handler(event: Dict[str, Any], lambda_context) -> Dict[str, Any]:
    logger.info(f"Lambda handler invoked with event: {json.dumps(event, ensure_ascii=False)}")
    try:
        p = _parse_event(event)
        run_id, doc_id, session_id, inputs = p["run_id"], p["doc_id"], p["session_id"], p["inputs"]

        worker = SummarizerWorker()
        worker.process(doc_id, run_id, session_id, inputs, lambda_context)

        trigger_run_tick(run_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "worker": "summarizer"})
        }

    except Exception as e:
        logger.error(f"Lambda handler failed: {e}")
        # 실패 상태도 기록 시도
        try:
            rid = None
            try:
                rid = _parse_event(event)["run_id"]
            except Exception:
                pass
            if rid is not None:
                _upsert("summarizer", rid, {"status": "failed", "error": str(e)})
                trigger_run_tick(rid)
        except Exception:
            pass

        return {
            "ok": False,
            "error": str(e),
            "worker": "summarizer"
        }
