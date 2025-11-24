import os
import json
import boto3
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
L_RUN_TICK = os.getenv("L_RUN_TICK", "run-tick")

_lambda = boto3.client("lambda", region_name=AWS_REGION)

def trigger_run_tick(run_id: int):
    """
    run-tick Lambda를 비동기로 호출하여
    runs.status / progress / finished_at 갱신 유도
    """
    if not L_RUN_TICK:
        log.warning("L_RUN_TICK not set — skipping tick trigger")
        return

    try:
        _lambda.invoke(
            FunctionName=L_RUN_TICK,
            InvocationType="Event",
            Payload=json.dumps({"runId": run_id}).encode("utf-8")
        )
        log.info(f"Triggered run-tick for run_id={run_id}")
    except Exception as e:
        log.warning(f"run-tick invoke failed: {e}")
