import os, json, logging, datetime
from typing import Dict, Any, List, Tuple
import psycopg2

log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# DB
def db():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        sslmode="require",
        connect_timeout=5,
    )

# 핵심 로직: 1개 run 재계산 및 반영
def recompute_and_update_run(run_id: int) -> Dict[str, Any]:
    """
    run_results 상태 기반으로 runs.status/progress/finished_at 갱신
    - 분모: runs.plan JSONB (없으면 run_results의 key들 사용)
    - 분자: run_results.payload.status (done/failed/running/queued)
    - 정책: 모든 워커 done이면 runs.status = 'completed'
    """
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT plan, status, finished_at FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": f"run not found: {run_id}"}
        plan_json, current_status, finished_at = row

        # 1) 계획(분모) 및 현재 결과들 수집
        plan = plan_json or []
        cur.execute("SELECT worker_type, payload FROM run_results WHERE run_id=%s", (run_id,))
        rows = cur.fetchall()

        status_map = {}
        for w, p in rows:
            payload = p if isinstance(p, dict) else (json.loads(p) if isinstance(p, str) else {})
            status_map[w] = payload

        # __plan__ 같은 예약 키 제외
        workers: List[str] = list(plan) if plan else [w for w in status_map.keys() if w != "__plan__"]
        total = len(workers)
        if total == 0:
            return {"ok": False, "error": f"no workers found for run_id={run_id}"}

        # 2) 상태 집계
        done = 0
        any_failed = False
        any_running = False
        for w in workers:
            st = (status_map.get(w, {}) or {}).get("status", "queued")
            if st == "done":
                done += 1
            elif st == "failed":
                any_failed = True
            elif st == "running":
                any_running = True

        progress = int(round(100 * done / max(1, total)))
        progress = min(max(progress, 0), 100)

        # 3) 상태 결정
        if any_failed:
            new_status = "failed"
            set_finished_now = True
        elif total > 0 and done == total:
            # 모두 성공 → completed
            new_status = "completed"
            set_finished_now = True
        elif any_running:
            new_status = "running"
            set_finished_now = False
        else:
            new_status = "queued"
            set_finished_now = False

        # 4) DB 반영
        if set_finished_now and finished_at is None:
            cur.execute("""
                UPDATE runs
                   SET status=%s,
                       progress=%s,
                       finished_at=NOW()
                 WHERE id=%s
            """, (new_status, progress, run_id))
        else:
            cur.execute("""
                UPDATE runs
                   SET status=%s,
                       progress=%s
                 WHERE id=%s
            """, (new_status, progress, run_id))
        conn.commit()

        log.info(f"[run-tick] run {run_id} → {new_status} ({done}/{total}, {progress}%)")

        return {
            "ok": True,
            "runId": run_id,
            "status": new_status,
            "progress": progress,
            "done": done,
            "total": total,
            "failed": any_failed,
            "workers": workers,
        }

# 배치 처리(옵션): 최근 N분 내 running 상태인 run 일괄 갱신
def tick_all_running(max_age_min: int = 180) -> Dict[str, Any]:
    since = datetime.datetime.utcnow() - datetime.timedelta(minutes=max_age_min)
    ids: List[int] = []
    updated: List[Dict[str, Any]] = []
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id
              FROM runs
             WHERE status IN ('queued','running')
               AND started_at >= %s
             ORDER BY id DESC
            """, (since,))
        ids = [r[0] for r in cur.fetchall()]

    for rid in ids:
        try:
            updated.append(recompute_and_update_run(rid))
        except Exception as e:
            updated.append({"ok": False, "runId": rid, "error": str(e)})

    return {"ok": True, "count": len(ids), "results": updated}

def _parse_event(event: dict) -> tuple[int | None, bool]:
    """
    지원 케이스:
    - API GW: event["body"] = JSON string/object, pathParameters, queryStringParameters
    - Direct Lambda invoke: event = {"runId": 123}
    - EventBridge 등: event 자체가 dict일 수 있음
    """
    # 1) body 우선
    body = None
    if isinstance(event, dict):
        body = event.get("body")
        # body가 없으면 top-level dict 자체를 바디로 사용
        if body is None:
            body = event

    # 2) 문자열이면 JSON 파싱
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            body = {}

    if not isinstance(body, dict):
        body = {}

    # 3) path/query도 함께 처리
    path  = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}

    # 4) 배치 여부
    all_flag = bool(body.get("all") or query.get("all") in ("1", "true", "True"))

    # 5) runId 해석 (camel/snake + path)
    run_id = body.get("runId") or body.get("run_id") or path.get("runId") or path.get("run_id")
    run_id = int(run_id) if run_id is not None else None

    return run_id, all_flag

# 핸들러 (API GW/이벤트 모두 지원)
# - 단건: {"runId":123}
# - 배치: {"all":true} 또는 쿼리스트링 all=1
def lambda_handler(event, context):
     # 프리플라이트
    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
            },
        }

    try:
        # 통합 파서 사용
        run_id, all_flag = _parse_event(event)

        if all_flag:
            max_age = int(os.getenv("MAX_TICK_AGE_MIN", "180"))
            out = tick_all_running(max_age)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(out, ensure_ascii=False),
            }

        if run_id is None:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"ok": False, "error": "runId required"}),
            }

        out = recompute_and_update_run(run_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(out, ensure_ascii=False),
        }
    except Exception as e:
        log.exception("run-tick failed")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": False, "error": str(e)}),
        }