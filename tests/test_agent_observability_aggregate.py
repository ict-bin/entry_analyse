from app.api import tasks as tasks_api


def test_build_agent_runtime_aggregate_exposes_failed_target_details() -> None:
    snapshot = {
        "summary": {
            "aggregate_partial": True,
            "aggregate_sources": 2,
            "aggregate_fanout_errors": 1,
            "aggregate_failed_targets": ["entry-worker-3"],
            "aggregate_failed_target_details": [
                {
                    "pod_name": "entry-worker-3",
                    "pod_ip": "10.0.0.23",
                    "http_port": 8080,
                    "attempted_urls": ["http://10.0.0.23:8080/api/app/entry-analyse"],
                    "error_kind": "http_error",
                    "status_code": 500,
                    "message": "boom",
                }
            ],
            "aggregate_all_sources_failed": False,
        },
        "pods": [],
        "processes": [],
        "tasks": [],
    }

    runtime = tasks_api._build_agent_runtime_aggregate(snapshot)
    assert runtime["summary"]["aggregate_failed_target_details"][0]["status_code"] == 500
    assert runtime["summary"]["aggregate_failed_target_details"][0]["http_port"] == 8080

