from fastapi.testclient import TestClient

from app.main import app


def test_end_to_end_workflow_job_lineage_and_nl_query():
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

        r = client.post(
            "/workflows",
            json={
                "name": "churn_prediction_wf",
                "owner_team": "ml-platform",
                "owner_email": "ml-platform@example.com",
                "sla_minutes": 120,
                "tags": {"domain": "churn"},
            },
        )
        assert r.status_code == 201, r.text
        workflow = r.json()

        r = client.post(
            "/jobs",
            json={
                "workflow_id": workflow["workflow_id"],
                "name": "churn_prediction",
                "cron_expression": "0 6 * * *",
            },
        )
        assert r.status_code == 201, r.text
        job = r.json()

        r = client.post(
            "/tasks",
            json={
                "job_id": job["job_id"],
                "name": "train_model",
                "exec_type": "PYSPARK",
                "source_code": "spark.table('features.churn_features')",
            },
        )
        assert r.status_code == 201, r.text
        upstream_task = r.json()

        r = client.post(
            "/tasks",
            json={
                "job_id": job["job_id"],
                "name": "publish_report",
                "task_order": 1,
                "exec_type": "SQL",
            },
        )
        assert r.status_code == 201, r.text
        downstream_task = r.json()

        r = client.post(
            "/dependencies",
            json={
                "task_id": upstream_task["task_id"],
                "depends_on_kind": "DATASET",
                "depends_on_table": "features.churn_features",
            },
        )
        assert r.status_code == 201, r.text

        r = client.post(
            "/dependencies",
            json={
                "task_id": downstream_task["task_id"],
                "depends_on_kind": "TASK",
                "depends_on_task_id": upstream_task["task_id"],
            },
        )
        assert r.status_code == 201, r.text

        r = client.get("/blast-radius/dataset:features.churn_features")
        assert r.status_code == 200
        impacted_ids = {n["node_id"] for n in r.json()["impacted_nodes"]}
        assert upstream_task["task_id"] in impacted_ids
        assert downstream_task["task_id"] in impacted_ids

        r = client.get(f"/rca/{downstream_task['task_id']}")
        assert r.status_code == 200
        candidate_ids = {n["node_id"] for n in r.json()["root_cause_candidates"]}
        assert upstream_task["task_id"] in candidate_ids
        assert "dataset:features.churn_features" in candidate_ids

        r = client.post("/lineage/parse-sql", json={"sql": "CREATE TABLE a.b AS SELECT x FROM c.d"})
        assert r.status_code == 200
        assert r.json()[0]["target_table"] == "a.b"

        r = client.post(
            "/lineage/parse-pyspark",
            json={"code": "df = spark.table('a.b')\ndf.write.saveAsTable('c.d')\n"},
        )
        assert r.status_code == 200
        assert r.json()[0]["target_table"] == "c.d"

        r = client.post(
            "/nl-query",
            json={
                "question": (
                    "Which downstream reporting tables will be affected if the "
                    "churn_prediction job fails?"
                )
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert body["tool_calls"][0]["name"] == "get_blast_radius"
        assert downstream_task["task_id"] in {
            n["id"] for n in body["tool_calls"][0]["result"]["impacted_nodes"]
        }


def test_workflow_validation_rejects_non_positive_sla():
    with TestClient(app) as client:
        r = client.post(
            "/workflows",
            json={
                "name": "bad_sla_wf",
                "owner_team": "team",
                "owner_email": "team@example.com",
                "sla_minutes": 0,
            },
        )
        assert r.status_code == 422
