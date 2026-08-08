from app.lineage.graph import DependencyEdge, LineageGraph


def _sample_graph() -> LineageGraph:
    # dataset:raw.orders -> ingest_task -> transform_task -> {report_task_a, report_task_b}
    edges = [
        DependencyEdge(task_id="ingest_task", depends_on_table="raw.orders"),
        DependencyEdge(task_id="transform_task", depends_on_task_id="ingest_task"),
        DependencyEdge(task_id="report_task_a", depends_on_task_id="transform_task"),
        DependencyEdge(task_id="report_task_b", depends_on_task_id="transform_task"),
    ]
    return LineageGraph.from_dependencies(edges)


def test_blast_radius_from_task():
    graph = _sample_graph()
    impacted = {n.node_id: n.depth for n in graph.blast_radius("ingest_task")}
    assert impacted == {"transform_task": 1, "report_task_a": 2, "report_task_b": 2}


def test_blast_radius_from_dataset():
    graph = _sample_graph()
    impacted = {n.node_id: n.depth for n in graph.blast_radius("dataset:raw.orders")}
    assert impacted["ingest_task"] == 1
    assert impacted["report_task_a"] == 3
    assert impacted["report_task_b"] == 3


def test_rca_from_downstream_task():
    graph = _sample_graph()
    candidates = {n.node_id: n.depth for n in graph.rca("report_task_a")}
    assert candidates == {
        "transform_task": 1,
        "ingest_task": 2,
        "dataset:raw.orders": 3,
    }


def test_blast_radius_unknown_root_returns_empty():
    graph = _sample_graph()
    assert graph.blast_radius("does_not_exist") == []


def test_max_depth_cutoff():
    graph = _sample_graph()
    impacted = graph.blast_radius("dataset:raw.orders", max_depth=1)
    assert [n.node_id for n in impacted] == ["ingest_task"]


def test_no_cycle_in_dag():
    assert not _sample_graph().has_cycle()


def test_many_to_one_reconciliation_fan_in():
    # Two independent jobs both write into the same reconciliation table.
    edges = [
        DependencyEdge(task_id="job_a_load", depends_on_table="raw.feed_a"),
        DependencyEdge(task_id="job_b_load", depends_on_table="raw.feed_b"),
        DependencyEdge(task_id="reconcile_task", depends_on_task_id="job_a_load"),
        DependencyEdge(task_id="reconcile_task", depends_on_task_id="job_b_load"),
    ]
    graph = LineageGraph.from_dependencies(edges)
    rca = {n.node_id for n in graph.rca("reconcile_task")}
    assert rca == {"job_a_load", "job_b_load", "dataset:raw.feed_a", "dataset:raw.feed_b"}


def test_one_to_many_fan_out():
    edges = [DependencyEdge(task_id=f"downstream_{i}", depends_on_task_id="fanout_job") for i in range(20)]
    graph = LineageGraph.from_dependencies(edges)
    impacted = graph.blast_radius("fanout_job")
    assert len(impacted) == 20
    assert all(n.depth == 1 for n in impacted)
