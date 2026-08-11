import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { CodeViewer } from "../components/CodeViewer";
import { DagView } from "../components/DagView";
import { EmptyState } from "../components/EmptyState";
import { SlaBadge } from "../components/SlaBadge";
import { Spinner } from "../components/Spinner";
import type { Job, JobAlert, JobAudit, JobDependency, Task } from "../api/types";

export function JobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const [job, setJob] = useState<Job | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [audits, setAudits] = useState<JobAudit[]>([]);
  const [alerts, setAlerts] = useState<JobAlert[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [dependencies, setDependencies] = useState<JobDependency[]>([]);

  useEffect(() => {
    if (!jobId) return;
    setJob(null);
    api.get<Job>(`/jobs/${jobId}`).then(setJob).catch(() => {});
    api
      .get<Task[]>(`/jobs/${jobId}/tasks`)
      .then((t) => {
        setTasks(t);
        setSelectedTask(t[0] ?? null);
      })
      .catch(() => {});
    api.get<JobAudit[]>(`/jobs/${jobId}/audits`).then(setAudits).catch(() => {});
    api.get<JobAlert[]>(`/jobs/${jobId}/alerts`).then(setAlerts).catch(() => {});
  }, [jobId]);

  useEffect(() => {
    if (!selectedTask) {
      setDependencies([]);
      return;
    }
    api.get<JobDependency[]>(`/tasks/${selectedTask.task_id}/dependencies`).then(setDependencies).catch(() => {});
  }, [selectedTask]);

  if (!job) {
    return (
      <div>
        <Spinner label="Loading job…" />
      </div>
    );
  }

  return (
    <div>
      <div className="breadcrumb">
        <Link to="/">Dashboard</Link>
        <span>/</span>
        <span>{job.name}</span>
      </div>

      <div className="page-header">
        <div>
          <h2>{job.name}</h2>
          <p className="page-subtitle">
            Cron <code>{job.cron_expression ?? "—"}</code> · Max retries {job.max_retries} · Max concurrency{" "}
            {job.max_concurrency}
            {typeof job.cluster_config?.workers === "number" && (
              <> · Cluster {String(job.cluster_config.node_type ?? "")} × {String(job.cluster_config.workers)}</>
            )}
          </p>
        </div>
        <span className={`badge ${job.is_active ? "badge-ok" : "badge-neutral"}`}>
          {job.is_active ? "active" : "paused"}
        </span>
      </div>

      <div className="card">
        <div className="card-header">
          <h3>Task DAG</h3>
          <span className="card-header-meta">{tasks.length} task{tasks.length === 1 ? "" : "s"}</span>
        </div>
        <DagView tasks={tasks} selectedTaskId={selectedTask?.task_id} onSelect={setSelectedTask} />
      </div>

      {selectedTask && (
        <div className="card">
          <div className="card-header">
            <h3>Code Viewer — {selectedTask.name}</h3>
          </div>
          <select
            value={selectedTask.task_id}
            onChange={(e) => setSelectedTask(tasks.find((t) => t.task_id === e.target.value) ?? null)}
            style={{ marginBottom: 10 }}
          >
            {tasks.map((t) => (
              <option key={t.task_id} value={t.task_id}>
                {t.name}
              </option>
            ))}
          </select>
          <CodeViewer code={selectedTask.source_code ?? "-- no inline source recorded --"} language={selectedTask.exec_type} />

          {dependencies.length > 0 && (
            <div style={{ marginTop: 14 }}>
              <p className="field-label" style={{ margin: "0 0 6px" }}>
                Depends on
              </p>
              <div className="chip-row">
                {dependencies.map((dep) => (
                  <span key={dep.dependency_id} className="chip" style={{ cursor: "default" }}>
                    {dep.depends_on_kind === "DATASET" ? `dataset: ${dep.depends_on_table}` : `task: ${dep.depends_on_task_id}`}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      <div className="card">
        <div className="card-header">
          <h3>Recent Runs</h3>
        </div>
        {audits.length === 0 ? (
          <EmptyState icon="▸" title="No runs recorded yet" />
        ) : (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Rows</th>
                  <th>Cost</th>
                </tr>
              </thead>
              <tbody>
                {audits.map((a) => (
                  <tr key={a.audit_id}>
                    <td>{a.run_id}</td>
                    <td><SlaBadge status={a.status} /></td>
                    <td>{new Date(a.started_at).toLocaleString()}</td>
                    <td>{a.rows_processed?.toLocaleString() ?? "—"}</td>
                    <td>{a.cost_usd != null ? `$${a.cost_usd.toFixed(2)}` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-header">
          <h3>Alerts &amp; Distribution Lists</h3>
        </div>
        {alerts.length === 0 ? (
          <EmptyState icon="✉" title="No alert routing configured" />
        ) : (
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {alerts.map((a) => (
              <li key={a.alert_id} style={{ marginBottom: 4 }}>
                <span className="badge badge-neutral">{a.alert_type}</span>{" "}
                <span className="muted">→ {a.distribution_list.join(", ") || "—"}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
