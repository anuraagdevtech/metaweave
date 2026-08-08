import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api } from "../api/client";
import { CodeViewer } from "../components/CodeViewer";
import { DagView } from "../components/DagView";
import { SlaBadge } from "../components/SlaBadge";
import type { Job, JobAlert, JobAudit, Task } from "../api/types";

export function JobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const [job, setJob] = useState<Job | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [audits, setAudits] = useState<JobAudit[]>([]);
  const [alerts, setAlerts] = useState<JobAlert[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);

  useEffect(() => {
    if (!jobId) return;
    api.get<Job>(`/jobs/${jobId}`).then(setJob).catch(() => {});
    api.get<Task[]>(`/jobs/${jobId}/tasks`).then((t) => {
      setTasks(t);
      setSelectedTask(t[0] ?? null);
    }).catch(() => {});
    api.get<JobAudit[]>(`/jobs/${jobId}/audits`).then(setAudits).catch(() => {});
    api.get<JobAlert[]>(`/jobs/${jobId}/alerts`).then(setAlerts).catch(() => {});
  }, [jobId]);

  if (!job) return <p className="muted">Loading…</p>;

  return (
    <div>
      <h2>{job.name}</h2>
      <p className="muted">Cron: {job.cron_expression ?? "—"} · Max retries: {job.max_retries} · Max concurrency: {job.max_concurrency}</p>

      <div className="card">
        <h3>Task DAG</h3>
        <DagView tasks={tasks} />
      </div>

      {selectedTask && (
        <div className="card">
          <h3>Code Viewer — {selectedTask.name}</h3>
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
        </div>
      )}

      <div className="card">
        <h3>Recent Runs</h3>
        {audits.length === 0 ? (
          <p className="muted">No runs recorded yet.</p>
        ) : (
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
                  <td>{a.rows_processed ?? "—"}</td>
                  <td>{a.cost_usd != null ? `$${a.cost_usd.toFixed(2)}` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="card">
        <h3>Alerts &amp; Distribution Lists</h3>
        {alerts.length === 0 ? (
          <p className="muted">No alert routing configured.</p>
        ) : (
          <ul>
            {alerts.map((a) => (
              <li key={a.alert_id}>
                <strong>{a.alert_type}</strong> → {a.distribution_list.join(", ") || "—"}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
