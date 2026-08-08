import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { Job, Workflow } from "../api/types";

export function Dashboard() {
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([api.get<Workflow[]>("/workflows"), api.get<Job[]>("/jobs")])
      .then(([w, j]) => {
        setWorkflows(w);
        setJobs(j);
      })
      .catch((e) => setError(String(e)));
  }, []);

  return (
    <div>
      <h2>Runbook Dashboard</h2>
      {error && <p className="muted">Backend not reachable yet: {error}</p>}

      <div className="card">
        <h3>Workflows</h3>
        {workflows.length === 0 ? (
          <p className="muted">No workflows yet. Create one via the API to get started.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Owner Team</th>
                <th>SLA (min)</th>
                <th>Jobs</th>
              </tr>
            </thead>
            <tbody>
              {workflows.map((wf) => (
                <tr key={wf.workflow_id}>
                  <td>{wf.name}</td>
                  <td>{wf.owner_team}</td>
                  <td>{wf.sla_minutes}</td>
                  <td>{jobs.filter((j) => j.workflow_id === wf.workflow_id).length}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="card">
        <h3>Jobs</h3>
        {jobs.length === 0 ? (
          <p className="muted">No jobs yet.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Cron</th>
                <th>Concurrency</th>
                <th>Active</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.job_id}>
                  <td>{job.name}</td>
                  <td>{job.cron_expression ?? "—"}</td>
                  <td>{job.max_concurrency}</td>
                  <td>
                    <span className={`badge ${job.is_active ? "badge-ok" : "badge-neutral"}`}>
                      {job.is_active ? "active" : "paused"}
                    </span>
                  </td>
                  <td>
                    <Link to={`/jobs/${job.job_id}`}>View →</Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
