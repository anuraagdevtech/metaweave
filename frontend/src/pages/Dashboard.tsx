import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { Job, SqlExample, Workflow } from "../api/types";
import { EmptyState } from "../components/EmptyState";
import { ErrorBanner } from "../components/ErrorBanner";
import { SkeletonRows } from "../components/SkeletonRows";
import { StatTile } from "../components/StatTile";

export function Dashboard() {
  const [workflows, setWorkflows] = useState<Workflow[] | null>(null);
  const [jobs, setJobs] = useState<Job[] | null>(null);
  const [sqlExamples, setSqlExamples] = useState<SqlExample[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  useEffect(() => {
    Promise.all([
      api.get<Workflow[]>("/workflows"),
      api.get<Job[]>("/jobs"),
      api.get<SqlExample[]>("/sql-examples"),
    ])
      .then(([w, j, s]) => {
        setWorkflows(w);
        setJobs(j);
        setSqlExamples(s);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const activeJobCount = useMemo(() => jobs?.filter((j) => j.is_active).length ?? 0, [jobs]);

  const filteredJobs = useMemo(() => {
    if (!jobs) return [];
    const q = search.trim().toLowerCase();
    if (!q) return jobs;
    return jobs.filter(
      (j) =>
        j.name.toLowerCase().includes(q) ||
        (workflows?.find((w) => w.workflow_id === j.workflow_id)?.name.toLowerCase().includes(q) ?? false)
    );
  }, [jobs, workflows, search]);

  const loading = !workflows || !jobs || !sqlExamples;

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>Runbook Dashboard</h2>
          <p className="page-subtitle">
            An overview of every tracked workflow, job and SQL task — jump into a job for its DAG, code and
            run history, or browse the full SQL example library.
          </p>
        </div>
      </div>

      {error && <ErrorBanner message={`Backend not reachable yet: ${error}`} />}

      {!loading && (
        <div className="stat-grid">
          <StatTile value={workflows.length} label="Workflows" />
          <StatTile value={jobs.length} label="Jobs" />
          <StatTile value={activeJobCount} label="Active jobs" />
          <StatTile value={sqlExamples.length} label="SQL examples" />
        </div>
      )}

      <div className="card">
        <div className="card-header">
          <h3>Workflows</h3>
        </div>
        {!workflows ? (
          <SkeletonRows count={3} />
        ) : workflows.length === 0 ? (
          <EmptyState
            icon="⚏"
            title="No workflows yet"
            subtitle="Create one via the API, or run backend/scripts/seed.py to load the demo dataset."
          />
        ) : (
          <div className="table-scroll">
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
                    <td>{jobs?.filter((j) => j.workflow_id === wf.workflow_id).length ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-header">
          <h3>Jobs</h3>
        </div>
        {!jobs ? (
          <SkeletonRows count={5} />
        ) : jobs.length === 0 ? (
          <EmptyState icon="⚙" title="No jobs yet" subtitle="Jobs will appear here once a workflow defines them." />
        ) : (
          <>
            <div className="search-field" style={{ marginBottom: 12 }}>
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Filter jobs by name or workflow…" />
            </div>
            {filteredJobs.length === 0 ? (
              <EmptyState icon="⌕" title="No jobs match your search" />
            ) : (
              <div className="table-scroll">
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
                    {filteredJobs.map((job) => (
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
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
