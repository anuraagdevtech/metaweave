import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { SqlExample } from "../api/types";
import { CodeViewer } from "../components/CodeViewer";
import { ComplexityBadges } from "../components/ComplexityBadges";
import { EmptyState } from "../components/EmptyState";
import { ErrorBanner } from "../components/ErrorBanner";
import { Pagination } from "../components/Pagination";
import { Spinner } from "../components/Spinner";
import { StatTile } from "../components/StatTile";

const PAGE_SIZE = 10;

export function SqlExamples() {
  const [examples, setExamples] = useState<SqlExample[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState<string>("All");
  const [domain, setDomain] = useState<"all" | "banking" | "patterns">("all");
  const [page, setPage] = useState(1);

  useEffect(() => {
    api
      .get<SqlExample[]>("/sql-examples")
      .then(setExamples)
      .catch((e) => setError(String(e)));
  }, []);

  const inDomain = useMemo(
    () => (e: SqlExample) =>
      domain === "all" || (domain === "banking" ? e.domain === "banking" : e.domain !== "banking"),
    [domain]
  );

  const domainScoped = useMemo(() => (examples ?? []).filter(inDomain), [examples, inDomain]);

  const categories = useMemo(
    () => Array.from(new Set(domainScoped.map((e) => e.category).filter((c): c is string => Boolean(c)))).sort(),
    [domainScoped]
  );

  const bankingCount = useMemo(
    () => (examples ?? []).filter((e) => e.domain === "banking").length,
    [examples]
  );

  const workflowCount = useMemo(
    () => new Set(domainScoped.map((e) => e.workflow_id)).size,
    [domainScoped]
  );

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return domainScoped.filter((e) => {
      if (category !== "All" && e.category !== category) return false;
      if (!q) return true;
      return (
        e.task_name.toLowerCase().includes(q) ||
        (e.description ?? "").toLowerCase().includes(q) ||
        e.sql.toLowerCase().includes(q) ||
        e.job_name.toLowerCase().includes(q) ||
        e.workflow_name.toLowerCase().includes(q)
      );
    });
  }, [domainScoped, search, category]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const pageItems = filtered.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  function onFilterChange(fn: () => void) {
    fn();
    setPage(1);
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>SQL Examples Library</h2>
          <p className="page-subtitle">
            Real, runnable SQL — {bankingCount} complex bank finance cases (deposits, loans, FTP, net interest
            income, cost allocation, P&amp;L, RWA, ALM and regulatory reporting, each a multi-CTE join across
            positions, dimensions and reference data) plus a general pattern library — every one tied to the
            job and task that runs it in production.
          </p>
        </div>
      </div>

      {error && <ErrorBanner message={`Backend not reachable yet: ${error}`} />}

      {!examples && !error && (
        <div className="card">
          <Spinner label="Loading SQL examples…" />
        </div>
      )}

      {examples && (
        <>
          <div className="stat-grid">
            <StatTile value={examples.length} label="SQL examples" />
            <StatTile value={bankingCount} label="Complex banking cases" />
            <StatTile value={categories.length} label="Categories" />
            <StatTile value={workflowCount} label="Workflows covered" />
          </div>

          <div className="card">
            <label className="field-label">Library</label>
            <div className="domain-toggle">
              {(
                [
                  ["all", `All (${examples.length})`],
                  ["banking", `Banking — complex joins (${bankingCount})`],
                  ["patterns", `Pattern library (${examples.length - bankingCount})`],
                ] as const
              ).map(([key, label]) => (
                <button
                  key={key}
                  className={`chip${domain === key ? " active" : ""}`}
                  onClick={() =>
                    onFilterChange(() => {
                      setDomain(key);
                      setCategory("All");
                    })
                  }
                >
                  {label}
                </button>
              ))}
            </div>

            <label className="field-label">Search</label>
            <div className="search-field">
              <input
                value={search}
                onChange={(e) => onFilterChange(() => setSearch(e.target.value))}
                placeholder="Search by task name, SQL text, job, or workflow…"
              />
            </div>

            <label className="field-label">Category</label>
            <div className="chip-row">
              <button
                className={`chip${category === "All" ? " active" : ""}`}
                onClick={() => onFilterChange(() => setCategory("All"))}
              >
                All ({domainScoped.length})
              </button>
              {categories.map((cat) => (
                <button
                  key={cat}
                  className={`chip${category === cat ? " active" : ""}`}
                  onClick={() => onFilterChange(() => setCategory(cat))}
                >
                  {cat} ({domainScoped.filter((e) => e.category === cat).length})
                </button>
              ))}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h3>Results</h3>
              <span className="card-header-meta">
                {filtered.length} of {examples.length}
              </span>
            </div>

            {filtered.length === 0 ? (
              <EmptyState
                icon="⌕"
                title="No examples match your filters"
                subtitle="Try a different search term or clear the category filter."
              />
            ) : (
              <>
                {pageItems.map((example) => (
                  <details key={example.task_id} className="example-card">
                    <summary style={{ cursor: "pointer", listStyle: "none" }}>
                      <div className="example-card-header">
                        <p className="example-card-title">{example.task_name}</p>
                        {example.category && <span className="badge badge-accent">{example.category}</span>}
                      </div>
                      <div className="example-card-path">
                        <Link to={`/jobs/${example.job_id}`} onClick={(e) => e.stopPropagation()}>
                          {example.workflow_name}
                        </Link>{" "}
                        / {example.job_name}
                      </div>
                      {example.description && <p className="example-card-desc">{example.description}</p>}
                      <ComplexityBadges complexity={example.complexity} />
                    </summary>
                    <CodeViewer code={example.sql} language="SQL" />
                  </details>
                ))}
                <Pagination page={currentPage} pageCount={pageCount} onChange={setPage} />
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
}
