import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import { ComplexityBadges } from "../components/ComplexityBadges";
import { EmptyState } from "../components/EmptyState";
import { ErrorBanner } from "../components/ErrorBanner";
import { Spinner } from "../components/Spinner";
import type { LineageResult, SqlExample } from "../api/types";

const SAMPLE_SQL = `INSERT INTO analytics.customer_summary (customer_id, total_orders, total_revenue)
SELECT c.customer_id, COUNT(o.order_id) AS total_orders, SUM(o.amount) AS total_revenue
FROM sales.customers c
JOIN sales.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id;`;

const SAMPLE_PYSPARK = `df = spark.table("sales.customers")
orders = spark.read.format("delta").load("staging.orders")
joined = df.join(orders, "customer_id")
result = joined.withColumn("total", col("amount") * 2)
result.write.format("delta").mode("overwrite").saveAsTable("analytics.customer_totals")`;

export function Lineage() {
  const [mode, setMode] = useState<"sql" | "pyspark">("sql");
  const [code, setCode] = useState(SAMPLE_SQL);
  const [results, setResults] = useState<LineageResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [resultView, setResultView] = useState<"table" | "attribute">("table");

  const [examples, setExamples] = useState<SqlExample[] | null>(null);
  const [exampleSearch, setExampleSearch] = useState("");
  const [exampleDomain, setExampleDomain] = useState<"banking" | "all">("banking");
  const [exampleCategory, setExampleCategory] = useState("All");
  const [selectedExampleId, setSelectedExampleId] = useState<string | null>(null);

  useEffect(() => {
    api
      .get<SqlExample[]>("/sql-examples")
      .then(setExamples)
      .catch(() => {
        /* the case browser is a convenience — parsing still works without it */
      });
  }, []);

  const domainExamples = useMemo(
    () => (examples ?? []).filter((e) => exampleDomain === "all" || e.domain === "banking"),
    [examples, exampleDomain]
  );

  const bankingCount = useMemo(
    () => (examples ?? []).filter((e) => e.domain === "banking").length,
    [examples]
  );

  const exampleCategories = useMemo(
    () => Array.from(new Set(domainExamples.map((e) => e.category).filter((c): c is string => Boolean(c)))).sort(),
    [domainExamples]
  );

  const filteredExamples = useMemo(() => {
    const q = exampleSearch.trim().toLowerCase();
    return domainExamples.filter((e) => {
      if (exampleCategory !== "All" && e.category !== exampleCategory) return false;
      if (!q) return true;
      return (
        e.task_name.toLowerCase().includes(q) ||
        (e.description ?? "").toLowerCase().includes(q) ||
        e.job_name.toLowerCase().includes(q) ||
        e.workflow_name.toLowerCase().includes(q)
      );
    });
  }, [domainExamples, exampleSearch, exampleCategory]);

  const attributeRows = useMemo(
    () =>
      (results ?? []).flatMap((r, statementIndex) =>
        r.column_lineage.map((c, columnIndex) => ({ ...c, statementIndex, columnIndex }))
      ),
    [results]
  );

  async function runParse(sourceCode: string, sourceMode: "sql" | "pyspark") {
    setError(null);
    setLoading(true);
    try {
      const path = sourceMode === "sql" ? "/lineage/parse-sql" : "/lineage/parse-pyspark";
      const body = sourceMode === "sql" ? { sql: sourceCode } : { code: sourceCode };
      const res = await api.post<LineageResult[]>(path, body);
      setResults(res);
      setResultView("table");
    } catch (e) {
      setError(String(e));
      setResults(null);
    } finally {
      setLoading(false);
    }
  }

  function loadExample(example: SqlExample) {
    setSelectedExampleId(example.task_id);
    setMode("sql");
    setCode(example.sql);
    runParse(example.sql, "sql");
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>Lineage Parser</h2>
          <p className="page-subtitle">
            Paste a SQL statement or PySpark script — or load one of {bankingCount || "50+"} complex bank
            finance cases below (multi-CTE joins across deposits, loans, FTP, allocations, P&amp;L, RWA and
            regulatory reporting) — to extract table- and column-level lineage.
          </p>
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <h3>Browse example cases</h3>
          <span className="card-header-meta">
            {examples ? `${filteredExamples.length} of ${domainExamples.length}` : <Spinner label="Loading cases…" />}
          </span>
        </div>

        {examples && (
          <>
            <div className="search-field" style={{ marginBottom: 10 }}>
              <input
                value={exampleSearch}
                onChange={(e) => setExampleSearch(e.target.value)}
                placeholder="Search cases by task, job, or workflow…"
              />
            </div>
            <div className="domain-toggle">
              <button
                className={`chip${exampleDomain === "banking" ? " active" : ""}`}
                onClick={() => {
                  setExampleDomain("banking");
                  setExampleCategory("All");
                }}
              >
                Banking — complex joins ({bankingCount})
              </button>
              <button
                className={`chip${exampleDomain === "all" ? " active" : ""}`}
                onClick={() => {
                  setExampleDomain("all");
                  setExampleCategory("All");
                }}
              >
                All cases ({examples.length})
              </button>
            </div>
            <div className="chip-row" style={{ marginBottom: 12 }}>
              <button
                className={`chip${exampleCategory === "All" ? " active" : ""}`}
                onClick={() => setExampleCategory("All")}
              >
                All ({domainExamples.length})
              </button>
              {exampleCategories.map((cat) => (
                <button
                  key={cat}
                  className={`chip${exampleCategory === cat ? " active" : ""}`}
                  onClick={() => setExampleCategory(cat)}
                >
                  {cat} ({domainExamples.filter((e) => e.category === cat).length})
                </button>
              ))}
            </div>

            {filteredExamples.length === 0 ? (
              <EmptyState icon="⌕" title="No cases match your filters" />
            ) : (
              <div style={{ maxHeight: 280, overflowY: "auto", paddingRight: 4 }}>
                {filteredExamples.map((example) => (
                  <button
                    key={example.task_id}
                    className="btn-secondary"
                    onClick={() => loadExample(example)}
                    disabled={loading}
                    style={{
                      display: "block",
                      width: "100%",
                      textAlign: "left",
                      marginTop: 6,
                      borderColor: example.task_id === selectedExampleId ? "var(--accent)" : undefined,
                      background: example.task_id === selectedExampleId ? "var(--accent-dim)" : undefined,
                    }}
                  >
                    <span style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                      <span>{example.task_name}</span>
                      {example.category && <span className="badge badge-accent">{example.category}</span>}
                    </span>
                    <span style={{ display: "flex", flexWrap: "wrap", alignItems: "baseline", gap: 12, marginTop: 2 }}>
                      <span className="example-card-path">
                        {example.workflow_name} / {example.job_name}
                      </span>
                      <ComplexityBadges complexity={example.complexity} />
                    </span>
                  </button>
                ))}
              </div>
            )}
          </>
        )}
      </div>

      <div className="card">
        <div className="dag-row">
          <button
            className={mode === "sql" ? "" : "btn-secondary"}
            onClick={() => {
              setMode("sql");
              setCode(SAMPLE_SQL);
              setSelectedExampleId(null);
            }}
          >
            SQL
          </button>
          <button
            className={mode === "pyspark" ? "" : "btn-secondary"}
            onClick={() => {
              setMode("pyspark");
              setCode(SAMPLE_PYSPARK);
              setSelectedExampleId(null);
            }}
          >
            PySpark
          </button>
        </div>
        <label className="field-label">{mode === "sql" ? "SQL statement(s)" : "PySpark script"}</label>
        <textarea
          value={code}
          onChange={(e) => {
            setCode(e.target.value);
            setSelectedExampleId(null);
          }}
        />
        <div>
          <button onClick={() => runParse(code, mode)} disabled={loading || !code.trim()}>
            {loading ? <Spinner label="Parsing…" /> : "Extract Lineage"}
          </button>
        </div>
        {error && <ErrorBanner message={error} />}
      </div>

      {results && (
        <div className="card">
          <div className="card-header">
            <h3>Results</h3>
            <span className="card-header-meta">{results.length} statement{results.length === 1 ? "" : "s"}</span>
          </div>

          <div className="chip-row" style={{ marginBottom: 16 }}>
            <button
              className={`chip${resultView === "table" ? " active" : ""}`}
              onClick={() => setResultView("table")}
            >
              Table Lineage
            </button>
            <button
              className={`chip${resultView === "attribute" ? " active" : ""}`}
              onClick={() => setResultView("attribute")}
            >
              Attribute Lineage ({attributeRows.length})
            </button>
          </div>

          {resultView === "table" ? (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Statement Type</th>
                    <th>Target Table</th>
                    <th>Source Tables</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => (
                    <tr key={i}>
                      <td>{i + 1}</td>
                      <td>
                        <span className="badge badge-neutral">{r.statement_type}</span>
                      </td>
                      <td>{r.target_table ? <strong>{r.target_table}</strong> : "—"}</td>
                      <td className="wrap">
                        {r.source_tables.length > 0 ? (
                          <div className="chip-row">
                            {r.source_tables.map((t) => (
                              <span key={t} className="chip" style={{ cursor: "default" }}>
                                {t}
                              </span>
                            ))}
                          </div>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : attributeRows.length === 0 ? (
            <EmptyState
              icon="⛓"
              title="No column-level lineage extracted"
              subtitle="This statement type doesn't produce column-to-column mappings."
            />
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Target Table</th>
                    <th>Target Column</th>
                    <th>Source Table</th>
                    <th>Source Column</th>
                    <th>Transformation</th>
                  </tr>
                </thead>
                <tbody>
                  {attributeRows.map((c) => (
                    <tr key={`${c.statementIndex}-${c.columnIndex}`}>
                      <td>{c.statementIndex + 1}</td>
                      <td>{c.target_table ?? "—"}</td>
                      <td>{c.target_column}</td>
                      <td>{c.source_table ?? "—"}</td>
                      <td>{c.source_column ?? "—"}</td>
                      <td>
                        <code>{c.transformation}</code>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
