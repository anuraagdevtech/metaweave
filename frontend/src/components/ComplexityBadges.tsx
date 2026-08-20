import type { SqlComplexity } from "../api/types";

/** Compact "3 CTEs · 7 joins · 9 tables" strip shown on complex SQL examples. */
export function ComplexityBadges({ complexity }: { complexity: SqlComplexity | null }) {
  if (!complexity) return null;
  const parts = [
    { value: complexity.ctes, label: complexity.ctes === 1 ? "CTE" : "CTEs" },
    { value: complexity.joins, label: complexity.joins === 1 ? "join" : "joins" },
    { value: complexity.tables, label: complexity.tables === 1 ? "table" : "tables" },
    { value: complexity.lines, label: "lines" },
  ].filter((p) => p.value > 0);

  return (
    <span className="complexity-strip">
      {parts.map((p, i) => (
        <span key={p.label}>
          {i > 0 && <span className="complexity-sep">·</span>}
          <strong>{p.value}</strong> {p.label}
        </span>
      ))}
    </span>
  );
}
