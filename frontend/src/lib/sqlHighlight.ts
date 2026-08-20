const KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS",
  "ON", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "INSERT", "INTO", "VALUES",
  "UPDATE", "SET", "DELETE", "MERGE", "USING", "WHEN", "MATCHED", "NOT", "THEN",
  "ELSE", "END", "CASE", "AS", "AND", "OR", "IN", "IS", "NULL", "NULLIF", "LIKE",
  "BETWEEN", "DISTINCT", "CREATE", "TABLE", "OR REPLACE", "REPLACE", "WITH",
  "OVER", "PARTITION", "ROWS", "RANGE", "PRECEDING", "FOLLOWING", "CURRENT",
  "ROW", "UNBOUNDED", "UNION", "ALL", "EXISTS", "CAST", "INTERVAL", "DAY",
  "MONTH", "YEAR", "HOUR", "MINUTE", "SECOND", "DESC", "ASC", "BEGIN", "COMMIT",
  "ROLLBACK", "TRUNCATE", "ALTER", "ADD", "COLUMN", "DEFAULT", "PRIMARY", "KEY",
  "FOREIGN", "REFERENCES", "CHECK", "CONSTRAINT", "TRUE", "FALSE",
]);

const FUNCTIONS = new Set([
  "SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE", "ROUND", "ABS",
  "COALESCE", "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "DATE_TRUNC",
  "DATEDIFF", "GENERATE_SERIES", "NOW", "CURRENT_TIMESTAMP", "CURRENT_DATE",
  "EXTRACT", "CONCAT", "SUBSTRING", "TRIM", "LOWER", "UPPER", "LENGTH", "NTILE",
]);

const TOKEN_PATTERN = /(--[^\n]*)|('(?:[^']|'')*')|(\b\d+\.?\d*\b)|(\b[A-Za-z_][A-Za-z0-9_]*\b)/g;

function escapeHtml(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/** Minimal, dependency-free SQL syntax highlighter. Returns safe HTML. */
export function highlightSql(sql: string): string {
  const escaped = escapeHtml(sql);
  return escaped.replace(TOKEN_PATTERN, (match, comment, str, num, word) => {
    if (comment) return `<span class="tok-comment">${comment}</span>`;
    if (str) return `<span class="tok-string">${str}</span>`;
    if (num) return `<span class="tok-number">${num}</span>`;
    if (word) {
      const upper = word.toUpperCase();
      if (KEYWORDS.has(upper)) return `<span class="tok-keyword">${word}</span>`;
      if (FUNCTIONS.has(upper)) return `<span class="tok-function">${word}</span>`;
    }
    return match;
  });
}
