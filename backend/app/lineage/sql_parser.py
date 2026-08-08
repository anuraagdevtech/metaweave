"""SQL lineage extraction (spec §4): SELECT / INSERT / MERGE / CREATE TABLE AS.

Given a single SQL statement, resolves which tables it reads from, which table
(if any) it writes to, and a column-level source -> target mapping — without
requiring a pre-registered catalog/schema. This is the same class of heuristic
used by tools like `sqlglot.lineage`, `sqllineage`, and OpenLineage's SQL
extractor: parse into an AST, resolve table aliases in scope, then walk each
output-column expression for the `Column` references it draws from.

It intentionally does not attempt to resolve `SELECT *` or deeply nested CTEs
into concrete columns — those require a live catalog and are flagged via
`ColumnLineage(source_column="*", ...)` / unresolved subquery tables rather
than silently guessed.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


class UnsupportedStatementError(ValueError):
    """Raised when the parsed statement isn't SELECT/INSERT/MERGE/CTAS."""


@dataclass(frozen=True)
class ColumnLineage:
    target_table: str | None
    target_column: str
    source_table: str | None
    source_column: str | None
    transformation: str  # raw expression text that produces the target column


@dataclass
class LineageResult:
    statement_type: str  # SELECT | INSERT | MERGE | CREATE_TABLE_AS
    source_tables: list[str] = field(default_factory=list)
    target_table: str | None = None
    column_lineage: list[ColumnLineage] = field(default_factory=list)


def _table_name(table: exp.Table) -> str:
    parts = [p for p in (table.catalog, table.db, table.name) if p]
    return ".".join(parts)


def _alias_map(root: exp.Expression) -> dict[str, str]:
    """Map every alias/bare-name a Column node could reference to its full table name."""
    mapping: dict[str, str] = {}
    for table in root.find_all(exp.Table):
        mapping[table.alias_or_name] = _table_name(table)
    return mapping


def _column_sources(expr: exp.Expression, alias_map: dict[str, str]) -> list[tuple[str | None, str]]:
    cols = list(expr.find_all(exp.Column))
    if not cols:
        return []
    return [(alias_map.get(col.table) if col.table else None, col.name) for col in cols]


def _select_lineage(
    select: exp.Select,
    target_table: str | None,
    declared_columns: list[str] | None,
    alias_map: dict[str, str],
) -> list[ColumnLineage]:
    mappings: list[ColumnLineage] = []
    for idx, proj in enumerate(select.selects):
        if isinstance(proj, exp.Star) or (isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)):
            mappings.append(
                ColumnLineage(target_table, "*", alias_map.get(proj.table) if proj.table else None, "*", proj.sql())
            )
            continue

        output_name = declared_columns[idx] if declared_columns and idx < len(declared_columns) else proj.alias_or_name
        sources = _column_sources(proj, alias_map)
        if not sources:
            # Literal/constant projection, e.g. `SELECT 'batch' AS load_type`.
            mappings.append(ColumnLineage(target_table, output_name, None, None, proj.sql()))
            continue
        for src_table, src_col in sources:
            mappings.append(ColumnLineage(target_table, output_name, src_table, src_col, proj.sql()))
    return mappings


def _merge_lineage(merge: exp.Merge, alias_map: dict[str, str]) -> list[ColumnLineage]:
    target_table = _table_name(merge.this)
    mappings: list[ColumnLineage] = []

    for when in merge.expressions:
        then = when.args.get("then")
        if isinstance(then, exp.Update):
            for eq in then.expressions:
                if not isinstance(eq, exp.EQ):
                    continue
                target_column = eq.this.name
                sources = _column_sources(eq.expression, alias_map)
                if not sources:
                    mappings.append(
                        ColumnLineage(target_table, target_column, None, None, eq.expression.sql())
                    )
                for src_table, src_col in sources:
                    mappings.append(
                        ColumnLineage(target_table, target_column, src_table, src_col, eq.expression.sql())
                    )
        elif isinstance(then, exp.Insert):
            target_cols = [c.name for c in then.this.expressions] if isinstance(then.this, exp.Tuple) else []
            values = then.expression.expressions if isinstance(then.expression, exp.Tuple) else []
            for target_column, value_expr in zip(target_cols, values):
                sources = _column_sources(value_expr, alias_map)
                if not sources:
                    mappings.append(ColumnLineage(target_table, target_column, None, None, value_expr.sql()))
                for src_table, src_col in sources:
                    mappings.append(
                        ColumnLineage(target_table, target_column, src_table, src_col, value_expr.sql())
                    )
    return mappings


def parse_sql_lineage(sql: str, dialect: str = "postgres") -> LineageResult:
    """Parse a single SQL statement and extract table + column-level lineage."""
    tree = sqlglot.parse_one(sql, dialect=dialect)

    if isinstance(tree, exp.Insert):
        target_node = tree.this
        declared_cols = None
        if isinstance(target_node, exp.Schema):
            declared_cols = [c.name for c in target_node.expressions]
            target_node = target_node.this
        target_table = _table_name(target_node)

        source = tree.expression
        if not isinstance(source, exp.Select):
            return LineageResult("INSERT", [], target_table, [])
        alias_map = _alias_map(source)
        return LineageResult(
            statement_type="INSERT",
            source_tables=sorted(set(alias_map.values())),
            target_table=target_table,
            column_lineage=_select_lineage(source, target_table, declared_cols, alias_map),
        )

    if isinstance(tree, exp.Merge):
        target_table = _table_name(tree.this)
        using = tree.args.get("using")
        alias_map = _alias_map(using) if using is not None else {}
        source_tables = sorted(set(alias_map.values()))
        return LineageResult(
            statement_type="MERGE",
            source_tables=source_tables,
            target_table=target_table,
            column_lineage=_merge_lineage(tree, alias_map),
        )

    if isinstance(tree, exp.Create) and tree.args.get("kind") == "TABLE" and isinstance(tree.expression, exp.Select):
        target_node = tree.this
        declared_cols = None
        if isinstance(target_node, exp.Schema):
            declared_cols = [c.name for c in target_node.expressions]
            target_node = target_node.this
        target_table = _table_name(target_node)

        source = tree.expression
        alias_map = _alias_map(source)
        return LineageResult(
            statement_type="CREATE_TABLE_AS",
            source_tables=sorted(set(alias_map.values())),
            target_table=target_table,
            column_lineage=_select_lineage(source, target_table, declared_cols, alias_map),
        )

    if isinstance(tree, exp.Select):
        alias_map = _alias_map(tree)
        return LineageResult(
            statement_type="SELECT",
            source_tables=sorted(set(alias_map.values())),
            target_table=None,
            column_lineage=_select_lineage(tree, None, None, alias_map),
        )

    raise UnsupportedStatementError(
        f"Unsupported statement type for lineage extraction: {type(tree).__name__}"
    )


def parse_sql_script(sql: str, dialect: str = "postgres") -> list[LineageResult]:
    """Parse a multi-statement script (semicolon separated) into per-statement lineage."""
    results: list[LineageResult] = []
    for statement in sqlglot.parse(sql, dialect=dialect):
        if statement is None:
            continue
        try:
            results.append(parse_sql_lineage(statement.sql(dialect=dialect), dialect=dialect))
        except UnsupportedStatementError:
            continue
    return results
