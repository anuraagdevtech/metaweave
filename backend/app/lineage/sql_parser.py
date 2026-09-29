"""SQL lineage extraction (spec §4): SELECT / INSERT / MERGE / CREATE TABLE AS.

Given a single SQL statement, resolves which tables it reads from, which table
(if any) it writes to, and a column-level source -> target mapping — without
requiring a pre-registered catalog/schema. This is the same class of heuristic
used by tools like `sqlglot.lineage`, `sqllineage`, and OpenLineage's SQL
extractor: parse into an AST, resolve table aliases in scope, then walk each
output-column expression for the `Column` references it draws from.

Alias resolution is **scope-aware**. A real warehouse statement reuses short
aliases across scopes — `FROM fact.loan_balance_daily l` inside one CTE and
`JOIN liabilities l` in the outer query — so a single flattened alias map
silently attributes columns to the wrong table. Each SELECT is resolved
against only the sources in its own FROM/JOIN clause, and a reference into a
CTE or derived table is followed through to the physical column it selects
(bounded by `_MAX_SCOPE_DEPTH`, with a cycle guard for recursive CTEs).

It intentionally does not attempt to resolve `SELECT *` into concrete columns
— that requires a live catalog — and flags it via
`ColumnLineage(source_column="*", ...)` rather than silently guessing.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

_MAX_SCOPE_DEPTH = 8


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


def _cte_names(root: exp.Expression) -> set[str]:
    """Names bound by `WITH` clauses anywhere in the statement.

    A reference to a CTE parses as an `exp.Table`, so without this it would be
    reported as a physical source table — badly misleading on the multi-CTE
    statements that dominate real warehouse SQL.
    """
    return {cte.alias_or_name for cte in root.find_all(exp.CTE)}


def _physical_sources(root: exp.Expression) -> list[str]:
    """Distinct physical tables read, excluding intermediate CTE names.

    Walks the `Table` nodes directly rather than a flattened alias map: an
    alias reused across two scopes would collapse to a single entry there and
    silently drop a source table from the result.
    """
    ctes = _cte_names(root)
    return sorted({_table_name(t) for t in root.find_all(exp.Table)} - ctes)


def _named_selects(root: exp.Expression) -> dict[str, exp.Select]:
    """Every name a column reference can be followed *into*.

    Covers `WITH` bindings and derived tables (`FROM (SELECT ...) alias`), so
    `alias.column` can be resolved to the physical column it ultimately reads.
    """
    scopes: dict[str, exp.Select] = {}
    for cte in root.find_all(exp.CTE):
        inner = cte.this
        if isinstance(inner, exp.Select):
            scopes[cte.alias_or_name] = inner
    for subquery in root.find_all(exp.Subquery):
        alias = subquery.alias_or_name
        inner = subquery.this
        if alias and isinstance(inner, exp.Select):
            scopes.setdefault(alias, inner)
    return scopes


def _source_nodes(select: exp.Select) -> list[exp.Expression]:
    """The FROM and JOIN targets of this select — not those of nested scopes."""
    nodes: list[exp.Expression] = []
    from_clause = select.args.get("from")
    if from_clause is not None:
        nodes.append(from_clause.this)
    for join in select.args.get("joins") or []:
        nodes.append(join.this)
    return [node for node in nodes if node is not None]


def _local_alias_map(select: exp.Select) -> dict[str, str]:
    """Alias -> source name for this select's own scope only.

    A derived table or CTE reference maps to its *name*, which
    `_resolve_column` then follows into, rather than leaking the tables
    nested inside it into this scope.
    """
    mapping: dict[str, str] = {}
    for node in _source_nodes(select):
        if isinstance(node, exp.Table):
            mapping[node.alias_or_name] = _table_name(node)
        elif isinstance(node, exp.Subquery):
            alias = node.alias_or_name
            if alias:
                mapping[alias] = alias
    return mapping


def _sole_source(alias_map: dict[str, str]) -> str | None:
    """The single source of a scope, so unqualified columns can be attributed."""
    distinct = set(alias_map.values())
    return next(iter(distinct)) if len(distinct) == 1 else None


def _projection_for(select: exp.Select, column: str) -> exp.Expression | None:
    for projection in select.selects:
        if isinstance(projection, exp.Star):
            return None
        if projection.alias_or_name == column:
            return projection
    return None


def _resolve_column(
    table_alias: str,
    column: str,
    alias_map: dict[str, str],
    scopes: dict[str, exp.Select],
    depth: int,
    seen: frozenset[str],
) -> list[tuple[str | None, str]]:
    """Resolve one `alias.column` reference to (physical table, column) pairs.

    Follows CTE and derived-table references through to what they select. If
    the chain cannot be followed — a recursive CTE, a `SELECT *`, a missing
    projection — the CTE's own name is returned, which is accurate as far as
    it goes rather than wrong.
    """
    resolved = alias_map.get(table_alias) if table_alias else _sole_source(alias_map)
    if resolved is None:
        return [(None, column)]

    inner = scopes.get(resolved)
    if inner is None or depth >= _MAX_SCOPE_DEPTH or resolved in seen:
        return [(resolved, column)]

    projection = _projection_for(inner, column)
    if projection is None:
        return [(resolved, column)]

    inner_aliases = _local_alias_map(inner)
    references = list(projection.find_all(exp.Column))
    if not references:
        return [(resolved, column)]

    resolved_pairs: list[tuple[str | None, str]] = []
    for reference in references:
        resolved_pairs.extend(
            _resolve_column(
                reference.table,
                reference.name,
                inner_aliases,
                scopes,
                depth + 1,
                seen | {resolved},
            )
        )
    return resolved_pairs or [(resolved, column)]


def _column_sources(
    expr: exp.Expression,
    alias_map: dict[str, str],
    scopes: dict[str, exp.Select],
) -> list[tuple[str | None, str]]:
    references = list(expr.find_all(exp.Column))
    if not references:
        return []
    sources: list[tuple[str | None, str]] = []
    for reference in references:
        sources.extend(
            _resolve_column(reference.table, reference.name, alias_map, scopes, 0, frozenset())
        )
    # preserve order, drop duplicates introduced by fan-out through a CTE
    seen: set[tuple[str | None, str]] = set()
    unique: list[tuple[str | None, str]] = []
    for source in sources:
        if source not in seen:
            seen.add(source)
            unique.append(source)
    return unique


def _select_lineage(
    select: exp.Select,
    target_table: str | None,
    declared_columns: list[str] | None,
    scopes: dict[str, exp.Select],
) -> list[ColumnLineage]:
    alias_map = _local_alias_map(select)
    mappings: list[ColumnLineage] = []
    for idx, proj in enumerate(select.selects):
        if isinstance(proj, exp.Star) or (isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)):
            mappings.append(
                ColumnLineage(target_table, "*", alias_map.get(proj.table) if proj.table else None, "*", proj.sql())
            )
            continue

        output_name = declared_columns[idx] if declared_columns and idx < len(declared_columns) else proj.alias_or_name
        sources = _column_sources(proj, alias_map, scopes)
        if not sources:
            # Literal/constant projection, e.g. `SELECT 'batch' AS load_type`.
            mappings.append(ColumnLineage(target_table, output_name, None, None, proj.sql()))
            continue
        for src_table, src_col in sources:
            mappings.append(ColumnLineage(target_table, output_name, src_table, src_col, proj.sql()))
    return mappings


def _merge_lineage(
    merge: exp.Merge, alias_map: dict[str, str], scopes: dict[str, exp.Select]
) -> list[ColumnLineage]:
    target_table = _table_name(merge.this)
    mappings: list[ColumnLineage] = []

    for when in merge.expressions:
        then = when.args.get("then")
        if isinstance(then, exp.Update):
            for eq in then.expressions:
                if not isinstance(eq, exp.EQ):
                    continue
                target_column = eq.this.name
                sources = _column_sources(eq.expression, alias_map, scopes)
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
                sources = _column_sources(value_expr, alias_map, scopes)
                if not sources:
                    mappings.append(ColumnLineage(target_table, target_column, None, None, value_expr.sql()))
                for src_table, src_col in sources:
                    mappings.append(
                        ColumnLineage(target_table, target_column, src_table, src_col, value_expr.sql())
                    )
    return mappings


def _merge_alias_map(using: exp.Expression) -> dict[str, str]:
    """`USING <table|subquery> AS src` -> {src: name}."""
    if isinstance(using, exp.Table):
        return {using.alias_or_name: _table_name(using)}
    if isinstance(using, exp.Subquery) and using.alias_or_name:
        return {using.alias_or_name: using.alias_or_name}
    return {}


def parse_sql_lineage(sql: str, dialect: str = "postgres") -> LineageResult:
    """Parse a single SQL statement and extract table + column-level lineage."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    scopes = _named_selects(tree)

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
        return LineageResult(
            statement_type="INSERT",
            source_tables=_physical_sources(source),
            target_table=target_table,
            column_lineage=_select_lineage(source, target_table, declared_cols, scopes),
        )

    if isinstance(tree, exp.Merge):
        target_table = _table_name(tree.this)
        using = tree.args.get("using")
        return LineageResult(
            statement_type="MERGE",
            source_tables=_physical_sources(using) if using is not None else [],
            target_table=target_table,
            column_lineage=_merge_lineage(tree, _merge_alias_map(using) if using is not None else {}, scopes),
        )

    if isinstance(tree, exp.Create) and tree.args.get("kind") == "TABLE" and isinstance(tree.expression, exp.Select):
        target_node = tree.this
        declared_cols = None
        if isinstance(target_node, exp.Schema):
            declared_cols = [c.name for c in target_node.expressions]
            target_node = target_node.this
        target_table = _table_name(target_node)

        source = tree.expression
        return LineageResult(
            statement_type="CREATE_TABLE_AS",
            source_tables=_physical_sources(source),
            target_table=target_table,
            column_lineage=_select_lineage(source, target_table, declared_cols, scopes),
        )

    if isinstance(tree, exp.Select):
        return LineageResult(
            statement_type="SELECT",
            source_tables=_physical_sources(tree),
            target_table=None,
            column_lineage=_select_lineage(tree, None, None, scopes),
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
