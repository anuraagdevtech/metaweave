"""PySpark script lineage extraction (spec §4) via Python's `ast` module.

This is a static, heuristic parser — it does not execute the script or know
the real schema of any table. It walks each DataFrame variable's call chain
(`spark.table(...)`, `spark.read...load(...)`, `.join`, `.withColumn`,
`.withColumnRenamed`, `.select`, `.write...saveAsTable`/`.save`/`.insertInto`)
and produces the same `LineageResult`/`ColumnLineage` shape the SQL parser
produces, so the API layer can treat SQL and PySpark tasks uniformly.

Known limitation (documented, not silently guessed): once two frames are
`.join`-ed, a downstream column's source table is ambiguous, so
`source_table` is left `None` for columns derived after a join unless the
base frame still has exactly one source table.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field

from app.lineage.sql_parser import ColumnLineage, LineageResult

_WRITE_METHODS = {"saveAsTable", "save", "insertInto"}
_PASSTHROUGH_TRANSFORMS = {
    "filter", "where", "groupBy", "agg", "orderBy", "sort", "dropDuplicates",
    "distinct", "alias", "cache", "persist", "repartition", "coalesce", "limit",
    "na", "drop", "fillna", "withWatermark", "hint",
}

# (target_column, source_table, source_column, transformation)
_ColDeriv = tuple[str, str | None, str | None, str]


@dataclass
class _Frame:
    source_tables: set[str] = field(default_factory=set)
    columns: list[_ColDeriv] = field(default_factory=list)


def _const_str(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _single_source(frame: _Frame) -> str | None:
    return next(iter(frame.source_tables)) if len(frame.source_tables) == 1 else None


def _is_spark_name(node: ast.expr) -> bool:
    return isinstance(node, ast.Name) and node.id == "spark"


def _contains_spark_read(node: ast.expr) -> bool:
    return any(
        isinstance(n, ast.Attribute) and n.attr == "read" and _is_spark_name(n.value)
        for n in ast.walk(node)
    )


def _extract_colname(node: ast.expr) -> str | None:
    literal = _const_str(node)
    if literal:
        return literal
    if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute)):
        func_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        if func_name == "col" and node.args:
            return _const_str(node.args[0])
    return None


def _find_col_refs(expr: ast.expr) -> list[str]:
    refs: list[str] = []
    for node in ast.walk(expr):
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute)):
            func_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr
            if func_name == "col" and node.args:
                name = _const_str(node.args[0])
                if name:
                    refs.append(name)
    return refs


def _resolve(node: ast.expr, env: dict[str, _Frame]) -> _Frame:
    if isinstance(node, ast.Name):
        return env.get(node.id, _Frame())

    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return _Frame()

    method = node.func.attr
    base_node = node.func.value

    if method in ("table", "load") and (_is_spark_name(base_node) or _contains_spark_read(base_node)):
        source = _const_str(node.args[0]) if node.args else None
        return _Frame({source} if source else set(), [])

    base = _resolve(base_node, env)

    if method == "join":
        other = _resolve(node.args[0], env) if node.args else _Frame()
        return _Frame(base.source_tables | other.source_tables, base.columns + other.columns)

    if method == "withColumn" and len(node.args) >= 2:
        name = _const_str(node.args[0])
        expr = node.args[1]
        if name is None:
            return base
        transform = ast.unparse(expr)
        table = _single_source(base)
        refs = _find_col_refs(expr)
        new_deriv = [(name, table, ref, transform) for ref in refs] or [(name, table, None, transform)]
        return _Frame(base.source_tables, base.columns + new_deriv)

    if method == "withColumnRenamed" and len(node.args) >= 2:
        old, new = _const_str(node.args[0]), _const_str(node.args[1])
        if old is None or new is None:
            return base
        table = _single_source(base)
        return _Frame(base.source_tables, base.columns + [(new, table, old, f"rename({old} -> {new})")])

    if method == "select":
        table = _single_source(base)
        new_deriv = []
        for arg in node.args:
            colname = _extract_colname(arg)
            if colname:
                new_deriv.append((colname, table, colname, colname))
        return _Frame(base.source_tables, base.columns + new_deriv)

    if method in _PASSTHROUGH_TRANSFORMS or method not in _WRITE_METHODS:
        return base

    return base


def _find_write(call: ast.Call) -> ast.expr | None:
    """If `call` is `<df>.write[...].saveAsTable/save/insertInto(...)`, return `<df>`."""
    if not isinstance(call.func, ast.Attribute) or call.func.attr not in _WRITE_METHODS:
        return None
    cur: ast.expr = call.func.value
    while True:
        if isinstance(cur, ast.Attribute):
            if cur.attr == "write":
                return cur.value
            cur = cur.value
        elif isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
            cur = cur.func.value
        else:
            return None


def parse_pyspark_lineage(code: str) -> list[LineageResult]:
    """Parse a PySpark script and return one LineageResult per write action found."""
    tree = ast.parse(code)
    env: dict[str, _Frame] = {}
    results: list[LineageResult] = []

    for stmt in tree.body:
        value: ast.expr | None = None
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            value = stmt.value
            env[stmt.targets[0].id] = _resolve(value, env)
        elif isinstance(stmt, ast.Expr):
            value = stmt.value

        if isinstance(value, ast.Call):
            df_expr = _find_write(value)
            if df_expr is not None:
                target_table = _const_str(value.args[0]) if value.args else None
                frame = _resolve(df_expr, env)
                column_lineage = [
                    ColumnLineage(target_table, name, src_table, src_col, transform)
                    for name, src_table, src_col, transform in frame.columns
                ]
                results.append(
                    LineageResult(
                        statement_type="PYSPARK",
                        source_tables=sorted(frame.source_tables),
                        target_table=target_table,
                        column_lineage=column_lineage,
                    )
                )

    return results
