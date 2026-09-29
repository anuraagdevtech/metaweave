"""Job lineage from Airflow DAG files, via Python's `ast` module.

Static analysis only — the DAG file is never imported or executed, so this is
safe to point at an unfamiliar repository and works without Airflow installed
or any connection configured.

What it recovers:

* the DAG itself — `DAG(...)`, `with DAG(...) as dag:`, or an `@dag`-decorated
  function — with its `dag_id`, schedule, owner and tags;
* its tasks — any `*Operator`/`*Sensor` call assigned to a variable, plus
  TaskFlow `@task`-decorated functions;
* intra-DAG dependencies from `>>` / `<<` chains (including list fan-out/fan-in),
  `.set_upstream()` / `.set_downstream()`, and `chain(...)`;
* cross-DAG dependencies from `ExternalTaskSensor(external_dag_id=...)` and
  `TriggerDagRunOperator(trigger_dag_id=...)`;
* Dataset inlets/outlets, which Airflow uses for data-aware scheduling;
* the code each task runs — inline `sql=`, a `.sql` file reference, a Spark
  `application=`, a notebook path — so the table/attribute layer has something
  to parse.

Anything it cannot resolve statically (an f-string `dag_id`, a task built in a
loop over a runtime value) is reported as a warning rather than guessed at.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from app.extract.models import ExtractedJob, ExtractedTask

_SQL_KWARGS = ("sql", "query")
_SPARK_KWARGS = ("application", "python_file", "main_class_name")
_NOTEBOOK_KWARGS = ("notebook_path", "notebook_task")
_BASH_KWARGS = ("bash_command",)

_DAG_KWARG_ALIASES = {
    "schedule": ("schedule", "schedule_interval", "timetable"),
    "description": ("description",),
}


def _literal(node: ast.expr | None, env: dict | None = None):
    """Best-effort constant folding; returns None when not statically known.

    `env` carries module-level constants, so the near-universal Airflow idiom
    of defining `default_args = {...}` above the DAG and passing it by name
    resolves instead of being dropped.
    """
    if node is None:
        return None
    if isinstance(node, ast.Name) and env and node.id in env:
        return env[node.id]
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError, TypeError):
        return None


def module_constants(tree: ast.AST) -> dict:
    """Module-level `NAME = <literal>` bindings, for resolving kwargs by name."""
    constants: dict = {}
    for node in getattr(tree, "body", []):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            constants[target.id] = ast.literal_eval(node.value)
        except (ValueError, SyntaxError, TypeError):
            continue
    return constants


def _kwarg(call: ast.Call, *names: str) -> ast.expr | None:
    for keyword in call.keywords:
        if keyword.arg in names:
            return keyword.value
    return None


def _callee_name(node: ast.expr) -> str:
    """`PythonOperator` / `airflow.operators.PythonOperator` -> `PythonOperator`."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _is_operator_call(node: ast.expr) -> bool:
    if not isinstance(node, ast.Call):
        return False
    name = _callee_name(node.func)
    return name.endswith(("Operator", "Sensor")) and name not in {"Operator", "Sensor"}


def _dataset_uris(node: ast.expr | None, env: dict | None = None) -> list[str]:
    """Pull URIs out of `Dataset("...")` calls, however they are nested."""
    if node is None:
        return []
    uris: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call) and _callee_name(child.func) in {"Dataset", "Asset"}:
            if child.args:
                uri = _literal(child.args[0], env)
                if isinstance(uri, str):
                    uris.append(uri)
    return uris


@dataclass
class _TaskVar:
    """A python variable bound to a task, and the task it refers to."""

    task_id: str
    node: ast.Call | None = None


@dataclass
class _DagScope:
    dag_id: str
    schedule: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    inlets: list[str] = field(default_factory=list)


class _DagFileVisitor(ast.NodeVisitor):
    def __init__(self, path: Path, root: Path, constants: dict | None = None) -> None:
        self.path = path
        self.root = root
        self.constants = constants or {}
        self.dags: list[_DagScope] = []
        self.tasks: dict[str, ExtractedTask] = {}   # task_id -> task
        self.var_to_task: dict[str, str] = {}       # variable name -> task_id
        self.edges: set[tuple[str, str]] = set()
        self.upstream_jobs: set[str] = set()
        self.downstream_jobs: set[str] = set()
        self.outlets: set[str] = set()
        self.warnings: list[str] = []

    # -- DAG discovery -----------------------------------------------------

    def _read_dag(self, call: ast.Call) -> _DagScope | None:
        dag_id = None
        if call.args:
            dag_id = self._lit(call.args[0])
        if dag_id is None:
            dag_id = self._lit(_kwarg(call, "dag_id"))
        if not isinstance(dag_id, str):
            self.warnings.append(
                f"{self._rel()}: found a DAG(...) whose dag_id is not a literal string; skipped"
            )
            return None

        scope = _DagScope(dag_id=dag_id)
        schedule = self._lit(_kwarg(call, *_DAG_KWARG_ALIASES["schedule"]))
        scope.schedule = schedule if isinstance(schedule, str) else None
        scope.inlets = self._uris(_kwarg(call, *_DAG_KWARG_ALIASES["schedule"]))

        description = self._lit(_kwarg(call, "description"))
        scope.description = description if isinstance(description, str) else None

        tags = self._lit(_kwarg(call, "tags"))
        if isinstance(tags, (list, tuple)):
            scope.tags = [str(t) for t in tags]

        default_args = self._lit(_kwarg(call, "default_args"))
        if isinstance(default_args, dict):
            owner = default_args.get("owner")
            scope.owner = str(owner) if owner else None
        return scope

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if _callee_name(node.func) == "DAG":
            scope = self._read_dag(node)
            if scope:
                self.dags.append(scope)
        elif _callee_name(node.func) == "chain":
            self._handle_chain(node)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:  # noqa: N802
        for item in node.items:
            if isinstance(item.context_expr, ast.Call) and _callee_name(item.context_expr.func) == "DAG":
                pass  # already captured by visit_Call
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        for decorator in node.decorator_list:
            name = _callee_name(decorator.func if isinstance(decorator, ast.Call) else decorator)
            if name == "dag":
                dag_id = None
                if isinstance(decorator, ast.Call):
                    if decorator.args:
                        dag_id = self._lit(decorator.args[0])
                    if dag_id is None:
                        dag_id = self._lit(_kwarg(decorator, "dag_id"))
                self.dags.append(_DagScope(dag_id=dag_id if isinstance(dag_id, str) else node.name))
            elif name == "task":
                task_id = node.name
                if isinstance(decorator, ast.Call):
                    explicit = self._lit(_kwarg(decorator, "task_id"))
                    if isinstance(explicit, str):
                        task_id = explicit
                self._register_task(task_id, operator="@task", call=None, var_name=node.name)
        self.generic_visit(node)

    # -- task discovery ----------------------------------------------------

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        if _is_operator_call(node.value):
            assert isinstance(node.value, ast.Call)
            call = node.value
            task_id = self._lit(_kwarg(call, "task_id"))
            if not isinstance(task_id, str):
                self.warnings.append(
                    f"{self._rel()}: {_callee_name(call.func)} has a non-literal task_id; skipped"
                )
            else:
                var_name = node.targets[0].id if isinstance(node.targets[0], ast.Name) else None
                self._register_task(task_id, _callee_name(call.func), call, var_name)
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> None:  # noqa: N802
        # A bare `PythonOperator(task_id=...)` inside a `with DAG(...)` block,
        # or a dependency expression like `a >> b >> c`.
        if _is_operator_call(node.value):
            assert isinstance(node.value, ast.Call)
            task_id = self._lit(_kwarg(node.value, "task_id"))
            if isinstance(task_id, str):
                self._register_task(task_id, _callee_name(node.value.func), node.value, None)
        elif isinstance(node.value, ast.BinOp):
            self._handle_shift(node.value)
        self.generic_visit(node)

    def _register_task(
        self, task_id: str, operator: str, call: ast.Call | None, var_name: str | None
    ) -> None:
        task = ExtractedTask(task_id=task_id, job_id="", operator=operator)
        if call is not None:
            self._attach_code(task, call)
            self._attach_cross_dag(task, call)
            outlets = self._uris(_kwarg(call, "outlets"))
            self.outlets.update(outlets)
        self.tasks[task_id] = task
        if var_name:
            self.var_to_task[var_name] = task_id

    def _attach_code(self, task: ExtractedTask, call: ast.Call) -> None:
        """Resolve whatever code the operator runs, for the table/column layer."""
        sql_node = _kwarg(call, *_SQL_KWARGS)
        sql_value = self._lit(sql_node)
        if isinstance(sql_value, str):
            if sql_value.strip().lower().endswith(".sql"):
                resolved = self._resolve_file(sql_value)
                task.language, task.source_ref = "SQL", sql_value
                if resolved is not None:
                    task.source_code = resolved.read_text(encoding="utf-8", errors="replace")
                    task.source_ref = str(resolved.relative_to(self.root))
                else:
                    task.notes.append(f"referenced SQL file not found in scan root: {sql_value}")
            else:
                task.language, task.source_ref, task.source_code = "SQL", "inline", sql_value
            return
        if isinstance(sql_value, (list, tuple)) and all(isinstance(s, str) for s in sql_value):
            task.language, task.source_ref = "SQL", "inline"
            task.source_code = ";\n".join(s.rstrip().rstrip(";") for s in sql_value) + ";"
            return

        spark_value = self._lit(_kwarg(call, *_SPARK_KWARGS))
        if isinstance(spark_value, str):
            resolved = self._resolve_file(spark_value)
            task.language, task.source_ref = "PYSPARK", spark_value
            if resolved is not None:
                task.source_code = resolved.read_text(encoding="utf-8", errors="replace")
                task.source_ref = str(resolved.relative_to(self.root))
            else:
                task.notes.append(f"referenced Spark application not found in scan root: {spark_value}")
            return

        notebook_value = self._lit(_kwarg(call, *_NOTEBOOK_KWARGS))
        if isinstance(notebook_value, str):
            task.language, task.source_ref = "NOTEBOOK", notebook_value
            return

        bash_value = self._lit(_kwarg(call, *_BASH_KWARGS))
        if isinstance(bash_value, str):
            task.language, task.source_ref, task.source_code = "PYTHON", "inline", bash_value
            return

        if _kwarg(call, "python_callable") is not None:
            task.language, task.source_ref = "PYTHON", "python_callable"

    def _attach_cross_dag(self, task: ExtractedTask, call: ast.Call) -> None:
        external = self._lit(_kwarg(call, "external_dag_id"))
        if isinstance(external, str):
            self.upstream_jobs.add(external)
            task.notes.append(f"waits on external DAG {external}")
        trigger = self._lit(_kwarg(call, "trigger_dag_id"))
        if isinstance(trigger, str):
            self.downstream_jobs.add(trigger)
            task.notes.append(f"triggers DAG {trigger}")

    def _resolve_file(self, reference: str) -> Path | None:
        """Find a referenced .sql/.py file relative to the DAG or the scan root."""
        candidate = (self.path.parent / reference).resolve()
        if candidate.is_file() and self._within_root(candidate):
            return candidate
        name = Path(reference).name
        for match in self.root.rglob(name):
            if match.is_file():
                return match.resolve()
        return None

    def _within_root(self, path: Path) -> bool:
        try:
            path.relative_to(self.root)
            return True
        except ValueError:
            return False

    # -- dependency expressions -------------------------------------------

    def _resolve_operand(self, node: ast.expr) -> list[str]:
        """Task ids a `>>` operand refers to (a name, a list, or a nested shift)."""
        if isinstance(node, ast.Name):
            task_id = self.var_to_task.get(node.id)
            return [task_id] if task_id else []
        if isinstance(node, ast.Call):
            # `extract()` for a TaskFlow function, or an inline operator
            name = _callee_name(node.func)
            if name in self.var_to_task:
                return [self.var_to_task[name]]
            task_id = self._lit(_kwarg(node, "task_id"))
            return [task_id] if isinstance(task_id, str) else []
        if isinstance(node, (ast.List, ast.Tuple)):
            resolved: list[str] = []
            for element in node.elts:
                resolved.extend(self._resolve_operand(element))
            return resolved
        return []

    def _handle_shift(self, node: ast.BinOp) -> tuple[list[str], list[str]]:
        """Record edges for a `>>`/`<<` chain; returns (head ids, tail ids)."""
        if not isinstance(node.op, (ast.RShift, ast.LShift)):
            return [], []

        def side(expr: ast.expr) -> tuple[list[str], list[str]]:
            if isinstance(expr, ast.BinOp) and isinstance(expr.op, (ast.RShift, ast.LShift)):
                return self._handle_shift(expr)
            ids = self._resolve_operand(expr)
            return ids, ids

        left_head, left_tail = side(node.left)
        right_head, right_tail = side(node.right)

        if isinstance(node.op, ast.RShift):
            for upstream in left_tail:
                for downstream in right_head:
                    self.edges.add((upstream, downstream))
        else:
            for downstream in left_head:
                for upstream in right_tail:
                    self.edges.add((upstream, downstream))
        return left_head, right_tail

    def _handle_chain(self, call: ast.Call) -> None:
        groups = [self._resolve_operand(arg) for arg in call.args]
        for upstream_group, downstream_group in zip(groups, groups[1:]):
            for upstream in upstream_group:
                for downstream in downstream_group:
                    self.edges.add((upstream, downstream))

    def visit_Call_setters(self, node: ast.Call) -> None:  # pragma: no cover - see _scan_setters
        pass

    def _lit(self, node: ast.expr | None):
        return _literal(node, self.constants)

    def _uris(self, node: ast.expr | None) -> list[str]:
        return _dataset_uris(node, self.constants)

    def _rel(self) -> str:
        try:
            return str(self.path.relative_to(self.root))
        except ValueError:
            return str(self.path)


def _scan_setters(tree: ast.AST, visitor: _DagFileVisitor) -> None:
    """`a.set_downstream(b)` / `a.set_upstream(b)` edges."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method = node.func.attr
        if method not in {"set_downstream", "set_upstream"} or not node.args:
            continue
        anchors = visitor._resolve_operand(node.func.value)
        others = visitor._resolve_operand(node.args[0])
        for anchor in anchors:
            for other in others:
                if method == "set_downstream":
                    visitor.edges.add((anchor, other))
                else:
                    visitor.edges.add((other, anchor))


def looks_like_airflow(source: str) -> bool:
    """Cheap pre-filter so we only AST-parse files that could be DAGs."""
    return "airflow" in source and ("DAG(" in source or "@dag" in source or "@task" in source)


def parse_airflow_dag(path: Path, root: Path) -> tuple[list[ExtractedJob], list[str]]:
    """Extract every DAG defined in one Python file."""
    source = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [], [f"{path}: could not parse as Python ({exc.msg})"]

    visitor = _DagFileVisitor(path, root, module_constants(tree))
    visitor.visit(tree)
    _scan_setters(tree, visitor)

    if not visitor.dags:
        return [], visitor.warnings

    # A file normally defines one DAG; if it defines several we cannot tell
    # statically which tasks belong to which, so tasks go to the first and
    # that ambiguity is reported rather than guessed.
    primary = visitor.dags[0]
    if len(visitor.dags) > 1:
        visitor.warnings.append(
            f"{visitor._rel()}: defines {len(visitor.dags)} DAGs; tasks attributed to '{primary.dag_id}'"
        )

    for task in visitor.tasks.values():
        task.job_id = primary.dag_id
    for upstream, downstream in sorted(visitor.edges):
        if downstream in visitor.tasks and upstream in visitor.tasks:
            visitor.tasks[downstream].upstream_task_ids.append(upstream)

    jobs = [
        ExtractedJob(
            job_id=primary.dag_id,
            system="airflow",
            source_file=visitor._rel(),
            schedule=primary.schedule,
            owner=primary.owner,
            description=primary.description,
            tags=primary.tags,
            tasks=sorted(visitor.tasks.values(), key=lambda t: t.task_id),
            upstream_job_ids=sorted(visitor.upstream_jobs),
            downstream_job_ids=sorted(visitor.downstream_jobs),
            inlet_datasets=sorted(set(primary.inlets)),
            outlet_datasets=sorted(visitor.outlets),
        )
    ]
    for extra in visitor.dags[1:]:
        jobs.append(
            ExtractedJob(
                job_id=extra.dag_id,
                system="airflow",
                source_file=visitor._rel(),
                schedule=extra.schedule,
                owner=extra.owner,
                description=extra.description,
                tags=extra.tags,
            )
        )
    return jobs, visitor.warnings
