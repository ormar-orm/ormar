"""
Compiles SQLAlchemy statements once per shape and binds their values for
execution directly on a native async driver.
"""

from collections import OrderedDict
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeGuard,
)

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import Executable
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.ddl import ExecutableDDLElement

ResultProcessor = Optional[Callable[[Any], Any]]


class StatementPlan:
    """
    Data derived once per compiled statement shape and reused by every
    execution of that shape.
    """

    def __init__(self, compiled: SQLCompiler) -> None:
        """
        Build the plan from a statement compiled with its cache key.

        :param compiled: statement compiled for the target dialect
        :type compiled: SQLCompiler
        """
        self.compiled = compiled
        result_columns = compiled._result_columns or []
        self.result_keys: Tuple[str, ...] = tuple(col[0] for col in result_columns)
        self.result_types: Tuple[Any, ...] = tuple(col[3] for col in result_columns)
        self.result_processors: Optional[List[ResultProcessor]] = None
        self.row_class = row_class_for(result_columns)
        self.returning = bool(compiled.effective_returning)
        self.is_insert = bool(compiled.isinsert)
        self.pk_param_key: Optional[str] = None
        # Native execution needs to know the shape of what it runs: DML, or a
        # statement with known result columns. Plain text and DDL go through
        # SQLAlchemy, as do defaults SQLAlchemy must pre-execute.
        is_dml = compiled.isinsert or compiled.isupdate or compiled.isdelete
        duplicate_labels = len(set(self.result_keys)) != len(self.result_keys)
        self.fallback = (
            not (is_dml or self.result_keys)
            or duplicate_labels
            or any(
                not (default.is_scalar or default.is_callable)
                for default in prefetch_defaults(compiled)
            )
        )


class NativeRow(dict):
    """
    A result row keyed by result label that, like SQLAlchemy's ``RowMapping``,
    can also be indexed by the selected column objects.
    """

    object_keys: Dict[int, str] = {}

    def __missing__(self, key: Any) -> Any:
        """
        Resolve a column object to its result label.

        :param key: column object (or unknown key)
        :type key: Any
        :raises KeyError: when the key is not a selected column
        :return: value of the matching result column
        :rtype: Any
        """
        label = self.object_keys.get(id(key))
        if label is None:
            raise KeyError(key)
        return self[label]


def row_class_for(result_columns: Sequence[Any]) -> type:
    """
    Build the row class of a plan, mapping each selected column object (by
    identity, as column ``==`` builds SQL expressions) to its result label.

    :param result_columns: SQLAlchemy ``_result_columns`` of the statement
    :type result_columns: Sequence[Any]
    :return: ``NativeRow`` subclass for the plan
    :rtype: type
    """
    object_keys = {id(obj): column[0] for column in result_columns for obj in column[2]}
    return type("NativeRow", (NativeRow,), {"object_keys": object_keys})


class BoundStatement(NamedTuple):
    """A statement plan with the SQL and positional driver arguments of one call."""

    plan: StatementPlan
    sql: str
    args: List[Any]
    params: MutableMapping[str, Any]


class BoundBatch(NamedTuple):
    """A statement plan with one list of positional driver arguments per row."""

    plan: StatementPlan
    sql: str
    args_list: List[List[Any]]


class DefaultContext:
    """
    Minimal stand-in for SQLAlchemy's execution context, passed to callable
    column defaults.
    """

    def __init__(self, params: MutableMapping[str, Any], column: Any) -> None:
        """
        :param params: bound parameters of the statement being executed
        :type params: MutableMapping[str, Any]
        :param column: column whose default is being computed
        :type column: Any
        """
        self.current_parameters = params
        self.current_column = column

    def get_current_parameters(
        self, isolate_multiinsert_groups: bool = True
    ) -> MutableMapping[str, Any]:
        """
        Return the parameters of the statement being executed, like
        SQLAlchemy's ``DefaultExecutionContext.get_current_parameters``.

        :param isolate_multiinsert_groups: kept for signature compatibility
        :type isolate_multiinsert_groups: bool
        :return: bound parameters keyed by column key
        :rtype: MutableMapping[str, Any]
        """
        return self.current_parameters


class NativeCompiler:
    """
    Compiles statements for one dialect, caching compiled plans by
    SQLAlchemy's statement cache key.
    """

    def __init__(self, dialect: Dialect, max_plans: int = 2048) -> None:
        """
        :param dialect: dialect of the database the statements run on
        :type dialect: Dialect
        :param max_plans: number of compiled statement shapes kept in memory
        :type max_plans: int
        """
        self.dialect = dialect
        self.max_plans = max_plans
        self._plans: "OrderedDict[Any, StatementPlan]" = OrderedDict()

    def bind(self, stmt: Executable) -> BoundStatement:
        """
        Compile (or reuse) the statement and bind its values.

        :param stmt: statement to execute
        :type stmt: Executable
        :return: plan, SQL string and positional driver arguments
        :rtype: BoundStatement
        """
        key = stmt._generate_cache_key()  # type: ignore[attr-defined]
        plan = self._plan(stmt, key)
        params = plan.compiled.construct_params(
            extracted_parameters=key[1] if key is not None else None,
            escape_names=True,
            _no_postcompile=True,
        )
        apply_prefetch_defaults(plan.compiled, params)
        sql, args = expand(plan.compiled, params)
        return BoundStatement(plan, sql, args, params)

    def bind_many(
        self, stmt: Executable, rows: Sequence[Mapping[str, Any]]
    ) -> BoundBatch:
        """
        Compile (or reuse) a statement with named bind parameters and bind one
        set of values per row.

        :param stmt: statement with ``bindparam`` placeholders
        :type stmt: Executable
        :param rows: parameter values per row, keyed by bind parameter name
        :type rows: Sequence[Mapping[str, Any]]
        :return: plan, SQL string and positional driver arguments per row
        :rtype: BoundBatch
        """
        key = stmt._generate_cache_key()  # type: ignore[attr-defined]
        plan = self._plan(stmt, key)
        sql = ""
        args_list = []
        for row in rows:
            params = plan.compiled.construct_params(
                params=row,
                extracted_parameters=key[1] if key is not None else None,
                escape_names=True,
                _no_postcompile=True,
            )
            apply_prefetch_defaults(plan.compiled, params)
            sql, args = expand(plan.compiled, params)
            args_list.append(args)
        return BoundBatch(plan, sql, args_list)

    def _plan(self, stmt: Executable, key: Any) -> StatementPlan:
        """
        Return the cached plan for the statement shape, compiling it on a miss.

        :param stmt: statement to plan
        :type stmt: Executable
        :param key: SQLAlchemy cache key of the statement, or None if uncacheable
        :type key: Any
        :return: plan for the statement shape
        :rtype: StatementPlan
        """
        if key is None:
            return self._compile(stmt, key)
        plan = self._plans.get(key.key)
        if plan is not None:
            self._plans.move_to_end(key.key)
            return plan
        plan = self._compile(stmt, key)
        self._plans[key.key] = plan
        if len(self._plans) > self.max_plans:
            self._plans.popitem(last=False)
        return plan

    def _compile(self, stmt: Executable, key: Any) -> StatementPlan:
        """
        Compile the statement with its cache key and derive the plan.

        :param stmt: statement to compile
        :type stmt: Executable
        :param key: SQLAlchemy cache key of the statement, or None
        :type key: Any
        :return: plan for the statement shape
        :rtype: StatementPlan
        """
        compiled = stmt._compiler(  # type: ignore[attr-defined]
            self.dialect, cache_key=key
        )
        plan = StatementPlan(compiled)
        if plan.is_insert:
            plan.pk_param_key = pk_param_key(compiled)
        return plan


def is_plannable(stmt: Any) -> TypeGuard[Executable]:
    """
    Return whether the statement can be planned for native execution at all;
    DDL is compiled by a different compiler and always runs through SQLAlchemy.

    :param stmt: statement to execute
    :type stmt: Any
    :return: False for DDL statements and raw SQL strings
    :rtype: TypeGuard[Executable]
    """
    return not isinstance(stmt, (ExecutableDDLElement, str))


def prefetch_defaults(compiled: SQLCompiler) -> List[Any]:
    """
    Return the Python-side defaults SQLAlchemy computes at execution time.

    :param compiled: compiled statement
    :type compiled: SQLCompiler
    :return: column defaults (or onupdates) of prefetched columns
    :rtype: List[Any]
    """
    defaults = [col.default for col in compiled.insert_prefetch]
    defaults.extend(col.onupdate for col in compiled.update_prefetch)
    return [default for default in defaults if default is not None]


def apply_prefetch_defaults(
    compiled: SQLCompiler, params: MutableMapping[str, Any]
) -> None:
    """
    Fill execution-time Python defaults into the bound parameters, the way
    SQLAlchemy's execution context does.

    :param compiled: compiled statement
    :type compiled: SQLCompiler
    :param params: bound parameters, updated in place
    :type params: MutableMapping[str, Any]
    """
    if not (compiled.insert_prefetch or compiled.update_prefetch):
        return
    key_getter = compiled._within_exec_param_key_getter
    for columns, attribute in (
        (compiled.insert_prefetch, "default"),
        (compiled.update_prefetch, "onupdate"),
    ):
        for column in columns:
            default = getattr(column, attribute)
            if default.is_scalar:
                params[key_getter(column)] = default.arg
            elif default.is_callable:
                context = DefaultContext(unescaped_view(compiled, params), column)
                params[key_getter(column)] = default.arg(context)


def unescaped_view(
    compiled: SQLCompiler, params: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """
    Return the parameters keyed by unescaped bind names, as SQLAlchemy shows
    them to context-sensitive column defaults.

    :param compiled: compiled statement
    :type compiled: SQLCompiler
    :param params: parameters keyed by escaped bind names
    :type params: MutableMapping[str, Any]
    :return: parameters keyed by original bind names
    :rtype: MutableMapping[str, Any]
    """
    escaped = compiled.escaped_bind_names
    if not escaped:
        return params
    original = {value: key for key, value in escaped.items()}
    return {original.get(key, key): value for key, value in params.items()}


def expand(
    compiled: SQLCompiler, params: MutableMapping[str, Any]
) -> Tuple[str, List[Any]]:
    """
    Expand ``IN`` lists and apply bind processors, producing the final SQL and
    positional driver arguments.

    :param compiled: compiled statement
    :type compiled: SQLCompiler
    :param params: bound parameters
    :type params: MutableMapping[str, Any]
    :return: SQL string and positional arguments
    :rtype: Tuple[str, List[Any]]
    """
    expanded = compiled.construct_expanded_state(params, escape_names=False)
    processors: Dict[str, Any] = {
        **compiled._bind_processors,
        **expanded.processors,
    }
    values = expanded.parameters
    args = [
        processors[name](values[name]) if name in processors else values[name]
        for name in expanded.positiontup or []
    ]
    return expanded.statement, args


def pk_param_key(compiled: SQLCompiler) -> Optional[str]:
    """
    Return the bind key of the table's primary key if the insert binds it.

    :param compiled: compiled insert statement
    :type compiled: SQLCompiler
    :return: bind parameter key of the primary key, or None
    :rtype: Optional[str]
    """
    table = compiled.statement.table  # type: ignore[union-attr]
    primary_key = list(table.primary_key.columns)
    if len(primary_key) != 1:  # pragma: no cover
        return None
    key = compiled._within_exec_param_key_getter(primary_key[0])
    return key if key in compiled.binds else None


def resolve_result_processors(
    plan: StatementPlan, dialect: Dialect, coltypes: Sequence[Any]
) -> None:
    """
    Resolve SQLAlchemy's result processors for the plan's result columns.

    :param plan: plan to resolve processors for
    :type plan: StatementPlan
    :param dialect: dialect of the database
    :type dialect: Dialect
    :param coltypes: driver-reported column types (or None per column)
    :type coltypes: Sequence[Any]
    """
    plan.result_processors = [
        type_._cached_result_processor(dialect, coltype)
        for type_, coltype in zip(plan.result_types, coltypes)
    ]


def row_to_dict(plan: StatementPlan, values: Sequence[Any]) -> Dict[str, Any]:
    """
    Convert one driver row into a dict keyed by result label, applying result
    processors.

    :param plan: plan of the executed statement, with resolved processors
    :type plan: StatementPlan
    :param values: row values in result column order
    :type values: Sequence[Any]
    :return: row keyed by result label
    :rtype: Dict[str, Any]
    """
    processors = plan.result_processors or [None] * len(plan.result_keys)
    return plan.row_class(  # type: ignore[no-any-return]
        {
            key: processor(value) if processor is not None else value
            for key, processor, value in zip(plan.result_keys, processors, values)
        }
    )
