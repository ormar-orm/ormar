"""
Turns SQLAlchemy statements into SQL text plus positional arguments that a
native async driver can run directly, skipping SQLAlchemy's execution layer.

SQLAlchemy still builds and compiles every statement; this module reuses its
compiled output. A compiled statement holds its SQL with the driver's
placeholders and the names of its bind parameters in placeholder order, so
running it natively means looking each value up by name in that order::

    stmt = select(users.c.id, users.c.name).where(users.c.id == 5)
    # compiled for asyncpg
    sql == "SELECT users.id, users.name FROM users WHERE users.id = $1::INTEGER"
    args == [5]  # value of the bind parameter named "id_1"

Compiling is the expensive part, so compiled statements are cached per
statement shape and only the values are bound on each call.
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
from sqlalchemy.sql.elements import BindParameter

ResultProcessor = Optional[Callable[[Any], Any]]


class StatementPlan:
    """
    Everything needed to run one statement shape natively, worked out once
    when the shape is first compiled and reused by every later execution.

    ``select(users.c.id).where(users.c.id == 5)`` and the same query with ``7``
    have one shape, so they share one plan; only their bound values differ.

    It holds the compiled statement, the result labels and types used to build
    rows, and ``fallback``: True for statements that must run through
    SQLAlchemy instead (plain text without result columns, duplicate result
    labels, or column defaults that have to run in the database first).
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
    A result row of native execution: a dict keyed by result label, which is
    how ormar reads rows.

    It also accepts the selected column objects as keys, like SQLAlchemy's
    ``RowMapping`` does, so code indexing rows that way keeps working::

        row = NativeRow({"id": 1, "name": "a"})
        row["name"] == row[users.c.name] == "a"

    Each plan builds its own subclass (``row_class_for``) whose
    ``object_keys`` maps the selected column objects to their labels.
    """

    object_keys: Dict[int, str] = {}

    def __missing__(self, key: Any) -> Any:
        """
        Called by dict lookup when the key is not a label: treat the key as a
        selected column object (``row[users.c.name]``) and return its value.

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
    Build the ``NativeRow`` subclass of one plan, so its rows can be indexed by
    the selected column objects as well as by label.

    Column objects are matched by ``id()``, because ``==`` on a column builds a
    SQL expression instead of comparing. Each entry of SQLAlchemy's
    ``_result_columns`` lists the objects a result column was selected as::

        ResultColumnsEntry(keyname="name", objects=(Column("name", ...), ...))
        # -> object_keys == {id(users.c.name): "name", ...}

    :param result_columns: SQLAlchemy ``_result_columns`` of the statement
    :type result_columns: Sequence[Any]
    :return: ``NativeRow`` subclass for the plan
    :rtype: type
    """
    object_keys = {id(obj): column[0] for column in result_columns for obj in column[2]}
    return type("NativeRow", (NativeRow,), {"object_keys": object_keys})


class BoundStatement(NamedTuple):
    """
    One statement ready to send to the driver: the plan of its shape, the
    final SQL, the positional arguments, and the named parameters they came
    from (kept so a primary key the caller bound can be returned as is)::

        sql == "SELECT users.id FROM users WHERE users.id IN ($1, $2, $3)"
        args == [1, 2, 3]
        params == {"id_1": [1, 2, 3]}
    """

    plan: StatementPlan
    sql: str
    args: List[Any]
    params: MutableMapping[str, Any]


class BoundBatch(NamedTuple):
    """
    One statement ready to run once per row with the driver's
    ``executemany``: the shared SQL and one argument list per row::

        sql == "INSERT INTO users (name) VALUES ($1)"
        args_list == [["a"], ["b"]]
    """

    plan: StatementPlan
    sql: str
    args_list: List[List[Any]]


class DefaultContext:
    """
    Stand-in for SQLAlchemy's execution context, passed to Python column
    defaults that take a context argument.

    SQLAlchemy calls such defaults with its execution context; the native path
    has none, so this offers what those defaults use, the statement's current
    parameters and the column::

        def slug_default(context):
            return context.get_current_parameters()["name"].lower()


        Column("slug", String, default=slug_default)
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
    Turns statements into ``BoundStatement`` / ``BoundBatch`` objects for one
    database dialect.

    Compiling is the costly step, so plans are kept in an LRU cache keyed by
    SQLAlchemy's statement cache key, which is equal for all statements of one
    shape whatever their values. On a hit only the new values are bound::

        compiler.bind(select(users).where(users.c.id == 5))  # compiles
        compiler.bind(select(users).where(users.c.id == 7))  # reuses the plan
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

    def bind(
        self, stmt: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> BoundStatement:
        """
        Compile the statement, or reuse the plan of its shape, and bind its values
        into SQL and positional driver arguments.

        The values come from the statement's own cache key, which carries the
        bound values of this particular statement::

            bind(select(users.c.id).where(users.c.id == 7))
            # -> sql "... WHERE users.id = $1::INTEGER", args [7]

        ``params`` supplies values by bind name instead. ormar's cached query
        templates use it: one statement built once with named slots, and only
        the values passed on each call::

            stmt = select(users.c.id).where(users.c.id == bindparam("ormar_p0"))
            bind(stmt, {"ormar_p0": 7})  # -> args [7]

        When ``binds_directly`` allows it, those values go to the driver
        without SQLAlchemy's binding steps (see ``direct_args``).

        :param stmt: statement to execute
        :type stmt: Executable
        :param params: values for named bind parameters, by name
        :type params: Optional[Mapping[str, Any]]
        :return: plan, SQL string and positional driver arguments
        :rtype: BoundStatement
        """
        key = stmt._generate_cache_key()  # type: ignore[attr-defined]
        plan = self._plan(stmt, key)
        if params is not None and binds_directly(plan.compiled, params):
            args = direct_args(plan, [params])[0]
            return BoundStatement(plan, plan.compiled.string, args, dict(params))
        bound_params = plan.compiled.construct_params(
            params=params,
            extracted_parameters=key[1] if key is not None else None,
            escape_names=True,
            _no_postcompile=True,
        )
        apply_prefetch_defaults(plan.compiled, bound_params)
        sql, args = expand(plan.compiled, bound_params)
        return BoundStatement(plan, sql, args, bound_params)

    def bind_many(
        self, stmt: Executable, rows: Sequence[Mapping[str, Any]]
    ) -> BoundBatch:
        """
        Compile a statement with named ``bindparam`` placeholders, or reuse its
        plan, and bind one set of values per row for ``executemany``::

            bind_many(users.insert(), [{"name": "a"}, {"name": "b"}])
            # -> sql "INSERT INTO users (name) VALUES ($1::VARCHAR)",
            #    args_list [["a"], ["b"]]

        Like ``bind``, rows that ``binds_directly`` accepts skip SQLAlchemy's
        binding steps.

        :param stmt: statement with ``bindparam`` placeholders
        :type stmt: Executable
        :param rows: parameter values per row, keyed by bind parameter name
        :type rows: Sequence[Mapping[str, Any]]
        :return: plan, SQL string and positional driver arguments per row
        :rtype: BoundBatch
        """
        key = stmt._generate_cache_key()  # type: ignore[attr-defined]
        plan = self._plan(stmt, key)
        if rows and binds_directly(plan.compiled, rows[0]):
            return BoundBatch(plan, plan.compiled.string, direct_args(plan, rows))
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
        Statements SQLAlchemy cannot cache (no cache key) are compiled every time;
        past ``max_plans`` the least recently used plan is dropped.

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
        Compile the statement for the dialect and derive its plan. Passing the
        cache key makes SQLAlchemy record which bind parameters come from the key,
        so later statements of the same shape can supply their own values.

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
    Return whether the statement can go through the native compiler at all.

    DDL (``CREATE TABLE`` and similar) is compiled by a different compiler
    without cache keys, and raw SQL strings have nothing to compile, so both
    always run through SQLAlchemy.

    :param stmt: statement to execute
    :type stmt: Any
    :return: False for DDL statements and raw SQL strings
    :rtype: TypeGuard[Executable]
    """
    return not isinstance(stmt, (ExecutableDDLElement, str))


def prefetch_defaults(compiled: SQLCompiler) -> List[Any]:
    """
    Return the column defaults SQLAlchemy fills in at execution time because
    the statement does not set those columns itself.

    SQLAlchemy calls these columns "prefetch": ``insert_prefetch`` for
    ``default=`` on INSERT and ``update_prefetch`` for ``onupdate=`` on
    UPDATE::

        Column("token", String, default=lambda: "tok")
        users.insert().values(name="a")  # -> [CallableColumnDefault(<lambda>)]

    ``StatementPlan`` falls back to SQLAlchemy when one of them is neither a
    constant nor a Python callable (a SQL expression run before the statement).

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
    Fill the values of execution-time column defaults into the bound
    parameters, which SQLAlchemy's execution context does on its own path.

    Constants are copied, callables are called (with a ``DefaultContext``)::

        Column("token", String, default=lambda: "tok")
        users.insert().values(name="a")
        # params {"name": "a"} -> {"name": "a", "token": "tok"}

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
    Return the parameters keyed by the original bind names, as SQLAlchemy
    shows them to column defaults reading ``get_current_parameters()``.

    SQLAlchemy escapes bind names with characters a driver cannot take in a
    parameter name, and bound parameters use the escaped form::

        Column("my col", Integer)
        {"my_col": 3}  # -> {"my col": 3}

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
    Produce the final SQL and positional driver arguments from named
    parameters.

    SQLAlchemy lists the bind names in placeholder order (``positiontup``), so
    the arguments are the values looked up in that order. Each value goes
    through its type's bind processor when it has one, the reverse of a result
    processor (e.g. a dict to a JSON string on SQLite). ``IN`` lists get one
    placeholder per value::

        {"id_1": [1, 2, 3]}
        # -> "... WHERE users.id IN ($1::INTEGER, $2::INTEGER, $3::INTEGER)",
        #    [1, 2, 3]

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


def binds_directly(compiled: SQLCompiler, row: Mapping[str, Any]) -> bool:
    """
    Return whether values given by bind name can go to the driver as they
    are, skipping SQLAlchemy's ``construct_params`` and ``expand``.

    Those two steps cost more than the query itself for small statements, and
    for most of ormar's cached templates they change nothing: the compiled SQL
    is final and the arguments are the row's values in placeholder order. It
    is not safe when SQLAlchemy still has to change the SQL or the values:

    - ``IN`` lists and other post-compile parameters (placeholders added per
      value) and literal-execute parameters (value rendered into the SQL)
    - column defaults SQLAlchemy fills in at execution time
    - escaped bind names (the row is keyed by the original name)
    - a bind the row does not supply::

        stmt = select(users.c.id).where(users.c.id == bindparam("ormar_p0"))
        binds_directly(compiled, {"ormar_p0": 7})  # -> True
        stmt = select(users.c.id).where(users.c.id.in_(bindparam("ormar_p0")))
        binds_directly(compiled, {"ormar_p0": [1, 2]})  # -> False

    :param compiled: compiled statement
    :type compiled: SQLCompiler
    :param row: first row of the batch, keyed by bind parameter name
    :type row: Mapping[str, Any]
    :return: True when ``direct_args`` produces the driver arguments
    :rtype: bool
    """
    names = compiled.positiontup
    return (
        names is not None
        and not compiled.post_compile_params
        and not compiled.literal_execute_params
        and not compiled.insert_prefetch
        and not compiled.update_prefetch
        and not compiled.escaped_bind_names
        and all(name in row for name in names)
    )


def direct_args(
    plan: StatementPlan, rows: Sequence[Mapping[str, Any]]
) -> List[List[Any]]:
    """
    Build the positional driver arguments of each row straight from its
    values, for statements ``binds_directly`` accepts. Values are taken in
    placeholder order and converted by their bind processors, which gives the
    same arguments as ``construct_params`` plus ``expand``::

        # positiontup ["name", "id_1"], no processors
        direct_args(plan, [{"id_1": 1, "name": "a"}])  # -> [["a", 1]]

    :param plan: plan of the statement
    :type plan: StatementPlan
    :param rows: parameter values per row, keyed by bind parameter name
    :type rows: Sequence[Mapping[str, Any]]
    :return: positional arguments per row
    :rtype: List[List[Any]]
    """
    compiled = plan.compiled
    processors: Dict[str, Any] = dict(compiled._bind_processors)
    binds = [(name, processors.get(name)) for name in compiled.positiontup or []]
    return [
        [
            processor(row[name]) if processor is not None else row[name]
            for name, processor in binds
        ]
        for row in rows
    ]


def pk_param_key(compiled: SQLCompiler) -> Optional[str]:
    """
    Return the parameter name the INSERT binds the primary key under, or None
    when the INSERT does not set it.

    A primary key provided by the caller is returned as the inserted key
    instead of asking the database, which cannot always report it::

        users.insert().values(id=3, name="a")  # -> "id"
        users.insert().values(name="a")  # -> None

    ormar's cached INSERT templates bind the primary key under their own
    parameter name, which is returned instead of the column key::

        users.insert().values(id=bindparam("ormar_id"))  # -> "ormar_id"

    :param compiled: compiled insert statement
    :type compiled: SQLCompiler
    :return: bind parameter key of the primary key, or None
    :rtype: Optional[str]
    """
    table = compiled.statement.table  # type: ignore[union-attr]
    primary_key = list(table.primary_key.columns)
    if len(primary_key) != 1:  # pragma: no cover
        return None
    values = getattr(compiled.statement, "_values", None) or {}
    for column, value in values.items():
        column_key = column if isinstance(column, str) else column.key
        named_bind = isinstance(value, BindParameter) and not value.unique
        if column_key == primary_key[0].key and named_bind:
            return str(value.key)
    key = compiled._within_exec_param_key_getter(primary_key[0])
    return key if key in compiled.binds else None


def resolve_result_processors(
    plan: StatementPlan, dialect: Dialect, coltypes: Sequence[Any]
) -> None:
    """
    Look up, once per plan, the result processor of each result column.

    A result processor is the function a SQLAlchemy type uses to turn the raw
    value a driver returns into the Python value of that type. Drivers return
    what the database stores, which is not always what the column type
    promises, e.g. on SQLite::

        DateTime  "2024-01-02 03:04:05" -> datetime(2024, 1, 2, 3, 4, 5)
        JSON      '{"a": 1}'            -> {"a": 1}
        Enum(C)   "red"                 -> C.red
        Boolean   1                     -> True
        Integer   needs none            -> None

    SQLAlchemy runs them on its own path; the native path must run the same
    ones, or rows would hold driver values. Some types pick their processor by
    the column type the driver reports (a PostgreSQL ``NUMERIC`` read into a
    ``Numeric(asdecimal=False)`` column needs a float conversion), so this runs
    on the first execution, when the driver has reported them.

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
    Convert one driver row into a ``NativeRow`` keyed by result label, running
    each value through its result processor::

        (1, '{"a": 1}')  # -> {"id": 1, "data": {"a": 1}} on SQLite

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
