from datetime import datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mysql.asyncmy import MySQLDialect_asyncmy
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.sqlite.aiosqlite import SQLiteDialect_aiosqlite

from ormar.databases.native.compiler import (
    NativeCompiler,
    resolve_result_processors,
    row_to_dict,
)

metadata = sa.MetaData()
counter = {"calls": 0}


def counting_default() -> str:
    counter["calls"] += 1
    return f"tag-{counter['calls']}"


items = sa.Table(
    "compiler_items",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.String(50)),
    sa.Column("tag", sa.String(50), default=counting_default),
    sa.Column("kind", sa.String(10), default="plain"),
    sa.Column("price", sa.Numeric(10, 2)),
    sa.Column("meta", sa.JSON),
    sa.Column("created", sa.DateTime),
)
stamped = sa.Table(
    "compiler_stamped",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("at", sa.DateTime, default=sa.func.now()),
)
generated_pk = sa.Table(
    "compiler_generated_pk",
    metadata,
    sa.Column("id", sa.String(36), primary_key=True, default=sa.func.uuid()),
    sa.Column("x", sa.Integer),
)

PLACEHOLDERS = {
    PGDialect_asyncpg: "$1",
    MySQLDialect_asyncmy: "%s",
    SQLiteDialect_aiosqlite: "?",
}
DIALECTS = list(PLACEHOLDERS)


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_select_placeholders(dialect_cls):
    bound = NativeCompiler(dialect_cls()).bind(items.select().where(items.c.id == 5))
    assert PLACEHOLDERS[dialect_cls] in bound.sql
    assert bound.args == [5]


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_reuses_plan_for_same_shape(dialect_cls):
    compiler = NativeCompiler(dialect_cls())
    first = compiler.bind(items.select().where(items.c.id == 5))
    second = compiler.bind(items.select().where(items.c.id == 6))
    assert first.plan is second.plan
    assert (first.args, second.args) == ([5], [6])


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_expands_in_lists_with_one_plan(dialect_cls):
    compiler = NativeCompiler(dialect_cls())
    two = compiler.bind(items.select().where(items.c.id.in_([1, 2])))
    three = compiler.bind(items.select().where(items.c.id.in_([1, 2, 3])))
    assert two.plan is three.plan
    assert (two.args, three.args) == ([1, 2], [1, 2, 3])


def test_bind_applies_bind_processors():
    dialect = SQLiteDialect_aiosqlite()
    stmt = (
        items.update()
        .where(items.c.id == 1)
        .values(price=Decimal("1.50"), meta={"a": 1})
    )
    key = stmt._generate_cache_key()
    compiled = stmt._compiler(dialect, cache_key=key)
    params = compiled.construct_params(extracted_parameters=key[1])
    expected = [
        compiled._bind_processors[name](params[name])
        if name in compiled._bind_processors
        else params[name]
        for name in compiled.positiontup
    ]
    bound = NativeCompiler(dialect).bind(stmt)
    assert bound.args == expected
    assert '{"a": 1}' in bound.args


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_insert_calls_python_default_once(dialect_cls):
    before = counter["calls"]
    bound = NativeCompiler(dialect_cls()).bind(items.insert().values(name="a"))
    assert counter["calls"] == before + 1
    assert f"tag-{before + 1}" in bound.args


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_insert_scalar_default(dialect_cls):
    bound = NativeCompiler(dialect_cls()).bind(items.insert().values(name="a"))
    assert "plain" in bound.args


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_inline_clause_default_runs_natively(dialect_cls):
    bound = NativeCompiler(dialect_cls()).bind(stamped.insert().values(id=1))
    assert bound.plan.fallback is False
    assert bound.args == [1]


def test_pre_executed_clause_default_marks_fallback():
    compiler = NativeCompiler(MySQLDialect_asyncmy())
    assert compiler.bind(generated_pk.insert().values(x=1)).plan.fallback is True


def test_insert_plan_flags():
    compiler = NativeCompiler(PGDialect_asyncpg())
    generated = compiler.bind(items.insert().values(name="a"))
    assert generated.plan.is_insert
    assert generated.plan.returning
    assert generated.plan.pk_param_key is None
    explicit = compiler.bind(items.insert().values(id=7, name="a"))
    assert explicit.plan.pk_param_key == "id"
    assert explicit.params["id"] == 7
    select = compiler.bind(items.select())
    assert not select.plan.is_insert
    assert select.plan.pk_param_key is None


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_many_rows(dialect_cls):
    stmt = (
        items.update()
        .where(items.c.id == sa.bindparam("new_id"))
        .values(name=sa.bindparam("new_name"))
    )
    rows = [{"new_id": i, "new_name": f"n{i}"} for i in range(3)]
    batch = NativeCompiler(dialect_cls()).bind_many(stmt, rows)
    assert batch.args_list == [["n0", 0], ["n1", 1], ["n2", 2]]
    assert PLACEHOLDERS[dialect_cls] in batch.sql


def test_plan_cache_is_bounded():
    compiler = NativeCompiler(SQLiteDialect_aiosqlite(), max_plans=2)
    first = compiler.bind(items.select().where(items.c.id == 1)).plan
    compiler.bind(items.select().where(items.c.name == "a"))
    compiler.bind(items.select().where(items.c.kind == "b"))
    assert compiler.bind(items.select().where(items.c.id == 1)).plan is not first


class UncachedType(sa.types.TypeDecorator):
    impl = sa.String
    cache_ok = None

    def process_bind_param(self, value, dialect):
        return f"<{value}>"


uncached = sa.Table(
    "compiler_uncached",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("label", UncachedType(20)),
)


@pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")
def test_uncacheable_statement_compiles_every_time():
    compiler = NativeCompiler(SQLiteDialect_aiosqlite())
    first = compiler.bind(uncached.select().where(uncached.c.label == "a"))
    second = compiler.bind(uncached.select().where(uncached.c.label == "b"))
    assert first.plan is not second.plan
    assert (first.args, second.args) == (["<a>"], ["<b>"])


def test_row_to_dict_applies_result_processors():
    dialect = SQLiteDialect_aiosqlite()
    bound = NativeCompiler(dialect).bind(
        sa.select(items.c.id, items.c.created).where(items.c.id == 1)
    )
    resolve_result_processors(bound.plan, dialect, [None, None])
    row = row_to_dict(bound.plan, (1, "2026-10-19 12:30:00.000000"))
    assert row == {"id": 1, "created": datetime(2026, 10, 19, 12, 30)}


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_statements_without_result_columns_fall_back(dialect_cls):
    compiler = NativeCompiler(dialect_cls())
    assert compiler.bind(sa.text("SELECT 1 AS x")).plan.fallback is True
    typed = sa.text("SELECT 1 AS x").columns(x=sa.Integer)
    assert compiler.bind(typed).plan.fallback is False
    assert compiler.bind(items.delete().where(items.c.id == 1)).plan.fallback is False


def context_default(context):
    return (
        f"{context.current_parameters['a.b']}-{context.get_current_parameters()['a.b']}"
    )


dotted = sa.Table(
    "compiler_dotted",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("a.b", sa.String(20)),
    sa.Column("derived", sa.String(50), default=context_default),
)


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_context_default_sees_unescaped_parameters(dialect_cls):
    bound = NativeCompiler(dialect_cls()).bind(dotted.insert().values({"a.b": "q"}))
    assert "q-q" in bound.args
    assert "q" in bound.args


def test_duplicate_result_labels_fall_back():
    compiler = NativeCompiler(SQLiteDialect_aiosqlite())
    stmt = sa.select(items.c.id, sa.literal(1).label("id"))
    assert compiler.bind(stmt).plan.fallback is True


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_bind_overrides_named_params(dialect_cls):
    compiler = NativeCompiler(dialect_cls())
    stmt = items.select().where(items.c.id == sa.bindparam("ormar_p0", 1))
    default = compiler.bind(stmt)
    overridden = compiler.bind(stmt, {"ormar_p0": 5})
    assert (default.args, overridden.args) == ([1], [5])
    assert default.plan is overridden.plan
