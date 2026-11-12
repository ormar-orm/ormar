from datetime import datetime

import pytest
from sqlalchemy.dialects import mysql

import ormar
from ormar.databases.dialects import (
    MysqlRules,
    conflict_updates,
    on_update_assignment,
    on_update_variable,
)
from ormar.databases.query_executor import QueryExecutor
from ormar.queryset.templates import value_binds
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()

TOUCHED_AT = datetime(2026, 11, 12, 10, 30, 15)


def touched_now() -> datetime:
    return TOUCHED_AT


class Counter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="buo_counters")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=20, unique=True)
    price: int = ormar.Integer(default=0)
    revision: int = ormar.Integer(default=0, on_update=7)
    touched: datetime = ormar.DateTime(
        default=datetime(2000, 1, 1), on_update=touched_now, name="touched_at"
    )


create_test_database = init_tests(base_ormar_config)


@pytest.fixture()
def executed(monkeypatch):
    calls: list[str] = []
    for method_name in ("execute", "execute_many"):
        original = getattr(QueryExecutor, method_name)

        def recording(self, *args, _original=original, _name=method_name, **kwargs):
            calls.append(_name)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(QueryExecutor, method_name, recording)
    return calls


async def stored() -> dict[str, tuple]:
    rows = await Counter.objects.order_by("code").values_list(
        ["code", "price", "revision", "touched"]
    )
    return {code: (price, revision, touched) for code, price, revision, touched in rows}


@pytest.mark.asyncio
async def test_bulk_upsert_sets_on_update_values_in_one_batch(executed):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Counter.objects.bulk_create(
                [Counter(code=code, price=1) for code in ("a", "b", "c")]
            )
            executed.clear()
            await Counter.objects.bulk_create(
                [Counter(code=code, price=2) for code in ("a", "b", "d")],
                on_conflict="update",
                conflict_target=["code"],
                update_fields=["price"],
            )
            assert executed.count("execute_many") == 1
            assert executed.count("execute") == (
                1 if DATABASE_URL.startswith("mysql") else 0
            )
            initial = datetime(2000, 1, 1)
            assert await stored() == {
                "a": (2, 7, TOUCHED_AT),
                "b": (2, 7, TOUCHED_AT),
                "c": (1, 0, initial),
                "d": (2, 0, initial),
            }


@pytest.mark.asyncio
async def test_bulk_upsert_on_update_with_mixed_key_sets():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Counter.objects.create(code="a", price=1)
            await Counter.objects.create(code="b", price=1)
            await Counter.objects.bulk_create(
                [
                    Counter(id=first.pk, code="a", price=3),
                    Counter(code="b", price=3),
                    Counter(code="c", price=3),
                ],
                on_conflict="update",
                conflict_target=["code"],
                update_fields=["price"],
            )
            stored_rows = await stored()
            assert stored_rows["a"][:2] == (3, 7)
            assert stored_rows["b"][:2] == (3, 7)
            assert stored_rows["c"][:2] == (3, 0)


def test_mysql_upsert_reads_on_update_values_from_variables():
    table = Counter.ormar_config.table
    statement = MysqlRules(backend_name="mysql").conflict_insert(
        table=table,
        values=value_binds(table=table, names=("code", "price")),
        on_conflict="update",
        target=("code",),
        update_names=("price",),
        on_update=("revision", "touched_at"),
        returning=(),
        on_update_variables=True,
    )
    sql = str(statement.compile(dialect=mysql.dialect()))
    assert sql.endswith(
        "price = VALUES(price), revision = @ormar_on_update_0, "
        "touched_at = @ormar_on_update_1"
    )
    assignment = on_update_assignment(table=table, on_update=("revision", "touched_at"))
    assert str(assignment.compile(dialect=mysql.dialect())) == (
        "SET @ormar_on_update_0 = %s, @ormar_on_update_1 = %s"
    )
    assert on_update_variable(index=3) == "@ormar_on_update_3"
    bound = conflict_updates(
        table=table, inserted=table.c, update_names=(), on_update=("revision",)
    )
    assert bound["revision"].key == "ormar_on_update_revision"
