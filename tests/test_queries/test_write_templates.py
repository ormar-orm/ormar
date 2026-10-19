import datetime
import functools
import uuid
from typing import Optional
from unittest import mock

import pytest
import pytest_asyncio
import sqlalchemy as sa

import ormar
from ormar.databases.native import compiler
from ormar.models import NewBaseModel
from ormar.queryset import templates
from ormar.queryset.templates import (
    build_pk_select,
    delete_template,
    insert_statement,
    insert_template,
    pk_select_template,
    template_cache_for,
    update_template,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    note: Optional[str] = ormar.String(max_length=100, nullable=True)


class Token(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_tokens")

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    name: str = ormar.String(max_length=100)


class Big(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_big")

    id: int = ormar.BigInteger(primary_key=True, autoincrement=False)
    name: str = ormar.String(max_length=50)


class Keyed(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_keyed")

    id: int = ormar.Integer(primary_key=True, name="key_col")
    name: str = ormar.String(max_length=50)


class Stamped(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_stamped")

    id: int = ormar.Integer(primary_key=True)
    stamp: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)
    payload: dict = ormar.JSON(default={})
    name: str = ormar.String(max_length=50)


class PkOnly(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_pk_only")

    id: int = ormar.Integer(primary_key=True)


create_test_database = init_tests(base_ormar_config)


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_owners")

    id: int = ormar.Integer(primary_key=True)


class Thing(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="write_template_things")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="items")


class CallCounter:
    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __get__(self, instance, owner):
        return functools.partial(self.call, instance)

    def call(self, table, *args, **kwargs):
        self.calls += 1
        return self.original(table, *args, **kwargs)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


@pytest.mark.asyncio
async def test_insert_template_per_column_set(connected):
    template_cache_for(Item).clear()
    first = insert_template(Item, {"name": "a", "note": None})
    second = insert_template(Item, {"note": "x", "name": "b"})
    other = insert_template(Item, {"name": "c"})
    assert first.statement is second.statement
    assert other.statement is not first.statement
    assert second.params == {"ormar_v0": "b", "ormar_v1": "x"}


@pytest.mark.asyncio
async def test_update_template_binds_values_and_pk(connected):
    template = update_template(Item, {"note": "n", "name": "m"}, 5)
    sql = str(template.statement.compile())
    assert "ormar_pk" in sql
    assert template.params == {"ormar_v0": "m", "ormar_v1": "n", "ormar_pk": 5}


@pytest.mark.asyncio
async def test_delete_and_pk_select_templates(connected):
    assert delete_template(Item, 3).params == {"ormar_pk": 3}
    assert delete_template(Item, 4).statement is delete_template(Item, 9).statement
    selected = pk_select_template(Item, 6)
    assert selected.params == {"ormar_pk": 6}
    assert pk_select_template(Item, 7).statement is selected.statement


@pytest.mark.asyncio
async def test_model_writes_build_no_statements_on_hit(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        warm = await Item(name="warm").save()
        warm.note = "n"
        await warm.update()
        await warm.load()
        await warm.upsert(__force_save__=True)
        await warm.delete()
        counters = {
            name: CallCounter(getattr(sa.Table, name))
            for name in ("insert", "update", "delete", "select")
        }
        for name, counter in counters.items():
            monkeypatch.setattr(sa.Table, name, counter)
        item = await Item(name="hit").save()
        item.note = "m"
        await item.update()
        await item.load()
        await item.upsert(__force_save__=True)
        await item.delete()
    assert {name: counter.calls for name, counter in counters.items()} == {
        "insert": 0,
        "update": 0,
        "delete": 0,
        "select": 0,
    }
    template_cache_for(Item).clear()
    insert_template(Item, {"name": "new shape"})
    assert counters["insert"].calls == 1


@pytest.mark.asyncio
async def test_save_with_explicit_pk_keeps_it(connected):
    async with base_ormar_config.database.transaction(force_rollback=True):
        item = await Item(id=4242, name="explicit").save()
        token_id = uuid.uuid4()
        token = await Token(id=token_id, name="t").save()
        assert item.id == 4242
        assert token.id == token_id
        assert (await Token.objects.get(id=token_id)).name == "t"


@pytest.mark.asyncio
async def test_write_cross_check_detects_mismatch(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    template_cache_for(Item).clear()
    table = Item.ormar_config.table
    wrong = build_pk_select(table, table.c.id)
    template_cache_for(Item).put(("delete",), wrong)
    with pytest.raises(AssertionError, match="template mismatch"):
        delete_template(Item, 1)
    template_cache_for(Item).clear()


@pytest.mark.asyncio
async def test_pk_column_property(connected):
    item = Item(name="x")
    assert item.pk_column is Item.ormar_config.table.c.id
    assert item.pk_column is Item.ormar_config.table.c.id


@pytest.mark.asyncio
async def test_queryset_writes_use_templates(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Item.objects.bulk_create([Item(name=f"q{i}") for i in range(4)])
        await Item.objects.filter(name="q0").update(note="warm")
        await Item.objects.filter(name="q0").delete()
        counters = {
            name: CallCounter(getattr(sa.Table, name)) for name in ("update", "delete")
        }
        for name, counter in counters.items():
            monkeypatch.setattr(sa.Table, name, counter)
        updated = await Item.objects.filter(name="q1").update(note="hit")
        deleted = await Item.objects.filter(name="q2").delete()
        notes = {item.name: item.note for item in await Item.objects.all()}
    assert {name: counter.calls for name, counter in counters.items()} == {
        "update": 0,
        "delete": 0,
    }
    assert (updated, deleted) == (1, 1)
    assert notes == {"q1": "hit", "q3": None}


@pytest.mark.asyncio
async def test_bulk_ops_use_templates(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Item.objects.bulk_create([Item(name="w1"), Item(name="w2")])
        warm = await Item.objects.all()
        await Item.objects.bulk_update(warm)
        counters = {
            name: CallCounter(getattr(sa.Table, name)) for name in ("insert", "update")
        }
        for name, counter in counters.items():
            monkeypatch.setattr(sa.Table, name, counter)
        await Item.objects.bulk_create(
            [Item(name="h1"), Item(name="h2"), Item(name="h3")]
        )
        await Item.objects.bulk_create([Item(name="single")])
        items = await Item.objects.all()
        for item in items:
            item.note = f"n-{item.name}"
        await Item.objects.bulk_update(items)
        stored = {item.name: item.note for item in await Item.objects.all()}
    assert {name: counter.calls for name, counter in counters.items()} == {
        "insert": 0,
        "update": 0,
    }
    assert stored == {
        name: f"n-{name}" for name in ("w1", "w2", "h1", "h2", "h3", "single")
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [True, False])
async def test_explicit_pk_kept_on_both_execution_paths(connected, native):
    database = base_ormar_config.database
    backend = database.native_backend
    if not native:
        database.native_backend = None
    try:
        async with database.transaction(force_rollback=True):
            big = await Big(id=1000, name="b").save()
            keyed = await Keyed(id=77, name="k").save()
            token_id = uuid.uuid4()
            token = await Token(id=token_id, name="t").save()
            assert (big.pk, keyed.pk, token.pk) == (1000, 77, token_id)
            await big.update(name="b2")
            assert (await Big.objects.get(id=1000)).name == "b2"
    finally:
        database.native_backend = backend


@pytest.mark.asyncio
async def test_update_with_non_column_value_raises_compile_error(connected):
    with pytest.raises(sa.exc.CompileError, match="Unconsumed column names"):
        await Owner.objects.filter(id=1).update(each=False, items=[])


@pytest.mark.asyncio
async def test_bulk_create_pk_only_models_inserts_every_row(connected):
    async with base_ormar_config.database.transaction(force_rollback=True):
        await PkOnly.objects.bulk_create([PkOnly(), PkOnly(), PkOnly()])
        assert await PkOnly.objects.count() == 3


@pytest.mark.asyncio
async def test_single_batch_bulk_create_relies_on_atomic_driver(connected, monkeypatch):
    database = base_ormar_config.database
    backend = database.native_backend
    atomic = backend is not None and backend.adapter.atomic_execute_many
    assert database.atomic_execute_many is atomic
    counter = mock.Mock(wraps=database.transaction)
    monkeypatch.setattr(database, "transaction", counter)
    await Item.objects.bulk_create([Item(name="a1"), Item(name="a2")])
    assert counter.call_count == (0 if atomic else 1)
    with pytest.raises(Exception):
        await Item.objects.bulk_create(
            [Item(id=9001, name="x"), Item(id=9001, name="y")]
        )
    assert await Item.objects.filter(id=9001).count() == 0
    await Item.objects.filter(name__in=["a1", "a2"]).delete()


@pytest.mark.asyncio
async def test_direct_batch_binding_matches_expanded_binding(connected, monkeypatch):
    backend = base_ormar_config.database.native_backend
    if backend is None:  # pragma: no cover
        pytest.skip("native execution only")
    names = ("name", "payload", "stamp")
    statement = insert_statement(Stamped, names)
    stamp = datetime.datetime(2021, 1, 2, 3, 4, 5)
    rows = [
        {"ormar_v0": f"n{i}", "ormar_v1": {"i": i}, "ormar_v2": stamp} for i in range(3)
    ]
    direct = backend.compiler.bind_many(statement, rows)
    monkeypatch.setattr(compiler, "binds_directly", lambda compiled, row: False)
    expanded = backend.compiler.bind_many(statement, rows)
    assert direct.sql == expanded.sql
    assert direct.args_list == expanded.args_list


@pytest.mark.asyncio
async def test_bulk_create_of_trusted_models_skips_model_dump(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    counter = mock.create_autospec(
        NewBaseModel.model_dump, side_effect=NewBaseModel.model_dump
    )
    monkeypatch.setattr(NewBaseModel, "model_dump", counter)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Item.objects.bulk_create([Item(name="d1"), Item(name="d2")])
        assert counter.call_count == 0
        assert await Item.objects.filter(name__in=["d1", "d2"]).count() == 2


class WrongDump:
    def __call__(self, *args, **kwargs):
        return {"name": "other"}


@pytest.mark.asyncio
async def test_trusted_dump_cross_check_detects_mismatch(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    monkeypatch.setattr(NewBaseModel, "model_dump", WrongDump())
    with pytest.raises(AssertionError, match="Trusted dump mismatch"):
        await Item.objects.bulk_create([Item(name="w")])


@pytest.mark.asyncio
async def test_direct_binding_of_one_statement_matches_expanded(connected, monkeypatch):
    backend = base_ormar_config.database.native_backend
    if backend is None:  # pragma: no cover
        pytest.skip("native execution only")
    stamp = datetime.datetime(2021, 1, 2, 3, 4, 5)
    template = update_template(
        Stamped, {"name": "n", "payload": {"a": 1}, "stamp": stamp}, 7
    )
    direct = backend.compiler.bind(template.statement, template.params)
    monkeypatch.setattr(compiler, "binds_directly", lambda compiled, row: False)
    expanded = backend.compiler.bind(template.statement, template.params)
    assert direct.sql == expanded.sql
    assert direct.args == expanded.args


@pytest.mark.asyncio
async def test_literal_execute_binds_are_rendered(connected):
    table = Item.ormar_config.table
    statement = table.select().where(
        table.c.name == sa.bindparam("name_value", literal_execute=True)
    )
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Item(name="lit").save()
        async with base_ormar_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(statement, {"name_value": "lit"})
    assert [row["name"] for row in rows] == ["lit"]
