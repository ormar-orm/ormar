from decimal import Decimal
from typing import Optional

import pytest
import pytest_asyncio
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import Coalesce, F, Greatest, Least
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import expression_clause
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fn_owners")

    id: int = ormar.Integer(primary_key=True)
    floor: int = ormar.Integer(default=0)


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fn_members")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    read_id: int = ormar.Integer(default=0)
    uses: int = ormar.Integer(default=0, name="uses_col")
    refunded: Optional[int] = ormar.Integer(nullable=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def members():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(floor=4)
            await Member.objects.create(name="a", read_id=5, uses=3, owner=owner)
            await Member.objects.create(name="b", read_id=9, uses=1, refunded=2)
            yield


async def column(name: str) -> dict:
    rows = await Member.objects.order_by("name").values_list(["name", name])
    return dict(rows)


@pytest.mark.asyncio
async def test_update_with_functions(members):
    for newest in (7, 3):
        await Member.objects.filter(name__in=["a", "b"]).update(
            read_id=Greatest(F("read_id"), newest)
        )
    assert await column("read_id") == {"a": 7, "b": 9}

    await Member.objects.update(each=True, uses=Greatest(F("uses") - 2, 0))
    assert await column("uses") == {"a": 1, "b": 0}

    await Member.objects.update(each=True, refunded=Coalesce(F("refunded"), 0) + 10)
    assert await column("refunded") == {"a": 10, "b": 12}

    await Member.objects.update(
        each=True, read_id=Least(Member.read_id, Member.uses + 5, 8) * 2
    )
    assert await column("read_id") == {"a": 12, "b": 10}


@pytest.mark.asyncio
async def test_filter_with_functions(members):
    by_greatest = Member.objects.filter(read_id__gte=Greatest(F("uses") * 3, 6))
    assert [member.name for member in await by_greatest.all()] == ["b"]
    by_coalesce = Member.objects.filter(Member.uses < Coalesce(F("refunded"), 4))
    assert [member.name for member in await by_coalesce.all()] == ["a", "b"]
    by_related = Member.objects.filter(read_id__gt=Least(F("owner__floor"), 99))
    assert [member.name for member in await by_related.all()] == ["a"]
    assert await Member.objects.exclude(
        uses=Greatest(1, Coalesce(F("refunded"), F("uses")))
    ).values_list("name", flatten=True) == ["b"]


@pytest.mark.asyncio
async def test_literal_arguments_keep_their_type(members):
    for limit in (3.5, Decimal("3.5")):
        for function in (Greatest(F("read_id") * 0, limit), Least(F("read_id"), limit)):
            queryset = Member.objects.filter(uses__lt=function)
            assert [member.name for member in await queryset.all()] == ["a", "b"]
    assert await Member.objects.filter(
        uses__lt=Greatest(2.5, F("refunded") * 0)
    ).values_list("name", flatten=True) == ["b"]


@pytest.mark.asyncio
async def test_greatest_and_least_with_null_follow_the_database(members):
    values = await Member.objects.order_by("name").values_list(["name", "refunded"])
    assert values == [("a", None), ("b", 2)]
    await Member.objects.update(
        each=True, read_id=Coalesce(Greatest(F("refunded"), F("uses")), -1)
    )
    ignores_null = base_ormar_config.database.dialect.name == "postgresql"
    assert await column("read_id") == {"a": 3 if ignores_null else -1, "b": 2}


@pytest.mark.asyncio
async def test_function_templates_are_cached_by_shape(members):
    cache = template_cache_for(model_cls=Member)
    cache.clear()
    for newest in (1, 2):
        await Member.objects.filter(name="a").update(
            read_id=Greatest(F("read_id"), newest)
        )
    assert len(cache.entries) == 1
    await Member.objects.filter(name="a").update(read_id=Least(F("read_id"), 1))
    await Member.objects.filter(name="a").update(
        read_id=Greatest(F("read_id"), F("uses"))
    )
    assert len(cache.entries) == 3
    assert await column("read_id") == {"a": 3, "b": 9}


def test_functions_compile_per_dialect():
    node = Greatest(Least(F("uses"), 2), Coalesce(F("refunded"), 0))._node()
    clause = expression_clause(
        node=node,
        columns=expression_columns(node=node, model=Member),
        slot="ormar_v0",
    )
    compiled = {
        dialect.name: str(clause.compile(dialect=dialect))
        for dialect in (sqlite.dialect(), postgresql.dialect(), mysql.dialect())
    }
    assert compiled["sqlite"] == (
        "max(min(fn_members.uses_col, ?), coalesce(fn_members.refunded, ?))"
    )
    assert compiled["postgresql"] == (
        "greatest(least(fn_members.uses_col, %(ormar_v0_0)s), "
        "coalesce(fn_members.refunded, %(ormar_v0_1)s))"
    )
    assert compiled["mysql"].startswith("greatest(least(")


def test_function_arguments():
    with pytest.raises(QueryDefinitionError, match="at least two"):
        Greatest(F("uses"))
    with pytest.raises(QueryDefinitionError, match="field or an expression"):
        Least(1, 2)
    with pytest.raises(QueryDefinitionError, match="None or bool"):
        Coalesce(F("refunded"), None)
    with pytest.raises(QueryDefinitionError, match="None or bool"):
        Greatest(F("uses"), True)
    with pytest.raises(TypeError):
        Greatest(F("uses"), 1) < 3
