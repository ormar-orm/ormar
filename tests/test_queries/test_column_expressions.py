from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar import F
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_owners")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer(default=0)
    limit: int = ormar.Integer(default=0)


class Slot(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_slots")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, default="")
    taken: int = ormar.Integer(default=0)
    capacity: int = ormar.Integer(default=0, name="cap_col")
    price: Optional[float] = ormar.Float(nullable=True)
    revision: int = ormar.Integer(default=0, on_update=0)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, nullable=True)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_tags")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer(default=0)
    cap: int = ormar.Integer(default=0)


class Event(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_events")

    id: int = ormar.Integer(primary_key=True)
    tags: list[Tag] = ormar.ManyToMany(Tag)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def slots():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(score=5, limit=3)
            await Slot.objects.create(name="full", taken=3, capacity=3, owner=owner)
            await Slot.objects.create(name="free", taken=1, capacity=3, price=1.0)
            yield


@pytest.mark.asyncio
async def test_filter_compares_columns(slots):
    free = [Slot.taken < Slot.capacity, ormar.and_(taken__lt=F("capacity"))]
    for clause in free:
        assert [s.name for s in await Slot.objects.filter(clause).all()] == ["free"]
    assert await Slot.objects.filter(taken__gte=F("capacity")).count() == 1
    assert await Slot.objects.exclude(taken=F("capacity")).values_list(
        "name", flatten=True
    ) == ["free"]
    assert (await Slot.objects.get(taken__lt=Slot.capacity - 1)).name == "free"
    assert await Slot.objects.filter(
        ormar.or_(taken__gt=F("capacity"), taken__lte=F("taken") * 0 + 1)
    ).exists()
    assert await Slot.objects.filter(
        Slot.capacity == Slot.taken + F("taken") + 1
    ).count()


@pytest.mark.asyncio
async def test_filter_compares_columns_of_the_same_related_model(slots):
    by_kwargs = Slot.objects.filter(owner__score__gt=F("owner__limit"))
    by_accessor = Slot.objects.filter(Slot.owner.score > Slot.owner.limit)
    assert [s.name for s in await by_kwargs.all()] == ["full"]
    assert [s.name for s in await by_accessor.all()] == ["full"]


@pytest.mark.asyncio
async def test_update_with_expressions(slots):
    qs = Slot.objects.filter(name="free")
    assert await qs.update(taken=Slot.taken + 1) == 1
    assert await qs.update(capacity=F("capacity") * 2 - F("taken")) == 1
    free = await qs.get()
    assert (free.taken, free.capacity) == (2, 4)
    await qs.update(taken=F("capacity"))
    assert (await qs.get()).taken == 4
    await Slot.objects.filter(name="full").update(price=F("price") + 1)
    assert (await Slot.objects.get(name="full")).price is None
    await Slot.objects.update(each=True, revision=F("revision") + 1)
    assert await Slot.objects.filter(revision=1).count() == 2


@pytest.mark.asyncio
async def test_literal_types_get_their_own_statement(slots):
    template_cache_for(model_cls=Slot).clear()
    qs = Slot.objects.filter(name="free")
    as_int = qs.build_write_template(kind="update", values={"price": F("price") + 1})
    as_float = qs.build_write_template(
        kind="update", values={"price": F("price") + 1.5}
    )
    assert as_int.statement is not as_float.statement
    await qs.update(price=Slot.price + 1)
    await qs.update(price=Slot.price + 1.5)
    assert (await qs.get()).price == 3.5


@pytest.mark.asyncio
async def test_expression_statements_are_shared_by_shape(slots):
    template_cache_for(model_cls=Slot).clear()
    qs = Slot.objects.filter(id=1)

    def update(value):
        return qs.build_write_template(kind="update", values={"taken": value})

    plus_one, plus_five = update(F("taken") + 1), update(F("taken") + 5)
    assert plus_one.statement is plus_five.statement
    assert plus_five.params == {"ormar_p0": 1, "ormar_v0_0": 5}
    assert update(F("taken") * 2).statement is not plus_one.statement
    assert update(F("capacity") + 1).statement is not plus_one.statement
    accessor = Slot.objects.filter(taken__lt=Slot.capacity + 1).build_select_template()
    by_path = Slot.objects.filter(taken__lt=F("capacity") + 2).build_select_template()
    assert accessor.statement is by_path.statement
    assert by_path.params == {"ormar_p0_0": 2}


@pytest.mark.asyncio
async def test_cached_statements_bind_the_literals_of_each_call(slots):
    template_cache_for(model_cls=Slot).clear()
    qs = Slot.objects.filter(name="free")
    await qs.update(taken=F("taken") + 1)
    await qs.update(taken=F("taken") + 5)
    assert (await qs.get()).taken == 7
    assert await Slot.objects.filter(taken__lt=F("capacity") + 4).count() == 1
    assert await Slot.objects.filter(taken__lt=F("capacity") + 5).count() == 2
    assert await Slot.objects.filter(taken__lt=F("taken") * 2).count() == 2
    assert await Slot.objects.filter(taken__lt=F("capacity") - 1).count() == 0


@pytest.mark.asyncio
async def test_update_rejects_reading_columns_assigned_in_the_same_update(slots):
    qs = Slot.objects.filter(name="free")
    with pytest.raises(QueryDefinitionError, match="capacity"):
        await qs.update(taken=F("capacity"), capacity=5)
    with pytest.raises(QueryDefinitionError, match="revision"):
        await qs.update(taken=F("revision") + 1)
    await qs.update(taken=F("taken") + 1, capacity=5)
    assert (await qs.get()).taken == 2


@pytest.mark.asyncio
async def test_expressions_reference_own_fields_of_the_compared_model(slots):
    invalid_filters = [
        {"taken__lt": F("owner__score")},
        {"taken__lt": F("owner__id")},
        {"owner__score__lt": F("id")},
        {"owner__score__lt": F("capacity")},
        {"taken__lt": F("owner")},
        {"taken__lt": F("missing")},
        {"taken__lt": Slot.owner.score},
        {"name__contains": F("name")},
        {"taken__in": F("capacity")},
    ]
    for kwargs in invalid_filters:
        with pytest.raises(QueryDefinitionError):
            await Slot.objects.filter(**kwargs).all()
    for kwargs in [
        {"owner": F("taken")},
        {"taken": F("owner__score")},
        {"taken": F("owner__id")},
        {"taken": F("owner")},
    ]:
        with pytest.raises(QueryDefinitionError):
            await Slot.objects.filter(id=1).update(**kwargs)
    with pytest.raises(QueryDefinitionError):
        await Slot.objects.filter(owner__score__lt=F("owner__limit")).update(taken=1)


def test_expression_operands():
    with pytest.raises(QueryDefinitionError):
        Slot.owner + 1
    with pytest.raises(QueryDefinitionError):
        Slot.taken + None
    with pytest.raises(QueryDefinitionError):
        F("taken") - True
    with pytest.raises(TypeError):
        F("taken") < F("capacity")
    with pytest.raises(TypeError):
        1 + F("taken")
    with pytest.raises(TypeError):
        F("taken") / 2
    assert (F("taken") == 5) is False
    with pytest.raises(QueryDefinitionError):
        Slot.objects.filter(F("taken") == 5)


@pytest.mark.asyncio
async def test_many_to_many_prefetch_filters_with_expressions():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            event = await Event.objects.create()
            for score in (1, 5):
                await event.tags.add(await Tag.objects.create(score=score, cap=3))
            loaded = await Event.objects.prefetch_related(
                ormar.Prefetch("tags", queryset=Tag.objects.filter(score__lt=Tag.cap))
            ).get()
            assert [tag.score for tag in loaded.tags] == [1]
