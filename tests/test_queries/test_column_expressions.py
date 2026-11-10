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


class Zone(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_zones")

    id: int = ormar.Integer(primary_key=True)
    budget: int = ormar.Integer(default=0)


class Slot(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_slots")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, default="")
    taken: int = ormar.Integer(default=0)
    capacity: int = ormar.Integer(default=0, name="cap_col")
    price: Optional[float] = ormar.Float(nullable=True)
    revision: int = ormar.Integer(default=0, on_update=0)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, nullable=True)
    zone: Optional[Zone] = ormar.ForeignKey(Zone, nullable=True)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_tags")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer(default=0)
    cap: int = ormar.Integer(default=0)


class Event(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fx_events")

    id: int = ormar.Integer(primary_key=True)
    level: int = ormar.Integer(default=0)
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


@pytest_asyncio.fixture()
async def related():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Owner.objects.create(score=5, limit=3)
            second = await Owner.objects.create(score=1, limit=9)
            for name, taken, capacity, owner, budget in [
                ("a", 3, 6, first, 4),
                ("b", 2, 8, second, 2),
            ]:
                zone = await Zone.objects.create(budget=budget)
                await Slot.objects.create(
                    name=name, taken=taken, capacity=capacity, owner=owner, zone=zone
                )
            await Slot.objects.create(name="c", taken=1, capacity=3)
            yield


async def names(queryset):
    return sorted(slot.name for slot in await queryset.all())


@pytest.mark.asyncio
async def test_filter_reads_columns_of_related_models(related):
    queryset = Slot.objects.filter(taken__gte=F("owner__limit"))
    assert queryset._select_related == ["owner"]
    assert await names(queryset=queryset) == ["a"]
    assert await names(queryset=Slot.objects.filter(Slot.taken >= Slot.owner.limit))
    selected = Slot.objects.select_related("owner").filter(taken__gte=F("owner__limit"))
    assert selected._select_related == ["owner"]
    assert (await selected.get()).owner.score == 5
    shared = Slot.objects.filter(capacity__lt=F("owner__score") + F("owner__limit"))
    assert shared._select_related == ["owner"]
    assert await names(queryset=shared) == ["a", "b"]


@pytest.mark.asyncio
async def test_filter_compares_fields_of_different_related_models(related):
    for queryset in [
        Slot.objects.filter(owner__score__gt=F("zone__budget")),
        Slot.objects.filter(Slot.owner.score > Slot.zone.budget),
    ]:
        assert sorted(queryset._select_related) == ["owner", "zone"]
        assert await names(queryset=queryset) == ["a"]
    two_models = Slot.objects.filter(
        taken__lt=F("owner__limit") * 2 - F("zone__budget")
    )
    assert await names(queryset=two_models) == ["b"]
    assert await names(
        queryset=Slot.objects.filter(capacity__lt=F("owner__score") + Slot.zone.budget)
    ) == ["a"]


@pytest.mark.asyncio
async def test_related_expressions_in_exclude_groups_and_shortcuts(related):
    by_limit = {"taken__gte": F("owner__limit")}
    assert await names(queryset=Slot.objects.exclude(**by_limit)) == await names(
        queryset=Slot.objects.exclude(owner__limit__lte=3)
    )
    either = ormar.or_(taken__gte=F("owner__limit"), capacity__gt=F("zone__budget") * 3)
    assert await names(queryset=Slot.objects.filter(either)) == ["a", "b"]
    assert (await Slot.objects.get(Slot.taken >= Slot.owner.limit)).name == "a"
    assert await Slot.objects.filter(**by_limit).count() == 1
    assert await Slot.objects.filter(**by_limit).exists()
    assert await Slot.objects.filter(**by_limit).values_list("name", flatten=True) == [
        "a"
    ]
    assert await Slot.objects.filter(**by_limit).values(["name", "owner__score"]) == [
        {"name": "a", "owner__score": 5}
    ]


@pytest.mark.asyncio
async def test_expressions_read_reverse_and_many_to_many_relations(related):
    owners = await Owner.objects.filter(limit__gt=F("slots__taken")).all()
    assert [owner.score for owner in owners] == [1]
    event = await Event.objects.create(level=3)
    await Event.objects.create(level=0)
    for score in (1, 5):
        await event.tags.add(await Tag.objects.create(score=score))
    loaded = (
        await Event.objects.prefetch_related("tags")
        .filter(Event.level > Event.tags.score)
        .all()
    )
    assert [(item.level, [tag.score for tag in item.tags]) for item in loaded] == [
        (3, [1])
    ]
    by_value = Event.objects.prefetch_related("tags").filter(tags__score__lt=3)
    assert [len(item.tags) for item in await by_value.all()] == [1]


@pytest.mark.asyncio
async def test_related_expressions_get_their_own_statement(related):
    template_cache_for(model_cls=Slot).clear()

    def template(value):
        return Slot.objects.filter(taken__lt=value).build_select_template()

    score, limit = (
        template(value=F("owner__score") + 1),
        template(value=F("owner__limit") + 1),
    )
    assert score.statement is not limit.statement
    assert template(value=F("owner__score") + 2).statement is score.statement
    assert template(value=F("zone__budget") + 1).statement is not score.statement
    assert await Slot.objects.filter(taken__lt=F("owner__score") + 1).count() == 1
    assert await Slot.objects.filter(taken__lt=F("owner__limit") + 1).count() == 2
    assert await Slot.objects.filter(taken__lt=F("zone__budget") + 1).count() == 2


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
async def test_expressions_reference_non_relation_fields(slots):
    invalid_filters = [
        {"taken__lt": F("owner")},
        {"taken__lt": F("missing")},
        {"taken__lt": F("owner__missing")},
        {"taken__lt": F("nowhere__score")},
        {"taken__lt": F("name__score")},
        {"taken__lt": F("owner__slots")},
        {"name__contains": F("name")},
        {"taken__in": F("capacity")},
    ]
    for kwargs in invalid_filters:
        with pytest.raises(QueryDefinitionError):
            await Slot.objects.filter(**kwargs).all()
    for kwargs in [{"owner": F("taken")}, {"taken": F("owner")}]:
        with pytest.raises(QueryDefinitionError):
            await Slot.objects.filter(id=1).update(**kwargs)
    for kwargs in [{"taken": F("owner__score")}, {"taken": Slot.owner.id + 1}]:
        with pytest.raises(QueryDefinitionError, match="not support .* yet"):
            await Slot.objects.filter(id=1).update(**kwargs)
    for related_filter in [
        Slot.objects.filter(owner__score__lt=F("owner__limit")),
        Slot.objects.filter(taken__lt=F("owner__score")),
        Slot.objects.exclude(ormar.or_(taken__lt=F("zone__budget"), id=1)),
    ]:
        with pytest.raises(QueryDefinitionError, match="related models"):
            await related_filter.update(taken=1)
        with pytest.raises(QueryDefinitionError, match="related models"):
            await related_filter.delete()


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
            with pytest.raises(QueryDefinitionError, match="many to many"):
                await Event.objects.prefetch_related(
                    ormar.Prefetch(
                        "tags", queryset=Tag.objects.filter(score__lt=F("events__id"))
                    )
                ).get()
