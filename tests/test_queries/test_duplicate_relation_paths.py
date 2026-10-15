from typing import ForwardRef, Optional

import pytest

import ormar
from ormar.exceptions import QueryDefinitionError, RelationshipInstanceError
from ormar.relations.relation_proxy import RelationProxy
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class City(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_cities")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=20, nullable=True)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_pets")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_people")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=20, nullable=True)
    city: Optional[City] = ormar.ForeignKey(City)
    pet: Optional[Pet] = ormar.ForeignKey(Pet, nullable=True)


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_orders")

    id: int = ormar.Integer(primary_key=True)
    zbuyer: Optional[Person] = ormar.ForeignKey(Person, related_name="bought")
    aseller: Optional[Person] = ormar.ForeignKey(Person, related_name="sold")


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_tags")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=20, nullable=True)


class ItemTag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_item_tags")

    id: int = ormar.Integer(primary_key=True)
    weight: int = ormar.Integer(default=0)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    tags: list[Tag] = ormar.ManyToMany(Tag, through=ItemTag)


class Bundle(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_bundles")

    id: int = ormar.Integer(primary_key=True)
    zmain: Optional[Item] = ormar.ForeignKey(Item, related_name="main_bundles")
    aextra: Optional[Item] = ormar.ForeignKey(Item, related_name="extra_bundles")


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="dup_members")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    friends: RelationProxy["Member"] = ormar.ManyToMany(
        ForwardRef("Member"), related_name="friend_of"
    )


Member.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


async def create_orders():
    x = await City.objects.create(name="X")
    y = await City.objects.create(name="Y")
    rex = await Pet.objects.create(name="rex")
    person_x = await Person.objects.create(name="px", city=x, pet=rex)
    person_y = await Person.objects.create(name="py", city=y, pet=rex)
    for order_id, buyer, seller in [
        (1, person_x, person_x),
        (2, person_x, person_y),
        (3, person_y, person_x),
        (4, person_y, person_y),
    ]:
        await Order.objects.create(id=order_id, zbuyer=buyer, aseller=seller)


def has_cartesian_product(queryset) -> bool:
    """A comma in the FROM clause means an unjoined (cross-product) table."""
    sql = str(queryset.build_select_expression().compile())
    return "," in sql.split("FROM", 1)[1].split("WHERE")[0]


async def order_ids(queryset):
    assert not has_cartesian_product(queryset)
    return sorted(order.id for order in await queryset.all())


@pytest.mark.asyncio
async def test_filters_and_excludes_in_every_order():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            buyer_y = Order.objects.filter(zbuyer__city__name="Y")
            buyer_not_x = Order.objects.exclude(zbuyer__city__name="X")
            assert await order_ids(buyer_y.filter(aseller__city__name="X")) == [3]
            assert await order_ids(
                Order.objects.filter(aseller__city__name="X").filter(
                    zbuyer__city__name="Y"
                )
            ) == [3]
            assert await order_ids(buyer_not_x.filter(aseller__city__name="X")) == [3]
            assert await order_ids(buyer_y.exclude(aseller__city__name="Y")) == [3]


@pytest.mark.asyncio
async def test_clauses_survive_join_reordering():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            base = Order.objects.filter(zbuyer__city__name="Y").filter(
                aseller__city__name="X"
            )
            assert await order_ids(base.select_related("aseller__pet")) == [3]
            assert await order_ids(base.filter(aseller__pet__name="rex")) == [3]


@pytest.mark.asyncio
async def test_derived_queryset_does_not_change_its_source():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            buyer_x = Order.objects.filter(zbuyer__city__name="X")
            assert await order_ids(buyer_x.filter(aseller__city__name="Y")) == [2]
            assert await order_ids(buyer_x) == [1, 2]


@pytest.mark.asyncio
async def test_order_by_both_paths_in_both_orders():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            both = Order.objects.select_related(["zbuyer__city", "aseller__city"])
            rows = await both.order_by(
                ["-aseller__city__name", "zbuyer__city__name"]
            ).all()
            assert [order.id for order in rows] == [2, 4, 1, 3]
            rows = await both.order_by(
                ["zbuyer__city__name", "-aseller__city__name"]
            ).all()
            assert [order.id for order in rows] == [2, 1, 4, 3]


@pytest.mark.asyncio
async def test_fields_and_exclude_fields_stay_on_their_path():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            both = Order.objects.select_related(["zbuyer__city", "aseller__city"])
            order = await both.fields(
                ["id", "zbuyer__id", "zbuyer__city__id", "aseller__id"]
                + ["aseller__city__name"]
            ).get(id=2)
            assert order.zbuyer.city.name is None
            assert order.aseller.city.name == "Y"
            order = await both.exclude_fields(["aseller__city__name"]).get(id=2)
            assert order.aseller.city.name is None
            assert order.zbuyer.city.name == "X"


@pytest.mark.asyncio
async def test_values_on_duplicate_paths():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            rows = (
                await Order.objects.select_related(["zbuyer__city", "aseller__city"])
                .filter(id=2)
                .values(["id", "zbuyer__city__name", "aseller__city__name"])
            )
            assert rows == [
                {
                    "id": 2,
                    "zbuyer__city__name": "X",
                    "aseller__city__name": "Y",
                }
            ]


@pytest.mark.asyncio
async def test_prefetch_on_duplicate_paths_with_fields():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_orders()
            order = (
                await Order.objects.prefetch_related(["zbuyer__city", "aseller__city"])
                .fields(["id", "zbuyer", "aseller", "zbuyer__id", "zbuyer__city"])
                .fields(["zbuyer__city__id", "zbuyer__city__name", "aseller__id"])
                .fields(["aseller__city", "aseller__city__id"])
                .get(id=2)
            )
            assert order.zbuyer.city.name == "X"
            assert order.aseller.city.name is None


async def create_bundles():
    red = await Tag.objects.create(name="red")
    blue = await Tag.objects.create(name="blue")
    main = await Item.objects.create(name="main")
    extra = await Item.objects.create(name="extra")
    await main.tags.add(red, weight=1)
    await extra.tags.add(blue, weight=5)
    await Bundle.objects.create(id=1, zmain=main, aextra=extra)
    await Bundle.objects.create(id=2, zmain=extra, aextra=main)


@pytest.mark.asyncio
async def test_through_filter_on_duplicate_m2m_paths():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_bundles()
            bundles = await Bundle.objects.filter(
                zmain__tags__itemtag__weight=1, aextra__tags__itemtag__weight=5
            ).all()
            assert [bundle.id for bundle in bundles] == [1]
            bundles = (
                await Bundle.objects.select_related(["zmain__tags", "aextra__tags"])
                .filter(zmain__tags__name="blue")
                .all()
            )
            assert [bundle.id for bundle in bundles] == [2]
            assert bundles[0].aextra.tags[0].name == "red"
            assert bundles[0].aextra.tags[0].itemtag.weight == 1


@pytest.mark.asyncio
async def test_prefetch_m2m_respects_fields_on_its_path():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_bundles()
            bundle = (
                await Bundle.objects.prefetch_related(["zmain__tags", "aextra__tags"])
                .fields(["id", "zmain", "aextra", "zmain__id", "zmain__tags__id"])
                .fields(["aextra__id"])
                .fields(["aextra__tags__id", "aextra__tags__name"])
                .get(id=1)
            )
            assert bundle.zmain.tags[0].name is None
            assert bundle.aextra.tags[0].name == "blue"


@pytest.mark.asyncio
async def test_queryset_proxy_on_duplicate_path_model():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_bundles()
            main = await Item.objects.get(name="main")
            tags = await main.tags.filter(itemtag__weight=1).all()
            assert [tag.name for tag in tags] == ["red"]
            assert tags[0].itemtag.weight == 1


@pytest.mark.asyncio
async def test_self_referencing_m2m_both_directions():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            ann = await Member.objects.create(name="ann")
            bob = await Member.objects.create(name="bob")
            cid = await Member.objects.create(name="cid")
            await ann.friends.add(bob)
            await cid.friends.add(ann)
            member = (
                await Member.objects.select_related(["friends", "friend_of"])
                .filter(friends__name="bob", friend_of__name="cid")
                .get()
            )
            assert member.name == "ann"
            assert [friend.name for friend in member.friends] == ["bob"]
            assert [friend.name for friend in member.friend_of] == ["cid"]


@pytest.mark.asyncio
async def test_alias_collision_raises(monkeypatch):
    async with base_ormar_config.database:
        monkeypatch.setattr("ormar.queryset.join.alias_for", lambda path: "tsame00")
        queryset = Order.objects.select_related(["zbuyer", "aseller"])
        with pytest.raises(QueryDefinitionError, match="collides"):
            queryset.build_select_expression()


@pytest.mark.asyncio
@pytest.mark.parametrize("related", ["itemtag", "tags__itemtag"])
async def test_select_related_on_through_field_is_rejected(related):
    async with base_ormar_config.database:
        with pytest.raises(RelationshipInstanceError, match="through model"):
            await Item.objects.select_related(related).all()
