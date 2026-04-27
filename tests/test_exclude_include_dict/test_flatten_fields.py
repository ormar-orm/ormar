"""
End-to-end tests for :py:meth:`QuerySet.flatten_fields` and the
``flatten_fields`` / ``flatten_all`` kwargs on :py:meth:`Model.model_dump`.
"""

from typing import ForwardRef, Optional

import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class HQ(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_hqs")

    id: int = ormar.Integer(primary_key=True)
    city: str = ormar.String(max_length=100)


class Company(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_companies")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    hq: Optional[HQ] = ormar.ForeignKey(HQ)


class Manager(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_managers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Car(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_cars")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    year: Optional[int] = ormar.Integer(nullable=True)
    manufacturer: Optional[Company] = ormar.ForeignKey(Company)
    lead_manager: Optional[Manager] = ormar.ForeignKey(Manager)


PersonRef = ForwardRef("Person")


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    supervisor = ormar.ForeignKey(to=PersonRef, related_name="reports")


Person.update_forward_refs()


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class PostCategory(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_post_categories")

    id: int = ormar.Integer(primary_key=True)
    rating: int = ormar.Integer(default=0)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Optional[Author] = ormar.ForeignKey(Author)
    categories: list[Category] = ormar.ManyToMany(Category, through=PostCategory)


create_test_database = init_tests(base_ormar_config)


async def _setup_car_graph():
    hq = await HQ.objects.create(city="Tokyo")
    company = await Company.objects.create(name="Toyota", hq=hq)
    manager = await Manager.objects.create(name="Akio")
    car = await Car.objects.create(
        name="Corolla", year=2020, manufacturer=company, lead_manager=manager
    )
    return hq, company, manager, car


async def _setup_post_graph():
    author = await Author.objects.create(name="Kent")
    cat_a = await Category.objects.create(name="Tech")
    cat_b = await Category.objects.create(name="News")
    post = await Post.objects.create(title="Intro", author=author)
    await post.categories.add(cat_a)
    await post.categories.add(cat_b)
    return author, cat_a, cat_b, post


# ---------------------------------------------------------------------------
# Core dunder / python style coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_fk_string_form():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, car = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields("manufacturer")
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_fk_list_form():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields(["manufacturer"])
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_fk_set_form():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields({"manufacturer"})
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_fk_dict_ellipsis_form():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields({"manufacturer": ...})
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_fk_field_accessor():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields(Car.manufacturer)
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_fk_multiple_relations():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, manager, _ = await _setup_car_graph()
            cars_string = (
                await Car.objects.select_related(["manufacturer", "lead_manager"])
                .flatten_fields(["manufacturer", "lead_manager"])
                .all()
            )
            cars_dict = (
                await Car.objects.select_related(["manufacturer", "lead_manager"])
                .flatten_fields({"manufacturer": ..., "lead_manager": ...})
                .all()
            )
            for car_row in (cars_string[0], cars_dict[0]):
                data = car_row.model_dump()
                assert data["manufacturer"] == company.id
                assert data["lead_manager"] == manager.id


@pytest.mark.asyncio
async def test_flatten_fk_deep_dunder_vs_dict():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            hq, _, _, _ = await _setup_car_graph()
            cars_dunder = (
                await Car.objects.select_related("manufacturer__hq")
                .flatten_fields("manufacturer__hq")
                .all()
            )
            cars_dict = (
                await Car.objects.select_related("manufacturer__hq")
                .flatten_fields({"manufacturer": {"hq": ...}})
                .all()
            )
            cars_accessor = (
                await Car.objects.select_related("manufacturer__hq")
                .flatten_fields([Car.manufacturer.hq])
                .all()
            )
            for car_row in (cars_dunder[0], cars_dict[0], cars_accessor[0]):
                data = car_row.model_dump()
                assert isinstance(data["manufacturer"], dict)
                assert data["manufacturer"]["hq"] == hq.id


# ---------------------------------------------------------------------------
# Relation-kind coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_reverse_fk_list_of_pks():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, car = await _setup_car_graph()
            car2 = await Car.objects.create(name="Yaris", manufacturer=company)

            companies = (
                await Company.objects.select_related("cars")
                .flatten_fields("cars")
                .all()
            )
            companies_sorted = sorted(companies, key=lambda m: m.id)
            data = companies_sorted[0].model_dump()
            assert sorted(data["cars"]) == sorted([car.id, car2.id])


@pytest.mark.asyncio
async def test_flatten_m2m_list_of_pks():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, cat_a, cat_b, _ = await _setup_post_graph()
            posts = (
                await Post.objects.select_related("categories")
                .flatten_fields("categories")
                .all()
            )
            posts[0].model_dump()
            assert sorted(posts[0].model_dump()["categories"]) == sorted(
                [cat_a.id, cat_b.id]
            )


@pytest.mark.asyncio
async def test_flatten_m2m_empty_list():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create(name="Kent")
            await Post.objects.create(title="Empty", author=author)

            posts = (
                await Post.objects.select_related("categories")
                .flatten_fields("categories")
                .all()
            )
            assert posts[0].model_dump()["categories"] == []


@pytest.mark.asyncio
async def test_flatten_null_fk_is_none():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            company = await Company.objects.create(name="No HQ")
            cars = (
                await Company.objects.select_related("hq")
                .flatten_fields("hq")
                .filter(id=company.id)
                .all()
            )
            assert cars[0].model_dump()["hq"] is None


@pytest.mark.asyncio
async def test_flatten_self_referential_fk():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            boss = await Person.objects.create(name="Boss")
            await Person.objects.create(name="Worker", supervisor=boss)

            rows = (
                await Person.objects.select_related("supervisor")
                .filter(name="Worker")
                .flatten_fields("supervisor")
                .all()
            )
            assert rows[0].model_dump()["supervisor"] == boss.id


@pytest.mark.asyncio
async def test_flatten_m2m_suppresses_through_models():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, cat_a, cat_b, _ = await _setup_post_graph()
            posts = (
                await Post.objects.select_related("categories")
                .flatten_fields("categories")
                .all()
            )
            data = posts[0].model_dump(exclude_through_models=False)
            assert sorted(data["categories"]) == sorted([cat_a.id, cat_b.id])
            # the scalar-pk list has no place for through dicts; still safe
            assert "postcategory" not in data


# ---------------------------------------------------------------------------
# Multi-level nesting combinations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_fk_then_m2m_both_styles():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, cat_a, cat_b, _ = await _setup_post_graph()

            posts_dunder = (
                await Author.objects.select_related("posts__categories")
                .flatten_fields("posts__categories")
                .all()
            )
            posts_dict = (
                await Author.objects.select_related("posts__categories")
                .flatten_fields({"posts": {"categories": ...}})
                .all()
            )
            for author_row in (posts_dunder[0], posts_dict[0]):
                data = author_row.model_dump()
                assert sorted(data["posts"][0]["categories"]) == sorted(
                    [cat_a.id, cat_b.id]
                )


@pytest.mark.asyncio
async def test_flatten_three_levels_deep():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            hq, _, _, _ = await _setup_car_graph()
            # flatten the deepest hop — car.manufacturer is still a dict, but
            # manufacturer.hq renders as its pk
            rows_dunder = (
                await Car.objects.select_related("manufacturer__hq")
                .flatten_fields("manufacturer__hq")
                .all()
            )
            rows_dict = (
                await Car.objects.select_related("manufacturer__hq")
                .flatten_fields({"manufacturer": {"hq": ...}})
                .all()
            )
            for row in (rows_dunder[0], rows_dict[0]):
                assert row.model_dump()["manufacturer"]["hq"] == hq.id


# ---------------------------------------------------------------------------
# Combination with include / exclude (non-conflicting)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_plus_include_whole_relation_ok():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .fields({"name", "manufacturer"})
                .flatten_fields("manufacturer")
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_plus_exclude_sibling_column_ok():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, _, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .exclude_fields("year")
                .flatten_fields("manufacturer")
                .all()
            )
            data = cars[0].model_dump()
            assert data["year"] is None
            assert isinstance(data["manufacturer"], int)


@pytest.mark.asyncio
async def test_flatten_plus_exclude_same_relation_drops_field():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = (
                await Car.objects.select_related("manufacturer")
                .flatten_fields("manufacturer")
                .exclude_fields("manufacturer")
                .all()
            )
            data = cars[0].model_dump()
            assert data.get("manufacturer") is None


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_conflicts_with_include_child_sub_fields():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
            Car.objects.flatten_fields("manufacturer").fields(
                {"manufacturer": {"name"}}
            )


@pytest.mark.asyncio
async def test_flatten_conflicts_with_exclude_child_sub_fields():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
            Car.objects.flatten_fields("manufacturer").exclude_fields(
                {"manufacturer": {"name"}}
            )


@pytest.mark.asyncio
async def test_flatten_conflicts_order_independent():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
            Car.objects.fields({"manufacturer": {"name"}}).flatten_fields(
                "manufacturer"
            )


@pytest.mark.asyncio
async def test_flatten_prefix_collision_list():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="unreachable"):
            Car.objects.flatten_fields(["manufacturer", "manufacturer__hq"])


@pytest.mark.asyncio
async def test_flatten_prefix_collision_chained():
    async with base_ormar_config.database:
        qs = Car.objects.flatten_fields("manufacturer")
        with pytest.raises(QueryDefinitionError, match="unreachable"):
            qs.flatten_fields("manufacturer__hq")


@pytest.mark.asyncio
async def test_flatten_rejects_non_relation_target():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="not a relation"):
            Car.objects.flatten_fields("name")


@pytest.mark.asyncio
async def test_flatten_rejects_unknown_relation():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="Unknown relation"):
            Car.objects.flatten_fields("nope")


@pytest.mark.asyncio
async def test_flatten_vs_exclude_primary_keys_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            with pytest.raises(QueryDefinitionError, match="exclude_primary_keys"):
                cars[0].model_dump(
                    flatten_fields="manufacturer", exclude_primary_keys=True
                )


@pytest.mark.asyncio
async def test_flatten_all_vs_exclude_primary_keys_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            with pytest.raises(QueryDefinitionError, match="exclude_primary_keys"):
                cars[0].model_dump(flatten_all=True, exclude_primary_keys=True)


# ---------------------------------------------------------------------------
# flatten_all
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_all_single_level():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, manager, _ = await _setup_car_graph()
            cars = await Car.objects.select_related(
                ["manufacturer", "lead_manager"]
            ).all()
            data = cars[0].model_dump(flatten_all=True)
            assert data["manufacturer"] == company.id
            assert data["lead_manager"] == manager.id


@pytest.mark.asyncio
async def test_flatten_all_deeply_nested():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer__hq").all()
            data = cars[0].model_dump(flatten_all=True)
            # top-level FK collapses first; nested hq is unreachable through
            # the scalar - which is the exact intent of flatten_all
            assert data["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_all_covers_m2m_and_reverse():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author, cat_a, cat_b, post = await _setup_post_graph()
            authors = await Author.objects.select_related("posts__categories").all()
            data = authors[0].model_dump(flatten_all=True)
            assert data["posts"] == [post.id]


# ---------------------------------------------------------------------------
# Auto-load select_related / prefetch_related
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_auto_adds_select_related_for_fk():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.flatten_fields("manufacturer").all()
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_auto_adds_prefetch_related_for_reverse_fk():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, car = await _setup_car_graph()
            companies = (
                await Company.objects.flatten_fields("cars").filter(id=company.id).all()
            )
            assert companies[0].model_dump()["cars"] == [car.id]


@pytest.mark.asyncio
async def test_flatten_auto_adds_prefetch_related_for_m2m():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, cat_a, cat_b, _ = await _setup_post_graph()
            posts = await Post.objects.flatten_fields("categories").all()
            assert sorted(posts[0].model_dump()["categories"]) == sorted(
                [cat_a.id, cat_b.id]
            )


@pytest.mark.asyncio
async def test_flatten_does_not_duplicate_select_related():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            qs = Car.objects.select_related("manufacturer").flatten_fields(
                "manufacturer"
            )
            assert qs._select_related.count("manufacturer") == 1
            cars = await qs.all()
            assert cars[0].model_dump()["manufacturer"] == company.id


# ---------------------------------------------------------------------------
# model_dump direct path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flatten_directly_on_model_dump():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            data = cars[0].model_dump(flatten_fields="manufacturer")
            assert data["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_directly_uses_pk_only_instance():
    # Even without select_related, ormar exposes the FK as a pk-only model,
    # so flattening on model_dump returns the pk value (not None).
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.all()
            data = cars[0].model_dump(flatten_fields="manufacturer")
            assert data["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_direct_with_null_fk_returns_none():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Car.objects.create(name="Orphan", manufacturer=None)
            cars = await Car.objects.filter(name="Orphan").all()
            data = cars[0].model_dump(flatten_fields="manufacturer")
            assert data["manufacturer"] is None


@pytest.mark.asyncio
async def test_flatten_through_model_dump_json():
    import json

    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            payload = cars[0].model_dump_json(flatten_fields="manufacturer")
            assert json.loads(payload)["manufacturer"] == company.id


# ---------------------------------------------------------------------------
# Filter composition, chaining, isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_on_deeper_relation_with_flattened_parent():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = (
                await Car.objects.filter(manufacturer__hq__city="Tokyo")
                .flatten_fields("manufacturer")
                .all()
            )
            assert cars[0].model_dump()["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_chained_flatten_fields_unions():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, manager, _ = await _setup_car_graph()
            qs = (
                Car.objects.select_related(["manufacturer", "lead_manager"])
                .flatten_fields("manufacturer")
                .flatten_fields("lead_manager")
            )
            cars = await qs.all()
            data = cars[0].model_dump()
            assert data["manufacturer"] == company.id
            assert data["lead_manager"] == manager.id


@pytest.mark.asyncio
async def test_sibling_querysets_remain_isolated():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            base = Car.objects.select_related("manufacturer")
            flat = base.flatten_fields("manufacturer")
            plain = base

            flat_cars = await flat.all()
            plain_cars = await plain.all()

            assert flat_cars[0].model_dump()["manufacturer"] == company.id
            assert isinstance(plain_cars[0].model_dump()["manufacturer"], dict)


@pytest.mark.asyncio
async def test_flatten_direct_field_accessor_single():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            data = cars[0].model_dump(flatten_fields=Car.manufacturer)
            assert data["manufacturer"] == company.id


@pytest.mark.asyncio
async def test_flatten_direct_field_accessor_list():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, manager, _ = await _setup_car_graph()
            cars = await Car.objects.select_related(
                ["manufacturer", "lead_manager"]
            ).all()
            data = cars[0].model_dump(
                flatten_fields=[Car.manufacturer, Car.lead_manager]
            )
            assert data["manufacturer"] == company.id
            assert data["lead_manager"] == manager.id


@pytest.mark.asyncio
async def test_flatten_direct_plus_include_child_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
                cars[0].model_dump(
                    flatten_fields="manufacturer",
                    include={"manufacturer": {"name"}},
                )


@pytest.mark.asyncio
async def test_flatten_direct_plus_exclude_child_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
                cars[0].model_dump(
                    flatten_fields="manufacturer",
                    exclude={"manufacturer": {"name": ...}},
                )


@pytest.mark.asyncio
async def test_flatten_direct_deep_conflict_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer__hq").all()
            with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
                cars[0].model_dump(
                    flatten_fields="manufacturer__hq",
                    include={"manufacturer": {"hq": {"city"}}},
                )


@pytest.mark.asyncio
async def test_flatten_list_plus_exclude_list_skips():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, _, _, _ = await _setup_post_graph()
            posts = (
                await Post.objects.select_related("categories")
                .flatten_fields("categories")
                .all()
            )
            data = posts[0].model_dump(exclude_list=True)
            assert "categories" not in data


@pytest.mark.asyncio
async def test_flatten_direct_with_dict_input_non_relation_is_silent():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            _, company, _, _ = await _setup_car_graph()
            cars = await Car.objects.select_related("manufacturer").all()
            # dict input is treated as already resolved; invalid leaves are
            # simply never visited during serialization and produce no effect.
            data = cars[0].model_dump(flatten_fields={"name": ...})
            assert data["name"] == "Corolla"
            assert isinstance(data["manufacturer"], dict)
