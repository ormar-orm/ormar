from typing import Optional

import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from ormar.models.excludable import ExcludableItems
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class NickNames(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nicks")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="hq_name")
    is_lame: Optional[bool] = ormar.Boolean(nullable=True)


class NicksHq(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nicks_x_hq")


class HQ(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="hq_name")
    nicks: list[NickNames] = ormar.ManyToMany(NickNames, through=NicksHq)


class Company(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="companies")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="company_name")
    founded: Optional[int] = ormar.Integer(nullable=True)
    hq: HQ = ormar.ForeignKey(HQ)


class Car(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    manufacturer: Optional[Company] = ormar.ForeignKey(Company)
    name: str = ormar.String(max_length=100)
    year: Optional[int] = ormar.Integer(nullable=True)
    gearbox_type: Optional[str] = ormar.String(max_length=20, nullable=True)
    gears: Optional[int] = ormar.Integer(nullable=True)
    aircon_type: Optional[str] = ormar.String(max_length=20, nullable=True)


create_test_database = init_tests(base_ormar_config)


def compare_results(excludable):
    car_excludable = excludable.get(Car)
    assert car_excludable.exclude == {"year", "gearbox_type", "gears", "aircon_type"}
    assert car_excludable.include == set()

    assert car_excludable.is_excluded("year")

    alias = Company.ormar_config.alias_manager.resolve_relation_alias(
        Car, "manufacturer"
    )
    manu_excludable = excludable.get(Company, alias=alias)
    assert manu_excludable.exclude == {"founded"}
    assert manu_excludable.include == set()

    assert manu_excludable.is_excluded("founded")


def compare_results_include(excludable):
    manager = Company.ormar_config.alias_manager
    car_excludable = excludable.get(Car)
    assert car_excludable.include == {"id", "name"}
    assert car_excludable.exclude == set()

    assert car_excludable.is_included("name")
    assert not car_excludable.is_included("gears")

    alias = manager.resolve_relation_alias(Car, "manufacturer")
    manu_excludable = excludable.get(Company, alias=alias)
    assert manu_excludable.include == {"name"}
    assert manu_excludable.exclude == set()

    assert manu_excludable.is_included("name")
    assert not manu_excludable.is_included("founded")

    alias = manager.resolve_relation_alias(Company, "hq")
    hq_excludable = excludable.get(HQ, alias=alias)
    assert hq_excludable.include == {"name"}
    assert hq_excludable.exclude == set()

    alias = manager.resolve_relation_alias(NicksHq, "nicknames")
    nick_excludable = excludable.get(NickNames, alias=alias)
    assert nick_excludable.include == {"name"}
    assert nick_excludable.exclude == set()


def test_excluding_fields_from_list():
    fields = ["gearbox_type", "gears", "aircon_type", "year", "manufacturer__founded"]
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="exclude")
    compare_results(excludable)


def test_excluding_fields_from_dict():
    fields = {
        "gearbox_type": ...,
        "gears": ...,
        "aircon_type": ...,
        "year": ...,
        "manufacturer": {"founded": ...},
    }
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="exclude")
    compare_results(excludable)


def test_excluding_fields_from_dict_with_set():
    fields = {
        "gearbox_type": ...,
        "gears": ...,
        "aircon_type": ...,
        "year": ...,
        "manufacturer": {"founded"},
    }
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="exclude")
    compare_results(excludable)


def test_gradual_build_from_lists():
    fields_col = [
        "year",
        ["gearbox_type", "gears"],
        "aircon_type",
        ["manufacturer__founded"],
    ]
    excludable = ExcludableItems()
    for fields in fields_col:
        excludable.build(items=fields, model_cls=Car, slot="exclude")
    compare_results(excludable)


def test_nested_includes():
    fields = [
        "id",
        "name",
        "manufacturer__name",
        "manufacturer__hq__name",
        "manufacturer__hq__nicks__name",
    ]
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="include")
    compare_results_include(excludable)


def test_nested_includes_from_dict():
    fields = {
        "id": ...,
        "name": ...,
        "manufacturer": {"name": ..., "hq": {"name": ..., "nicks": {"name": ...}}},
    }
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="include")
    compare_results_include(excludable)


def test_nested_includes_from_dict_with_set():
    fields = {
        "id": ...,
        "name": ...,
        "manufacturer": {"name": ..., "hq": {"name": ..., "nicks": {"name"}}},
    }
    excludable = ExcludableItems()
    excludable.build(items=fields, model_cls=Car, slot="include")
    compare_results_include(excludable)


def _compare_flatten_results_single(excludable):
    car_excludable = excludable.get(Car)
    assert car_excludable.flatten == {"manufacturer"}
    assert car_excludable.is_flattened("manufacturer")
    assert not car_excludable.is_flattened("gears")


def _compare_flatten_results_deep(excludable):
    manager = Company.ormar_config.alias_manager
    alias = manager.resolve_relation_alias(Car, "manufacturer")
    manu_excludable = excludable.get(Company, alias=alias)
    assert manu_excludable.flatten == {"hq"}
    assert manu_excludable.is_flattened("hq")

    car_excludable = excludable.get(Car)
    assert car_excludable.flatten == set()


def test_flatten_from_string():
    excludable = ExcludableItems()
    excludable.build(items="manufacturer", model_cls=Car, slot="flatten")
    _compare_flatten_results_single(excludable)
    assert excludable._flatten_paths == {("manufacturer",)}


def test_flatten_from_list():
    excludable = ExcludableItems()
    excludable.build(items=["manufacturer"], model_cls=Car, slot="flatten")
    _compare_flatten_results_single(excludable)


def test_flatten_from_set():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer"}, model_cls=Car, slot="flatten")
    _compare_flatten_results_single(excludable)


def test_flatten_from_dict_ellipsis():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer": ...}, model_cls=Car, slot="flatten")
    _compare_flatten_results_single(excludable)


def test_flatten_deep_dunder():
    excludable = ExcludableItems()
    excludable.build(items="manufacturer__hq", model_cls=Car, slot="flatten")
    _compare_flatten_results_deep(excludable)
    assert excludable._flatten_paths == {("manufacturer", "hq")}


def test_flatten_deep_dict():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer": {"hq": ...}}, model_cls=Car, slot="flatten")
    _compare_flatten_results_deep(excludable)


def test_flatten_deep_dict_with_set():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer": {"hq"}}, model_cls=Car, slot="flatten")
    _compare_flatten_results_deep(excludable)


def test_flatten_map_is_none_when_no_paths():
    excludable = ExcludableItems()
    assert excludable.flatten_map() is None


def test_flatten_map_caches_and_invalidates_on_new_path():
    excludable = ExcludableItems()
    excludable.build(items="hq", model_cls=Company, slot="flatten")
    first = excludable.flatten_map()
    assert first.data == {"hq": ...}
    assert excludable.flatten_map() is first  # cached: same FlattenMap reference

    # adding a new (non-colliding) flatten path invalidates the cache
    excludable.build(items="cars", model_cls=Company, slot="flatten")
    rebuilt = excludable.flatten_map()
    assert rebuilt is not first
    assert rebuilt.data == {"hq": ..., "cars": ...}


def test_flatten_has_flatten_entries_flag():
    excludable = ExcludableItems()
    assert excludable.has_flatten_entries() is False
    excludable.build(items="manufacturer", model_cls=Car, slot="flatten")
    assert excludable.has_flatten_entries() is True


def test_flatten_copy_preserves_paths_and_sets():
    excludable = ExcludableItems()
    excludable.build(items="manufacturer__hq", model_cls=Car, slot="flatten")
    clone = ExcludableItems.from_excludable(excludable)
    assert clone._flatten_paths == {("manufacturer", "hq")}
    manager = Company.ormar_config.alias_manager
    alias = manager.resolve_relation_alias(Car, "manufacturer")
    assert clone.get(Company, alias=alias).flatten == {"hq"}


def test_flatten_rejects_non_relation_leaf_from_dunder():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="not a relation"):
        excludable.build(items="manufacturer__name", model_cls=Car, slot="flatten")


def test_flatten_rejects_non_relation_leaf_from_dict():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="not a relation"):
        excludable.build(
            items={"manufacturer": {"name": ...}},
            model_cls=Car,
            slot="flatten",
        )


def test_flatten_rejects_non_relation_leaf_from_set_in_dict():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="not a relation"):
        excludable.build(
            items={"manufacturer": {"name"}}, model_cls=Car, slot="flatten"
        )


def test_flatten_rejects_unknown_relation():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="Unknown relation"):
        excludable.build(items="nope", model_cls=Car, slot="flatten")


def test_flatten_rejects_through_target():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="through model"):
        excludable.build(
            items="manufacturer__hq__nickshq", model_cls=Car, slot="flatten"
        )


def test_flatten_rejects_prefix_collision_list():
    excludable = ExcludableItems()
    with pytest.raises(QueryDefinitionError, match="unreachable"):
        excludable.build(
            items=["manufacturer", "manufacturer__hq"],
            model_cls=Car,
            slot="flatten",
        )


def test_flatten_rejects_prefix_collision_gradual():
    excludable = ExcludableItems()
    excludable.build(items="manufacturer", model_cls=Car, slot="flatten")
    with pytest.raises(QueryDefinitionError, match="unreachable"):
        excludable.build(items="manufacturer__hq", model_cls=Car, slot="flatten")


def test_flatten_vs_excludable_errors_on_child_sub_selection():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer__name"}, model_cls=Car, slot="include")
    excludable.build(items="manufacturer", model_cls=Car, slot="flatten")
    with pytest.raises(QueryDefinitionError, match="Flatten conflict"):
        excludable.validate_flatten_vs_excludable(source_model=Car)


def test_flatten_vs_excludable_allows_parent_whole_relation_include():
    excludable = ExcludableItems()
    excludable.build(items={"manufacturer"}, model_cls=Car, slot="include")
    excludable.build(items="manufacturer", model_cls=Car, slot="flatten")
    excludable.validate_flatten_vs_excludable(source_model=Car)


def test_includes_and_excludes_combo():
    fields_inc1 = ["id", "name", "year", "gearbox_type", "gears"]
    fields_inc2 = {"manufacturer": {"name"}}
    fields_exc1 = {"manufacturer__founded"}
    fields_exc2 = "aircon_type"
    excludable = ExcludableItems()
    excludable.build(items=fields_inc1, model_cls=Car, slot="include")
    excludable.build(items=fields_inc2, model_cls=Car, slot="include")
    excludable.build(items=fields_exc1, model_cls=Car, slot="exclude")
    excludable.build(items=fields_exc2, model_cls=Car, slot="exclude")

    car_excludable = excludable.get(Car)
    assert car_excludable.include == {"id", "name", "year", "gearbox_type", "gears"}
    assert car_excludable.exclude == {"aircon_type"}

    assert car_excludable.is_excluded("aircon_type")
    assert car_excludable.is_included("name")

    alias = Company.ormar_config.alias_manager.resolve_relation_alias(
        Car, "manufacturer"
    )
    manu_excludable = excludable.get(Company, alias=alias)
    assert manu_excludable.include == {"name"}
    assert manu_excludable.exclude == {"founded"}

    assert manu_excludable.is_excluded("founded")
