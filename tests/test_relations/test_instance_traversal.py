# mypy: disable-error-code="union-attr, attr-defined, index"
import json
import re
import typing
from collections.abc import Iterator
from typing import Any, ForwardRef, Optional

import pydantic
import pytest
from fastapi import FastAPI

import ormar
from ormar.models.mixins.relation_mixin import build_relation_tree
from ormar.models.traversal import (
    PathRelationMap,
    RelationTraversalSettings,
    relation_paths,
    set_relation_traversal,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()
MUTUAL_SIZE = 5


def make_mutual_model(
    index: int, targets: list[type[ormar.Model]]
) -> type[ormar.Model]:
    """
    Create a model with a foreign key to each of the given models, so every
    model of the graph is related to every other one in both directions.

    :param index: number of the model in the graph
    :type index: int
    :param targets: models to point the foreign keys at
    :type targets: list[type[ormar.Model]]
    :return: new model class
    :rtype: type[ormar.Model]
    """
    attrs: dict[str, Any] = {
        "ormar_config": base_ormar_config.copy(tablename=f"tm_{index}"),
        "__annotations__": {"id": int},
        "id": ormar.Integer(primary_key=True),
    }
    for target in targets:
        relation = f"to_{target.ormar_config.tablename}"
        attrs["__annotations__"][relation] = Optional[target]
        attrs[relation] = ormar.ForeignKey(
            to=target, related_name=f"from_{index}", nullable=True
        )
    return type(f"Mutual{index}", (ormar.Model,), attrs)


MUTUAL: list[type[ormar.Model]] = []
for mutual_index in range(MUTUAL_SIZE):
    MUTUAL.append(make_mutual_model(index=mutual_index, targets=list(MUTUAL)))

EmployeeRef = ForwardRef("Employee")
RingARef = ForwardRef("RingA")


class Employee(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_employees")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    manager: Optional[EmployeeRef] = ormar.ForeignKey(  # type: ignore
        to=EmployeeRef, related_name="reports"
    )
    coworkers: Optional[list[EmployeeRef]] = ormar.ManyToMany(  # type: ignore
        to=EmployeeRef, related_name="coworker_of"
    )


Employee.update_forward_refs()


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_writers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Label(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_labels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class ManuscriptLabel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_labelling")

    id: int = ormar.Integer(primary_key=True)
    weight: int = ormar.Integer(default=1)


class Manuscript(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_manuscripts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Optional[Writer] = ormar.ForeignKey(Writer, related_name="written")
    editor: Optional[Writer] = ormar.ForeignKey(Writer, related_name="edited")
    labels: Optional[list[Label]] = ormar.ManyToMany(Label, through=ManuscriptLabel)


class RingA(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_ring_a")

    id: int = ormar.Integer(primary_key=True)


class RingB(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_ring_b")

    id: int = ormar.Integer(primary_key=True)
    ring_a: Optional[RingA] = ormar.ForeignKey(RingA, related_name="ring_bs")


class RingC(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_ring_c")

    id: int = ormar.Integer(primary_key=True)
    ring_b: Optional[RingB] = ormar.ForeignKey(RingB, related_name="ring_cs")
    ring_a: Optional[RingARef] = ormar.ForeignKey(  # type: ignore
        RingARef, related_name="ring_cs"
    )


RingC.update_forward_refs()


class Pending(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="traversal_pending")

    id: int = ormar.Integer(primary_key=True)
    later: Optional[ForwardRef("NeverDefined")] = ormar.ForeignKey(  # type: ignore # noqa: F821
        ForwardRef("NeverDefined")
    )


create_test_database = init_tests(base_ormar_config)


@pytest.fixture
def instance_mode() -> Iterator[None]:
    previous = RelationTraversalSettings.mode
    set_relation_traversal(mode="instance")
    yield
    set_relation_traversal(mode=previous)


@pytest.fixture
def map_mode() -> Iterator[None]:
    previous = RelationTraversalSettings.mode
    set_relation_traversal(mode="map")
    yield
    set_relation_traversal(mode=previous)


def expanded_tree(model: type[ormar.Model], ancestors: tuple = ()) -> dict:
    """
    Reference relation tree with every path spelled out: a relation is
    followed unless its target is on the path above the current model.

    :param model: model of the current node
    :type model: type[ormar.Model]
    :param ancestors: models on the path above the current node
    :type ancestors: tuple
    :return: nested dict of relation names, ``...`` at the leaves
    :rtype: dict
    """
    tree: dict = {}
    for relation in sorted(model.extract_related_names()):
        target = model.ormar_config.model_fields[relation].to
        if target in ancestors:
            continue
        tree[relation] = (
            expanded_tree(model=target, ancestors=(*ancestors, model)) or Ellipsis
        )
    return tree


def repeated_models(
    dump: dict, model: type[ormar.Model], ancestors: tuple = ()
) -> list:
    """
    Lists relation paths of a dump that reach a model already on the path
    above the current model.

    :param dump: dict of one model
    :type dump: dict
    :param model: model of the dict
    :type model: type[ormar.Model]
    :param ancestors: models on the path above the current node
    :type ancestors: tuple
    :return: relation names leading to a repeated model
    :rtype: list
    """
    found = []
    for relation in model.extract_related_names():
        nested = dump.get(relation)
        if not nested:
            continue
        target = model.ormar_config.model_fields[relation].to
        if target in ancestors:
            found.append(relation)
        for item in nested if isinstance(nested, list) else [nested]:
            found.extend(
                repeated_models(dump=item, model=target, ancestors=(*ancestors, model))
            )
    return found


def mutual_instances() -> list[ormar.Model]:
    """
    Build one instance of every mutual model, each related to all others.

    :return: instances in model order
    :rtype: list[ormar.Model]
    """
    instances: list[ormar.Model] = []
    for model in MUTUAL:
        kwargs = {
            f"to_{target.ormar_config.tablename}": instances[target_index]
            for target_index, target in enumerate(MUTUAL[: len(instances)])
        }
        instances.append(model(id=len(instances) + 1, **kwargs))
    return instances


KEEP_ALIVE: list[ormar.Model] = []


def employees() -> Employee:
    boss = Employee(id=1, name="boss")
    middle = Employee(id=2, name="middle", manager=boss)
    junior = Employee(id=3, name="junior", manager=middle)
    peer = Employee(id=4, name="peer")
    middle.coworkers.append(peer)
    KEEP_ALIVE.extend([boss, junior, peer])
    return middle


def manuscript() -> Manuscript:
    return Manuscript(
        id=1,
        title="Draft",
        author=Writer(id=1, name="Ann"),
        editor=Writer(id=2, name="Ed"),
        labels=[Label(id=1, name="sci-fi")],
    )


def ring() -> RingA:
    ring_a = RingA(id=1)
    ring_b = RingB(id=1, ring_a=ring_a)
    ring_c = RingC(id=1, ring_b=ring_b, ring_a=ring_a)
    KEEP_ALIVE.extend([ring_b, ring_c])
    return ring_a


def test_old_tree_is_wrong_on_mutual_graph() -> None:
    wrong = [
        model
        for model in MUTUAL
        if build_relation_tree(model=model) != expanded_tree(model=model)
    ]
    assert wrong


@pytest.mark.parametrize("root_index", range(MUTUAL_SIZE))
def test_mutual_graph_dump_matches_expanded_tree(
    root_index: int, instance_mode: None
) -> None:
    instances = mutual_instances()
    root = instances[root_index]
    dump = root.model_dump()
    assert dump == root.model_dump(relation_map=expanded_tree(model=type(root)))
    assert repeated_models(dump=dump, model=type(root)) == []


def test_mutual_graph_old_dump_repeats_models(map_mode: None) -> None:
    dumps = [(type(root), root.model_dump()) for root in mutual_instances()]
    repeated = [
        path
        for model, dump in dumps
        for path in repeated_models(dump=dump, model=model)
    ]
    assert repeated


@pytest.mark.parametrize(
    "factory", [employees, manuscript, ring], ids=["self", "two_fks_m2m", "ring"]
)
def test_dump_equals_old_dump_where_old_tree_is_right(factory: Any) -> None:
    root = factory()
    assert build_relation_tree(model=type(root)) == expanded_tree(model=type(root))
    set_relation_traversal(mode="map")
    old_dump = root.model_dump()
    set_relation_traversal(mode="instance")
    try:
        assert root.model_dump() == old_dump
    finally:
        set_relation_traversal(mode="map")


def test_self_relations(instance_mode: None) -> None:
    dump = employees().model_dump()
    assert dump["manager"]["name"] == "boss"
    assert "manager" not in dump["manager"]
    assert [report["name"] for report in dump["reports"]] == ["junior"]
    assert "reports" not in dump["reports"][0]
    assert [coworker["name"] for coworker in dump["coworkers"]] == ["peer"]


def test_two_fks_to_one_model_and_reverse(instance_mode: None) -> None:
    draft = manuscript()
    dump = draft.model_dump()
    assert dump["author"] == {"id": 1, "name": "Ann"}
    assert dump["editor"] == {"id": 2, "name": "Ed"}
    writer_dump = draft.author.model_dump()
    assert [item["title"] for item in writer_dump["written"]] == ["Draft"]
    assert writer_dump["written"][0]["labels"][0]["name"] == "sci-fi"
    assert "editor" not in writer_dump["written"][0]


def test_cycle_stops_at_repeated_model(instance_mode: None) -> None:
    dump = ring().model_dump()
    ring_b = dump["ring_bs"][0]
    assert "ring_a" not in ring_b
    assert "ring_a" not in ring_b["ring_cs"][0]
    assert dump["ring_cs"][0]["ring_b"] == {"id": 1}


@pytest.mark.parametrize(
    "selector",
    [
        {"include": {"id", "author"}},
        {"exclude": {"labels"}},
        {"exclude": {"author": {"name"}}},
        {"include": {"author": {"written": {"title"}}}},
        {"exclude_list": True},
        {"flatten_fields": "author"},
        {"exclude_none": True},
    ],
)
def test_include_exclude_unchanged(selector: dict) -> None:
    root = manuscript()
    set_relation_traversal(mode="map")
    old_dump = root.model_dump(**selector)
    set_relation_traversal(mode="instance")
    try:
        assert root.model_dump(**selector) == old_dump
    finally:
        set_relation_traversal(mode="map")


def test_path_relation_map_answers_like_the_tree() -> None:
    node = PathRelationMap(model=Employee, ancestors=())
    assert set(node) == {"coworker_of", "coworkers", "manager", "reports"}
    assert len(node) == 4
    assert node.get("missing") is None
    assert node["manager"] is Ellipsis
    assert "manager" not in PathRelationMap(model=Employee, ancestors=(Employee,))
    assert not PathRelationMap(model=Employee, ancestors=(Employee,))
    with pytest.raises(KeyError):
        node["name"]


def test_relation_paths_respect_depth() -> None:
    assert relation_paths(model=RingB, max_depth=0) == []
    assert relation_paths(model=RingB, max_depth=1) == ["ring_a", "ring_cs"]
    assert relation_paths(model=RingB, max_depth=2) == [
        "ring_a__ring_cs",
        "ring_cs__ring_a",
    ]
    assert relation_paths(model=Manuscript, max_depth=3) == [
        "author",
        "editor",
        "labels",
    ]


def test_unknown_mode() -> None:
    with pytest.raises(ValueError):
        set_relation_traversal(mode="tree")


@pytest.mark.asyncio
async def test_save_related_and_load_all(instance_mode: None) -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            draft = manuscript()
            await draft.author.save()
            await draft.editor.save()
            await draft.save()
            count = await draft.save_related(follow=True, save_all=True)
            assert count == 4
            loaded = await Manuscript.objects.select_all(follow=True).get(id=1)
            assert loaded.labels[0].name == "sci-fi"
            assert loaded.author.name == "Ann"
            await loaded.load_all(follow=True, max_depth=1)
            assert loaded.editor.name == "Ed"
            assert loaded.model_dump()["labels"][0]["manuscriptlabel"]["weight"] == 1


def schema_models(schema: Any) -> set:
    """
    Collects a generated pydantic model and every model it references.

    :param schema: pydantic model
    :type schema: Any
    :return: set of pydantic models
    :rtype: set
    """
    found: set = set()
    pending = [schema]
    while pending:
        current = pending.pop()
        if current in found:
            continue
        found.add(current)
        for field in current.model_fields.values():
            for argument in typing.get_args(field.annotation):
                for nested in (argument, *typing.get_args(argument)):
                    if isinstance(nested, type) and issubclass(
                        nested, pydantic.BaseModel
                    ):
                        pending.append(nested)
    return found


@pytest.mark.parametrize("root_index", range(MUTUAL_SIZE))
def test_recursive_pydantic_accepts_the_dump(
    root_index: int, instance_mode: None
) -> None:
    instances = mutual_instances()
    root = instances[root_index]
    schema = type(root).get_pydantic()
    dump = root.model_dump()
    assert schema.model_validate(dump).model_dump(exclude_unset=True) == dump
    relation_count = sum(len(model.extract_related_names()) for model in MUTUAL)
    assert len(schema_models(schema=schema)) <= relation_count + 1


def test_recursive_pydantic_omits_back_relation(instance_mode: None) -> None:
    schema = Manuscript.get_pydantic()
    author = schema.model_fields["author"].annotation
    author_schema = typing.get_args(author)[0]
    assert author_schema.__name__.startswith("Writer_without_written")
    assert set(author_schema.model_fields) == {"id", "name", "edited"}
    assert set(Writer.get_pydantic().model_fields) == {
        "id",
        "name",
        "written",
        "edited",
    }


def test_recursive_pydantic_self_reference(instance_mode: None) -> None:
    schema = Employee.get_pydantic()
    middle = employees()
    assert schema.model_validate(middle.model_dump()).manager.name == "boss"
    nested = schema.model_validate(
        {
            "id": 1,
            "name": "a",
            "manager": {"id": 2, "name": "b", "manager": {"id": 3, "name": "c"}},
        }
    )
    assert nested.manager.manager.name == "c"


def test_recursive_pydantic_selectors_and_depth(instance_mode: None) -> None:
    included = Manuscript.get_pydantic(include={"id": ..., "author": {"name"}})
    assert set(included.model_fields) == {"id", "author"}
    author_schema = typing.get_args(included.model_fields["author"].annotation)[0]
    assert set(author_schema.model_fields) == {"name"}
    shallow = Manuscript.get_pydantic(max_depth=1)
    labels = typing.get_args(shallow.model_fields["labels"].annotation)[0]
    label_schema = typing.get_args(labels)[0]
    assert set(label_schema.model_fields) == {"id", "name"}
    flat = Manuscript.get_pydantic(flatten_fields="author")
    assert flat.model_validate({"id": 1, "title": "x", "author": {"id": 5}}).author == 5


def test_recursive_pydantic_openapi(instance_mode: None) -> None:
    app = FastAPI()
    for model in (*MUTUAL, Employee, Manuscript, RingA):
        app.add_api_route(
            path=f"/{model.get_name()}",
            endpoint=lambda: None,
            response_model=model.get_pydantic(),
        )
    document = app.openapi()
    components = document["components"]["schemas"]
    references = set(
        re.findall(r'"#/components/schemas/([^"]+)"', json.dumps(document))
    )
    assert references <= set(components)
    assert len(components) < 100


def test_recursive_pydantic_skips_unresolved_forward_refs(instance_mode: None) -> None:
    assert set(Pending.get_pydantic().model_fields) == {"id"}


def test_cached_mode_keeps_walked_nodes() -> None:
    previous = RelationTraversalSettings.mode
    set_relation_traversal(mode="instance_cached")
    try:
        draft = manuscript()
        first = draft.model_dump()
        root_node = Manuscript._default_relation_map()
        assert Manuscript._default_relation_map() is root_node
        assert draft.model_dump() == first
        assert set(root_node.children) == {"author", "editor", "labels"}
    finally:
        set_relation_traversal(mode=previous)


def test_recursive_pydantic_without_back_relation_variants(instance_mode: None) -> None:
    schema = Manuscript.get_pydantic(omit_back_relations=False)
    author_schema = typing.get_args(schema.model_fields["author"].annotation)[0]
    assert set(author_schema.model_fields) == {"id", "name", "written", "edited"}
    draft = manuscript()
    assert schema.model_validate(draft.model_dump()).author.name == "Ann"
