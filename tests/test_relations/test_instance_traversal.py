# mypy: disable-error-code="union-attr, attr-defined, index"
import json
import re
import typing
import warnings
from typing import Any, ForwardRef, Optional

import pydantic
import pytest
from fastapi import FastAPI

import ormar
from ormar.models import recursive_schema, traversal
from ormar.models.mixins.relation_mixin import build_relation_tree
from ormar.models.traversal import (
    NO_ANCESTORS,
    PathRelationMap,
    RelationTraversalSettings,
    relation_paths,
    set_relation_traversal,
)
from ormar.warnings import OrmarDeprecationWarning
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


def dump_in_mode(root: ormar.Model, mode: str, **kwargs: Any) -> dict:
    """
    Dumps a model with the given relation traversal mode.

    :param root: model to dump
    :type root: ormar.Model
    :param mode: ``map`` or ``instance``
    :type mode: str
    :return: dump of the model
    :rtype: dict
    """
    previous = RelationTraversalSettings.mode
    RelationTraversalSettings.mode = mode
    try:
        return root.model_dump(**kwargs)
    finally:
        RelationTraversalSettings.mode = previous


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
    root_index: int, instance_traversal: None
) -> None:
    instances = mutual_instances()
    root = instances[root_index]
    dump = root.model_dump()
    assert dump == root.model_dump(relation_map=expanded_tree(model=type(root)))
    assert repeated_models(dump=dump, model=type(root)) == []


def test_mutual_graph_old_dump_repeats_models(map_traversal: None) -> None:
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
    assert dump_in_mode(root=root, mode="instance") == dump_in_mode(
        root=root, mode="map"
    )


def test_self_relations(instance_traversal: None) -> None:
    dump = employees().model_dump()
    assert dump["manager"]["name"] == "boss"
    assert "manager" not in dump["manager"]
    assert [report["name"] for report in dump["reports"]] == ["junior"]
    assert "reports" not in dump["reports"][0]
    assert [coworker["name"] for coworker in dump["coworkers"]] == ["peer"]


def test_two_fks_to_one_model_and_reverse(instance_traversal: None) -> None:
    draft = manuscript()
    dump = draft.model_dump()
    assert dump["author"] == {"id": 1, "name": "Ann"}
    assert dump["editor"] == {"id": 2, "name": "Ed"}
    writer_dump = draft.author.model_dump()
    assert [item["title"] for item in writer_dump["written"]] == ["Draft"]
    assert writer_dump["written"][0]["labels"][0]["name"] == "sci-fi"
    assert "editor" not in writer_dump["written"][0]


def test_cycle_stops_at_repeated_model(instance_traversal: None) -> None:
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
    assert dump_in_mode(root=root, mode="instance", **selector) == dump_in_mode(
        root=root, mode="map", **selector
    )


def test_path_relation_map_answers_like_the_tree() -> None:
    node = PathRelationMap.for_path(model=Employee, ancestors=NO_ANCESTORS)
    assert set(node) == {"coworker_of", "coworkers", "manager", "reports"}
    assert set(node.keys()) == set(node)
    assert len(node) == 4
    assert node.get("missing") is None
    assert node["manager"] is Ellipsis
    assert dict(node.items())["reports"] is Ellipsis
    assert list(node.values()) == [Ellipsis] * 4
    assert node == node
    assert node != dict(node.items())
    below_self = PathRelationMap.for_path(
        model=Employee, ancestors=frozenset({Employee})
    )
    assert "manager" not in below_self
    assert not below_self
    assert PathRelationMap.for_path(model=Employee, ancestors=NO_ANCESTORS) is node
    with pytest.raises(KeyError):
        node["name"]


def test_node_cache_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(traversal, "PATH_NODE_CACHE_SIZE", 2)
    nodes = traversal.relation_index(model=Label).nodes
    nodes.clear()
    for ancestors in (NO_ANCESTORS, frozenset({Writer}), frozenset({Manuscript})):
        PathRelationMap.for_path(model=Label, ancestors=ancestors)
    assert list(nodes) == [frozenset({Writer}), frozenset({Manuscript})]


def test_relation_paths_respect_depth() -> None:
    assert relation_paths(model=RingB, max_depth=0) == []
    assert relation_paths(model=RingB, max_depth=5) == ["ring_a"]
    assert relation_paths(model=RingB, max_depth=1, reverse_depth=1) == [
        "ring_a",
        "ring_cs",
    ]
    assert relation_paths(model=RingB, max_depth=2, reverse_depth=2) == [
        "ring_a__ring_cs",
        "ring_cs__ring_a",
    ]
    assert relation_paths(model=RingC, max_depth=5) == ["ring_a", "ring_b__ring_a"]
    assert relation_paths(model=Manuscript, max_depth=3, reverse_depth=3) == [
        "author",
        "editor",
        "labels",
    ]


def test_unknown_mode() -> None:
    with pytest.raises(ValueError, match="Unknown relation traversal mode"):
        set_relation_traversal(mode="tree")


def test_instance_is_the_default_and_map_is_deprecated() -> None:
    assert traversal.DEFAULT_RELATION_TRAVERSAL == "instance"
    previous = RelationTraversalSettings.mode
    try:
        with pytest.warns(DeprecationWarning, match="'map' relation traversal"):
            set_relation_traversal(mode="map")
        assert RelationTraversalSettings.mode == "map"
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            set_relation_traversal(mode="instance")
        assert traversal.instance_traversal_enabled()
    finally:
        RelationTraversalSettings.mode = previous


@pytest.mark.parametrize(
    "value, mode, deprecated",
    [(None, "instance", False), ("instance", "instance", False), ("map", "map", True)],
)
def test_mode_from_environment(
    monkeypatch: pytest.MonkeyPatch, value: Optional[str], mode: str, deprecated: bool
) -> None:
    if value is None:
        monkeypatch.delenv(traversal.RELATION_TRAVERSAL_ENV, raising=False)
    else:
        monkeypatch.setenv(traversal.RELATION_TRAVERSAL_ENV, value)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert traversal.relation_traversal_from_environment() == mode
    assert [item.category for item in caught] == (
        [OrmarDeprecationWarning] if deprecated else []
    )


@pytest.mark.asyncio
async def test_save_related_and_load_all(instance_traversal: None) -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            draft = manuscript()
            await draft.author.save()
            await draft.editor.save()
            await draft.save()
            count = await draft.save_related(follow=True, save_all=True)
            assert count == 4
            loaded = await Manuscript.objects.select_all(follow=True).get(id=1)
            assert loaded.author.name == "Ann"
            assert loaded.labels == []
            loaded = await Manuscript.objects.select_all(
                follow=True, follow_reverse=True
            ).get(id=1)
            assert loaded.labels[0].name == "sci-fi"
            await loaded.load_all(follow=True, max_depth=1, follow_reverse=1)
            assert loaded.editor.name == "Ed"
            assert loaded.model_dump()["labels"][0]["manuscriptlabel"]["weight"] == 1


async def raw_row_count(queryset: Any) -> int:
    """
    Counts the rows the database returns for a queryset, before ormar merges
    them into instances.

    :param queryset: queryset to run
    :type queryset: Any
    :return: number of raw rows
    :rtype: int
    """
    async with base_ormar_config.database.connection() as connection:
        result = await connection.execute(queryset.build_select_expression())
        return len(result.fetchall())


@pytest.mark.asyncio
async def test_forward_follow_does_not_multiply_rows(instance_traversal: None) -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Writer.objects.create(name="Ann")
            labels = [
                await Label.objects.create(name=f"l{index}") for index in range(3)
            ]
            for index in range(2):
                draft = await Manuscript.objects.create(
                    title=f"d{index}", author=author, editor=author
                )
                for label in labels:
                    await draft.labels.add(label)
            base = await raw_row_count(queryset=Manuscript.objects)
            forward = await raw_row_count(
                queryset=Manuscript.objects.select_all(follow=True)
            )
            with_reverse = await raw_row_count(
                queryset=Manuscript.objects.select_all(follow=True, follow_reverse=True)
            )
            assert base == forward == 2
            assert with_reverse == 6


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
    root_index: int, instance_traversal: None
) -> None:
    instances = mutual_instances()
    root = instances[root_index]
    schema = type(root).get_pydantic()
    dump = root.model_dump()
    assert schema.model_validate(dump).model_dump(exclude_unset=True) == dump
    relation_count = sum(len(model.extract_related_names()) for model in MUTUAL)
    assert len(schema_models(schema=schema)) <= relation_count + 1


def test_recursive_pydantic_omits_back_relation(instance_traversal: None) -> None:
    schema = Manuscript.get_pydantic(omit_back_relations=True)
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


def test_recursive_pydantic_self_reference(instance_traversal: None) -> None:
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


def test_recursive_pydantic_selectors_and_depth(instance_traversal: None) -> None:
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


def test_recursive_pydantic_openapi(instance_traversal: None) -> None:
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


def test_recursive_pydantic_skips_unresolved_forward_refs(
    instance_traversal: None,
) -> None:
    assert set(Pending.get_pydantic().model_fields) == {"id"}


def test_recursive_pydantic_without_back_relation_variants(
    instance_traversal: None,
) -> None:
    schema = Manuscript.get_pydantic(omit_back_relations=False)
    author_schema = typing.get_args(schema.model_fields["author"].annotation)[0]
    assert set(author_schema.model_fields) == {"id", "name", "written", "edited"}
    draft = manuscript()
    assert schema.model_validate(draft.model_dump()).author.name == "Ann"


SameNameWriter = type(
    "Writer",
    (ormar.Model,),
    {
        "ormar_config": base_ormar_config.copy(tablename="traversal_other_writers"),
        "__annotations__": {"id": int},
        "id": ormar.Integer(primary_key=True),
        "__module__": "tests.other_module",
    },
)


def reset_generated_schemas() -> None:
    """
    Forgets every generated pydantic model, like a fresh process.
    """
    for name in recursive_schema.RecursiveSchemaBuilder.names.values():
        if hasattr(recursive_schema.SCHEMA_MODULE, name):
            delattr(recursive_schema.SCHEMA_MODULE, name)
    recursive_schema.RecursiveSchemaBuilder.built.clear()
    recursive_schema.RecursiveSchemaBuilder.names.clear()
    recursive_schema.RecursiveSchemaBuilder.variants_by_name.clear()


def openapi_document(models: list[type[ormar.Model]]) -> str:
    """
    Builds the OpenAPI document of one route per model.

    :param models: models in the order their schemas are generated
    :type models: list[type[ormar.Model]]
    :return: OpenAPI document as sorted json
    :rtype: str
    """
    app = FastAPI()
    for model in models:
        app.add_api_route(
            path=f"/{model.get_name()}",
            endpoint=lambda: None,
            response_model=model.get_pydantic(),
        )
        app.add_api_route(
            path=f"/{model.get_name()}/short",
            endpoint=lambda: None,
            response_model=model.get_pydantic(max_depth=1, exclude={"id"}),
        )
    return json.dumps(app.openapi(), sort_keys=True)


def test_openapi_does_not_depend_on_the_order_of_calls(
    instance_traversal: None,
) -> None:
    models: list[type[ormar.Model]] = [Manuscript, Writer, Label, Employee, *MUTUAL]
    reset_generated_schemas()
    forward = openapi_document(models=models)
    reset_generated_schemas()
    backward = openapi_document(models=list(reversed(models)))
    assert forward == backward
    assert "Writer_nested" in json.loads(forward)["components"]["schemas"]


def test_generated_names(instance_traversal: None) -> None:
    reset_generated_schemas()
    assert Manuscript.get_pydantic().__name__ == "Manuscript"
    assert Manuscript.get_pydantic() is Manuscript.get_pydantic()
    author = typing.get_args(
        Manuscript.get_pydantic().model_fields["author"].annotation
    )[0]
    assert author.__name__ == "Writer_nested"
    assert Manuscript.get_pydantic(max_depth=2).__name__ == "Manuscript_depth2"
    selected = Manuscript.get_pydantic(include={"title", "id"})
    assert selected is Manuscript.get_pydantic(include={"id": ..., "title": ...})
    assert re.fullmatch(r"Manuscript_[0-9a-f]{6}", selected.__name__)
    assert Writer.get_pydantic().__name__ == "Writer"
    assert SameNameWriter.get_pydantic().__name__ == "Writer_2"


def test_variant_keeps_its_name_when_rebuilt(instance_traversal: None) -> None:
    schema = Label.get_pydantic()
    traversal.RelationGraph.changed()
    rebuilt = Label.get_pydantic()
    assert rebuilt is not schema
    assert rebuilt.__name__ == schema.__name__


def test_canonical_selector_ignores_order() -> None:
    assert recursive_schema.canonical(
        {"b": ..., "a": {"y", "x"}}
    ) == recursive_schema.canonical({"a": {"x": ..., "y": ...}, "b": ...})
    assert recursive_schema.canonical(["a", "b"]) == "{a:...,b:...}"


def test_recursive_pydantic_validates_cyclic_instances(
    instance_traversal: None,
) -> None:
    middle = employees()
    validated = Employee.get_pydantic().model_validate(middle)
    assert validated.manager.name == "boss"
    assert validated.reports[0].name == "junior"
    assert validated.model_dump(exclude_unset=True) == middle.model_dump()


def test_recursive_pydantic_in_map_mode_with_new_parameters(
    map_traversal: None,
) -> None:
    shallow = Manuscript.get_pydantic(max_depth=1)
    assert shallow.__name__ == "Manuscript_depth1"
    assert "author" in Manuscript.get_pydantic(omit_back_relations=False).model_fields


def test_map_mode_keeps_the_old_follow_and_schema(map_traversal: None) -> None:
    assert Manuscript._follow_relation_paths() == Manuscript._iterate_related_models()
    assert "labels" in Manuscript._follow_relation_paths()
    assert "labels" not in Manuscript._follow_relation_paths(max_depth=5)
    flat = Manuscript.get_pydantic(flatten_fields="author")
    assert flat is Manuscript.get_pydantic(flatten_fields="author")
    assert flat.model_validate({"id": 1, "title": "x", "author": {"id": 5}}).author == 5
