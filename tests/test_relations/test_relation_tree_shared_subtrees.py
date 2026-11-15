import copy
import gc
import tracemalloc
import typing
from typing import Any, ForwardRef, Optional

import pydantic
import pytest

import ormar
from ormar.models.mixins.relation_mixin import RelationMixin
from ormar.models.relation_graph import RelationGraph
from ormar.queryset.utils import translate_list_to_dict
from tests.settings import create_config

base_ormar_config = create_config()
GRAPH_SIZE = 6


def make_graph_model(index: int, targets: list[type[ormar.Model]]) -> type[ormar.Model]:
    """
    Create a model with a foreign key and a many to many relation to each of
    the given models, so every model of the graph is related to every other one
    and the number of relation paths grows factorially.

    :param index: number of the model in the graph
    :type index: int
    :param targets: models to point the relations at
    :type targets: list[type[ormar.Model]]
    :return: new model class
    :rtype: type[ormar.Model]
    """
    attrs: dict[str, Any] = {
        "ormar_config": base_ormar_config.copy(tablename=f"shared_tree_{index}"),
        "__annotations__": {"id": int, "name": str},
        "id": ormar.Integer(primary_key=True),
        "name": ormar.String(max_length=100, default="node"),
    }
    for target in targets:
        target_name = target.ormar_config.tablename
        attrs["__annotations__"][f"fk_{target_name}"] = Optional[target]
        attrs[f"fk_{target_name}"] = ormar.ForeignKey(
            to=target, related_name=f"fk_from_{index}", nullable=True
        )
        attrs["__annotations__"][f"m2m_{target_name}"] = Optional[
            list[target]  # type: ignore[valid-type]
        ]
        attrs[f"m2m_{target_name}"] = ormar.ManyToMany(
            to=target, related_name=f"m2m_from_{index}"
        )
    return type(f"SharedTree{index}", (ormar.Model,), attrs)


GRAPH: list[type[ormar.Model]] = []
for graph_index in range(GRAPH_SIZE):
    GRAPH.append(make_graph_model(index=graph_index, targets=list(GRAPH)))
Leaf = GRAPH[-1]

PersonRef = ForwardRef("Person")
CycleARef = ForwardRef("CycleA")


class Mentorship(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_mentorships")

    id: int = ormar.Integer(primary_key=True)
    since: int = ormar.Integer(default=2020)


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    manager: Optional[PersonRef] = ormar.ForeignKey(  # type: ignore
        to=PersonRef, related_name="reports"
    )
    coworkers: Optional[list[PersonRef]] = ormar.ManyToMany(  # type: ignore
        to=PersonRef, related_name="coworker_of"
    )
    mentors: Optional[list[PersonRef]] = ormar.ManyToMany(  # type: ignore
        to=PersonRef, through=Mentorship, related_name="mentees"
    )


Person.update_forward_refs()


class Document(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_documents")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    created_by: Optional[Person] = ormar.ForeignKey(
        to=Person, related_name="created_documents"
    )
    updated_by: Optional[Person] = ormar.ForeignKey(
        to=Person, related_name="updated_documents"
    )


class Badge(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_badges")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Person] = ormar.ForeignKey(to=Person)
    awarded_by: Optional[Person] = ormar.ForeignKey(
        to=Person, related_name="awarded_badges"
    )


class CycleC(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_cycle_c")

    id: int = ormar.Integer(primary_key=True)
    next_a: Optional[CycleARef] = ormar.ForeignKey(  # type: ignore
        to=CycleARef, related_name="previous_c"
    )


class CycleB(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_cycle_b")

    id: int = ormar.Integer(primary_key=True)
    next_c: Optional[CycleC] = ormar.ForeignKey(to=CycleC, related_name="previous_b")


class CycleA(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_tree_cycle_a")

    id: int = ormar.Integer(primary_key=True)
    next_b: Optional[CycleB] = ormar.ForeignKey(to=CycleB, related_name="previous_a")


CycleC.update_forward_refs()

RELATED_TO_SELF = [Person, Mentorship, Document, Badge, CycleA, CycleB, CycleC]


def expanded_tree(model: type[ormar.Model]) -> dict:
    """
    The relation tree as ormar built it before: every relation path of the
    model spelled out and translated to a nested dict.

    :param model: model class
    :type model: type[ormar.Model]
    :return: nested dict of relation names
    :rtype: dict
    """
    model.__relation_map__ = None
    return translate_list_to_dict(list_to_trans=model._iterate_related_models())


def all_models() -> list[type[ormar.Model]]:
    """
    Every concrete ormar model defined so far in the test session, leaving out
    the classes whose definition raised.

    :return: model classes
    :rtype: list[type[ormar.Model]]
    """
    found: list[type[ormar.Model]] = []
    pending = list(ormar.Model.__subclasses__())
    while pending:
        model = pending.pop()
        pending.extend(model.__subclasses__())
        if "__relation_tree__" in vars(model) and not model.ormar_config.abstract:
            found.append(model)
    return found


def measure_build(build: Any) -> int:
    """
    Memory kept by the result of a tree build.

    :param build: callable building the tree
    :type build: Any
    :return: bytes still allocated after the build
    :rtype: int
    """
    gc.collect()
    tracemalloc.start()
    try:
        result = build()
        kept, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result
    return kept


def build_people() -> dict[str, ormar.Model]:
    """
    Instances of the self related models linked through every relation.

    :return: instances by name
    :rtype: dict[str, ormar.Model]
    """
    boss = Person(id=1, name="Boss")
    ann = Person(id=2, name="Ann", manager=boss, coworkers=[boss])
    bob = Person(id=3, name="Bob", manager=ann, coworkers=[ann, boss], mentors=[ann])
    document = Document(id=1, title="Plan", created_by=ann, updated_by=bob)
    Document(id=2, title="Notes", created_by=bob, updated_by=bob)
    badge = Badge(id=1, owner=bob, awarded_by=boss)
    cycle_c = CycleC(id=1)
    cycle_b = CycleB(id=1, next_c=cycle_c)
    cycle_a = CycleA(id=1, next_b=cycle_b)
    cycle_c.next_a = cycle_a  # type: ignore[assignment]
    return {
        "boss": boss,
        "ann": ann,
        "bob": bob,
        "document": document,
        "badge": badge,
        "cycle_a": cycle_a,
        "cycle_b": cycle_b,
        "cycle_c": cycle_c,
    }


def pydantic_shape(model: type[pydantic.BaseModel]) -> dict:
    """
    Field names of a generated pydantic model with the nested models expanded,
    independent of the random names of the generated classes.

    :param model: generated pydantic model
    :type model: type[pydantic.BaseModel]
    :return: field name to nested shape, or the annotation for plain fields
    :rtype: dict
    """
    shape: dict[str, Any] = {}
    for name, field in model.model_fields.items():
        nested = [
            argument
            for argument in typing.get_args(field.annotation) or [field.annotation]
            for argument in (typing.get_args(argument) or [argument])
            if isinstance(argument, type) and issubclass(argument, pydantic.BaseModel)
        ]
        shape[name] = pydantic_shape(nested[0]) if nested else str(field.annotation)
    return shape


@pytest.mark.parametrize("model", RELATED_TO_SELF)
def test_tree_of_self_related_models_equals_the_expanded_paths(model):
    assert model._relation_tree() == expanded_tree(model=model)


def test_relations_to_the_same_model_get_their_own_subtrees():
    tree = Person._relation_tree()
    assert {
        "manager",
        "reports",
        "coworkers",
        "coworker_of",
        "mentors",
        "mentees",
        "created_documents",
        "updated_documents",
        "badges",
        "awarded_badges",
    } == set(tree)
    reached_from_person = {
        "awarded_badges": ...,
        "badges": ...,
        "created_documents": ...,
        "updated_documents": ...,
    }
    for relation in ("manager", "reports", "coworkers", "mentors", "mentees"):
        assert tree[relation] == reached_from_person
    assert tree["created_documents"] is Ellipsis
    assert (
        Document._relation_tree()["created_by"]
        is not (Document._relation_tree()["updated_by"])
    )
    assert (
        Document._relation_tree()["created_by"]
        == (Document._relation_tree()["updated_by"])
    )
    assert CycleA._relation_tree() == {
        "next_b": {"next_c": ...},
        "previous_c": {"previous_b": ...},
    }


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"exclude": {"manager"}},
        {"exclude": {"coworkers": {"manager"}}},
        {"include": {"id", "name", "mentors"}},
        {"exclude_through_models": True},
        {"mode": "json"},
    ],
)
def test_dumps_of_self_related_models_match_the_expanded_paths(options):
    for name, instance in build_people().items():
        model = type(instance)
        expected = instance.model_dump(
            relation_map=expanded_tree(model=model), **options
        )
        assert instance.model_dump(**options) == expected, name


def test_pydantic_models_of_self_related_models_match_the_expanded_paths(
    monkeypatch,
):
    shared = {}
    for model in RELATED_TO_SELF:
        model.__cache__.clear()
        shared[model] = pydantic_shape(model=model.get_pydantic())
    monkeypatch.setattr(
        RelationMixin, "_relation_tree", classmethod(expanded_tree), raising=True
    )
    for model in RELATED_TO_SELF:
        model.__cache__.clear()
        assert pydantic_shape(model=model.get_pydantic()) == shared[model]
    Person.__cache__.clear()
    bob = build_people()["bob"]
    validated = Person.get_pydantic().model_validate(bob.model_dump())
    assert validated.model_dump() == bob.model_dump()


def test_tree_equals_the_expanded_paths_for_every_model():
    models = all_models()
    assert Leaf in models
    for model in models:
        assert model._relation_tree() == expanded_tree(model=model), model


def test_dense_graph_tree_equals_the_expanded_paths():
    assert len(Leaf._iterate_related_models()) == 9240
    for model in GRAPH:
        assert model._relation_tree() == expanded_tree(model=model)


def test_repeated_subtrees_are_one_object():
    tree = Leaf._relation_tree()
    first = tree["fk_shared_tree_0"]["fk_from_2"]
    assert isinstance(first, dict)
    assert tree["fk_shared_tree_1"]["fk_shared_tree_0"]["fk_from_2"] is first
    assert tree["m2m_shared_tree_3"]["fk_shared_tree_0"]["fk_from_2"] is first


def test_tree_is_built_without_the_relation_paths():
    RelationGraph.changed()
    Leaf._relation_tree()
    assert Leaf.__relation_map__ is None
    assert Leaf._iterate_related_models()
    assert Leaf.__relation_map__ is not None


def test_dense_graph_tree_keeps_a_fraction_of_the_expanded_memory():
    RelationGraph.changed()
    shared = measure_build(build=Leaf._relation_tree)
    expanded = measure_build(build=lambda: expanded_tree(model=Leaf))
    assert shared < 256 * 1024
    assert expanded > 20 * shared


def test_dumps_do_not_modify_the_shared_tree():
    root = GRAPH[0](id=1)
    middle = GRAPH[1](id=1, fk_shared_tree_0=root, m2m_shared_tree_0=[root])
    leaf = Leaf(id=1, fk_shared_tree_0=root, fk_shared_tree_1=middle)
    tree = Leaf._relation_tree()
    snapshot = copy.deepcopy(tree)
    for options in (
        {},
        {"exclude": {"fk_shared_tree_0"}},
        {"exclude": {"fk_shared_tree_1": {"fk_shared_tree_0": {"fk_from_1"}}}},
        {"include": {"id", "fk_shared_tree_1"}},
        {"exclude_through_models": True},
    ):
        leaf.model_dump(**options)
        middle.model_dump(**options)
    Leaf.get_pydantic(exclude={"fk_shared_tree_1__fk_shared_tree_0"})
    assert Leaf._relation_tree() is tree
    assert tree == snapshot


def test_tree_is_rebuilt_when_a_relation_is_registered():
    tree = Leaf._relation_tree()
    assert "extras" not in tree["fk_shared_tree_0"]

    class Extra(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="shared_tree_extras")

        id: int = ormar.Integer(primary_key=True)
        node: Optional[GRAPH[0]] = ormar.ForeignKey(GRAPH[0])  # type: ignore

    rebuilt = Leaf._relation_tree()
    assert rebuilt is not tree
    assert rebuilt["fk_shared_tree_0"]["extras"] is Ellipsis
    assert rebuilt == expanded_tree(model=Leaf)
    assert Extra._relation_tree() == expanded_tree(model=Extra)
