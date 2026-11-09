"""
Deferred rebuilds of pydantic schemas of ormar models.

Registering a reverse relation (or a through model field) adds a field to a
model that pydantic has already built. Instead of rebuilding such a model right
away, once per added field, it is marked as pending and rebuilt once, with its
final fields, when its schema, validator or serializer is first needed.

Until then other ormar models built in the meantime embed the schema the
pending model had before it was marked, the same snapshot a forced rebuild
used to leave behind, so nested relation shapes stay the same.
"""

import itertools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ForwardRef, Optional, TypeVar

from pydantic._internal._mock_val_ser import MockCoreSchema, MockValSer
from pydantic_core import CoreSchema

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model

T = TypeVar("T")

PENDING_ATTR = "__ormar_pending_rebuild__"
SNAPSHOT_ATTR = "__ormar_schema_snapshot__"

building_schemas_var: ContextVar[bool] = ContextVar(
    "ormar_building_schemas", default=False
)
pending_order = itertools.count()


@contextmanager
def building_schemas() -> Iterator[None]:
    """
    Marks that ormar is building pydantic schemas of its own models, so
    pending models embedded in them are not rebuilt but reused as they are.

    :return: context manager
    :rtype: Iterator[None]
    """
    token = building_schemas_var.set(True)
    try:
        yield
    finally:
        building_schemas_var.reset(token)


def related_models(model: type["Model"]) -> list[type["Model"]]:
    """
    Returns models directly linked to the model by its relation fields,
    including through models of many to many relations.

    Relations still pointing to an unresolved ForwardRef are skipped.

    :param model: model whose relations are collected
    :type model: Model class
    :return: related model classes
    :rtype: list[Model class]
    """
    related: list[type["Model"]] = []
    for field in model.ormar_config.model_fields.values():
        if not field.is_relation:
            continue
        targets = [field.to, getattr(field, "through", None)]
        related.extend(
            target
            for target in targets
            if target is not None and target.__class__ != ForwardRef
        )
    return related


def connected_models(model: type["Model"]) -> list[type["Model"]]:
    """
    Returns the model and every model reachable from it through relations.

    Reverse relations are registered on both sides, so this is the set of
    models whose schemas can embed each other.

    :param model: model to start from
    :type model: Model class
    :return: connected model classes, starting with the given model
    :rtype: list[Model class]
    """
    seen = {model: None}
    to_visit = [model]
    while to_visit:
        for related in related_models(model=to_visit.pop()):
            if related not in seen:
                seen[related] = None
                to_visit.append(related)
    return list(seen)


def rebuild_model(model: type["Model"]) -> None:
    """
    Rebuilds pydantic schema, validator and serializer of the model with its
    current fields and clears its pending rebuild mark.

    If the rebuild fails (i.e. a related model still has an unresolved
    ForwardRef) the pending mark, snapshot and rebuild mocks are restored, so
    the rebuild is retried once the reference is resolved.

    :param model: model to rebuild
    :type model: Model class
    """
    marks = {
        attr: model.__dict__[attr]
        for attr in (PENDING_ATTR, SNAPSHOT_ATTR)
        if attr in model.__dict__
    }
    for attr in marks:
        delattr(model, attr)
    pending = PENDING_ATTR in marks
    types_namespace = {
        related.__name__: related for related in related_models(model=model)
    }
    types_namespace[model.__name__] = model
    # A pending model is already mocked, so a plain rebuild regenerates it and
    # can reuse the compiled validators of nested models rebuilt before it.
    try:
        with building_schemas():
            model.model_rebuild(force=not pending, _types_namespace=types_namespace)
    except Exception:
        if pending:
            for attr, value in marks.items():
                setattr(model, attr, value)
            set_rebuild_mocks(model=model)
        raise


def rebuild_pending_models(model: type["Model"]) -> None:
    """
    Rebuilds every pending model connected to the model, each one once with
    its final fields, in the order they were last marked.

    That is the order in which eager rebuilds finished the models, so models
    rebuilt later embed the final schemas of the ones rebuilt before them and
    nested shapes stay close to the ones eager rebuilds produced.

    While ormar builds its own schemas only the given model is rebuilt (if
    pending), the other ones are embedded as snapshots.

    :param model: model whose schema is needed
    :type model: Model class
    """
    if building_schemas_var.get():
        candidates = [model]
    else:
        candidates = connected_models(model=model)
    pending = sorted(
        (
            candidate
            for candidate in candidates
            if PENDING_ATTR in candidate.__dict__
            and not candidate.ormar_config.requires_ref_update
        ),
        key=lambda candidate: candidate.__dict__[PENDING_ATTR],
    )
    for pending_model in pending:
        rebuild_model(model=pending_model)


def schema_snapshot(model: type["Model"]) -> Optional[CoreSchema]:
    """
    Returns the schema the model had before it was marked as pending, used
    while ormar builds schemas of other models.

    :param model: model to check
    :type model: Model class
    :return: schema snapshot or None if model is not pending or had none
    :rtype: Optional[CoreSchema]
    """
    if not building_schemas_var.get():
        return None
    return model.__dict__.get(SNAPSHOT_ATTR)


def mark_for_rebuild(model: type["Model"]) -> None:
    """
    Marks the model as needing a pydantic rebuild instead of rebuilding it.

    Schema, validator and serializer of the model are replaced with mocks that
    rebuild all pending connected models on first access. The previous schema
    is kept as a snapshot for schemas ormar builds until then.

    :param model: model whose fields changed
    :type model: Model class
    """
    if PENDING_ATTR not in model.__dict__:
        schema = model.__dict__.get("__pydantic_core_schema__")
        if schema is not None and not isinstance(schema, MockCoreSchema):
            setattr(model, SNAPSHOT_ATTR, schema)
        set_rebuild_mocks(model=model)
    setattr(model, PENDING_ATTR, next(pending_order))


def set_rebuild_mocks(model: type["Model"]) -> None:
    """
    Replaces pydantic schema, validator and serializer of the model with mocks
    that rebuild pending models on first access.

    :param model: model to mock
    :type model: Model class
    """
    message = (
        f"`{model.__name__}` is not fully defined; you should define all "
        f"referenced types, then call `{model.__name__}.update_forward_refs()`."
    )

    def rebuild_then(getter: Callable[[], T]) -> Callable[..., Optional[T]]:
        def attempt_rebuild(**kwargs: Any) -> Optional[T]:
            """
            Rebuilds pending models and returns the requested attribute.

            :param kwargs: pydantic rebuild options, not used
            :type kwargs: Any
            :return: rebuilt attribute or None if the model is still not built
            :rtype: Optional[T]
            """
            rebuild_pending_models(model=model)
            value = getter()
            return None if isinstance(value, (MockCoreSchema, MockValSer)) else value

        return attempt_rebuild

    model.__pydantic_complete__ = False  # type: ignore[attr-defined]
    model.__pydantic_core_schema__ = MockCoreSchema(  # type: ignore[misc]
        message,
        code="class-not-fully-defined",
        attempt_rebuild=rebuild_then(getter=lambda: model.__pydantic_core_schema__),
    )
    model.__pydantic_validator__ = MockValSer(  # type: ignore[assignment]
        message,
        code="class-not-fully-defined",
        val_or_ser="validator",
        attempt_rebuild=rebuild_then(getter=lambda: model.__pydantic_validator__),
    )
    model.__pydantic_serializer__ = MockValSer(  # type: ignore[assignment]
        message,
        code="class-not-fully-defined",
        val_or_ser="serializer",
        attempt_rebuild=rebuild_then(getter=lambda: model.__pydantic_serializer__),
    )
