"""
Prototype of ``get_pydantic()`` with named, reusable, recursive pydantic models.

A schema is built per variant of a model: the model with the relation back to
its parent left out (``Book_without_author`` below ``Author.books``), plus the
include/exclude/flatten selector still in scope and the remaining depth. A
relation points to the variant of its target by a forward reference, so cycles
become recursive models (``$ref`` in the JSON schema) instead of a copy of the
schema per path, and the number of models grows with the (model, relation)
pairs, not with the paths.
"""

import sys
import types
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ForwardRef, Optional, Union

import pydantic

from ormar.models.excludable import (
    FlattenMap,
    filter_not_excluded_fields,
    normalize_to_dict,
    skip_ellipsis,
)
from ormar.models.relation_graph import RelationGraph

if TYPE_CHECKING:  # pragma: no cover
    from ormar.fields import ForeignKeyField
    from ormar.models.mixins.pydantic_mixin import PydanticMixin

# generated models live in one module, so their forward references resolve
# whenever pydantic builds them, also lazily long after ``get_pydantic``
SCHEMA_MODULE = types.ModuleType("ormar.generated_schemas")
sys.modules[SCHEMA_MODULE.__name__] = SCHEMA_MODULE
# building a schema walks the cycle of variants depth first in pydantic
BUILD_RECURSION_LIMIT = 20000


@dataclass(frozen=True)
class SchemaVariant:
    """
    Identifies one generated pydantic model.
    """

    model: type["PydanticMixin"]
    omitted_relation: Optional[str]
    include: str
    exclude: str
    flatten: str
    remaining_depth: Optional[int]
    nested: bool
    omit_back_relations: bool


class RecursiveSchemaBuilder:
    """
    Builds the pydantic models of ``get_pydantic()`` as recursive models::

        RecursiveSchemaBuilder().build(model=Author)  # -> Author model
    """

    built: dict[tuple[int, SchemaVariant], type[pydantic.BaseModel]] = {}
    used_names: set[str] = set()

    def __init__(self, omit_back_relations: bool = True) -> None:
        """
        :param omit_back_relations: leave the relation back to the parent out
            of a nested model; False gives one nested model per model, with
            every relation optional
        :type omit_back_relations: bool
        """
        self.omit_back_relations = omit_back_relations
        self.names: dict[SchemaVariant, str] = {}

    def build(
        self,
        model: type["PydanticMixin"],
        include: Union[set, dict, None] = None,
        exclude: Union[set, dict, None] = None,
        flatten_map: Optional[FlattenMap] = None,
        max_depth: Optional[int] = None,
    ) -> type[pydantic.BaseModel]:
        """
        Returns the root pydantic model of a model, building every variant it
        needs that was not built before.

        :param model: ormar model to convert
        :type model: type[PydanticMixin]
        :param include: fields of own and nested models to include
        :type include: Union[set, dict, None]
        :param exclude: fields of own and nested models to exclude
        :type exclude: Union[set, dict, None]
        :param flatten_map: relations to represent as their primary key
        :type flatten_map: Optional[FlattenMap]
        :param max_depth: number of relation levels to nest at most
        :type max_depth: Optional[int]
        :return: pydantic model
        :rtype: type[pydantic.BaseModel]
        """
        root_name = self.variant_name(
            variant=self.variant(
                model=model,
                include=include,
                exclude=exclude,
                flatten_map=flatten_map,
                omitted_relation=None,
                remaining_depth=max_depth,
                nested=False,
            ),
            include=include,
            exclude=exclude,
            flatten_map=flatten_map,
        )
        root = getattr(SCHEMA_MODULE, root_name)
        if not root.__pydantic_complete__:
            recursion_limit = sys.getrecursionlimit()
            sys.setrecursionlimit(max(recursion_limit, BUILD_RECURSION_LIMIT))
            try:
                root.model_rebuild(force=True)
            finally:
                sys.setrecursionlimit(recursion_limit)
        return root

    def variant(  # noqa: CFQ002
        self,
        model: type["PydanticMixin"],
        include: Union[set, dict, None],
        exclude: Union[set, dict, None],
        flatten_map: Optional[FlattenMap],
        omitted_relation: Optional[str],
        remaining_depth: Optional[int],
        nested: bool,
    ) -> SchemaVariant:
        """
        Returns the key of one generated model.

        :return: variant key
        :rtype: SchemaVariant
        """
        return SchemaVariant(
            model=model,
            # without relations below it the omitted one makes no difference
            omitted_relation=omitted_relation if remaining_depth != 0 else None,
            include=str(normalize_to_dict(include) or {}),
            exclude=str(normalize_to_dict(exclude) or {}),
            flatten=str(flatten_map),
            remaining_depth=remaining_depth,
            nested=nested,
            omit_back_relations=self.omit_back_relations,
        )

    def variant_name(
        self,
        variant: SchemaVariant,
        include: Union[set, dict, None],
        exclude: Union[set, dict, None],
        flatten_map: Optional[FlattenMap],
    ) -> str:
        """
        Returns the class name of a variant, building the variant on first use.
        A variant that is being built is referenced by its name only, which
        closes the cycle.

        :return: class name of the variant
        :rtype: str
        """
        name = self.names.get(variant)
        if name is not None:
            return name
        cached = self.built.get((RelationGraph.version, variant))
        if cached is not None:
            self.names[variant] = cached.__name__
            return cached.__name__
        model = variant.model
        suffix = (
            f"_without_{variant.omitted_relation}" if variant.omitted_relation else ""
        )
        name = self.unique_name(base_name=f"{model.__name__}{suffix}")
        self.names[variant] = name
        created_model = self.create_model(
            variant=variant,
            name=name,
            include=normalize_to_dict(include),
            exclude=normalize_to_dict(exclude),
            flatten_map=flatten_map,
        )
        setattr(SCHEMA_MODULE, name, created_model)
        self.built[(RelationGraph.version, variant)] = created_model
        return name

    def unique_name(self, base_name: str) -> str:
        """
        Returns a class name not used by another generated model, so the
        OpenAPI components keep readable names (``Book_without_author``,
        ``Book_without_author_2`` for a variant with another selector).

        :param base_name: model name with the omitted relation
        :type base_name: str
        :return: unique class name
        :rtype: str
        """
        name = base_name
        counter = 1
        while name in self.used_names:
            counter += 1
            name = f"{base_name}_{counter}"
        self.used_names.add(name)
        return name

    def create_model(
        self,
        variant: SchemaVariant,
        name: str,
        include: Optional[dict],
        exclude: Optional[dict],
        flatten_map: Optional[FlattenMap],
    ) -> type[pydantic.BaseModel]:
        """
        Creates the pydantic class of a variant with forward references to the
        variants of its relations.

        :return: pydantic model with a deferred build
        :rtype: type[pydantic.BaseModel]
        """
        model = variant.model
        fields_dict: dict[str, Any] = {}
        defaults: dict[str, Any] = {}
        field_order = list(model.ormar_config.model_fields.keys())
        names = filter_not_excluded_fields(
            fields=set(field_order), include=include, exclude=exclude
        )
        names.sort(key=field_order.index)
        for field_name in names:
            field = model.ormar_config.model_fields[field_name]
            if field.is_through or field.skip_field:
                continue
            if not field.is_relation:
                fields_dict[field_name] = model._determine_pydantic_field_type(
                    name=field_name,
                    defaults=defaults,
                    include=include,
                    exclude=exclude,
                    flatten_map=None,
                    relation_map={},
                )
                continue
            target = self.relation_type(
                variant=variant,
                field=field,  # type: ignore[arg-type]
                include=include,
                exclude=exclude,
                flatten_map=flatten_map,
                defaults=defaults,
            )
            if target is not None:
                fields_dict[field_name] = target
        created = type(
            name,
            (pydantic.BaseModel,),
            {
                "__annotations__": fields_dict,
                "__module__": SCHEMA_MODULE.__name__,
                "model_config": pydantic.ConfigDict(defer_build=True),
                **defaults,
            },
        )
        model._copy_computed_fields(model=created, include=include, exclude=exclude)
        model._copy_field_validators(model=created, rebuild=False)
        return created

    def relation_type(  # noqa: CFQ002
        self,
        variant: SchemaVariant,
        field: "ForeignKeyField",
        include: Optional[dict],
        exclude: Optional[dict],
        flatten_map: Optional[FlattenMap],
        defaults: dict,
    ) -> Any:
        """
        Returns the annotation of a relation field: the pk type of a flattened
        relation or a forward reference to the variant of the target, left out
        for the relation back to the parent and beyond ``max_depth``. Nested
        relations are optional, as a dump stops where the target model is
        already on the path.

        :return: annotation or None when the relation is left out
        :rtype: Any
        """
        name = field.name
        model = variant.model
        if name == variant.omitted_relation or variant.remaining_depth == 0:
            return None
        if field.has_unresolved_forward_refs():
            return None
        if flatten_map is not None and flatten_map.is_field_flattened(name):
            flat_target = model._determine_flattened_relation_field_type(
                name=name, field=field, defaults=defaults
            )
            return Optional[flat_target] if field.nullable else flat_target
        nested_include = skip_ellipsis(include, name)
        nested_exclude = skip_ellipsis(exclude, name)
        nested_flatten = flatten_map.descend(name) if flatten_map else None
        child_name = self.variant_name(
            variant=self.variant(
                model=field.to,
                include=nested_include,
                exclude=nested_exclude,
                flatten_map=nested_flatten,
                omitted_relation=(
                    field.get_related_name() if self.omit_back_relations else None
                ),
                remaining_depth=(
                    variant.remaining_depth - 1
                    if variant.remaining_depth is not None
                    else None
                ),
                nested=True,
            ),
            include=nested_include,
            exclude=nested_exclude,
            flatten_map=nested_flatten,
        )
        target: Any = ForwardRef(child_name)
        if field.kind.many:
            target = list[target]  # type: ignore[valid-type]
        if field.nullable or variant.nested:
            defaults[name] = None
            target = Optional[target]
        return target
