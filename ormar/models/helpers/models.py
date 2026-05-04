import itertools
from typing import TYPE_CHECKING, Any, ForwardRef

import pydantic

import ormar  # noqa: I100
from ormar.models.helpers.pydantic import populate_pydantic_default_values

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.fields import BaseField


def is_field_an_forward_ref(field: "BaseField") -> bool:
    """
    Checks if field is a relation field and whether any of the referenced models
    are ForwardRefs that needs to be updated before proceeding.

    :param field: model field to verify
    :type field: type[BaseField]
    :return: result of the check
    :rtype: bool
    """
    return field.is_relation and (
        field.to.__class__ == ForwardRef or field.through.__class__ == ForwardRef
    )


def populate_default_options_values(  # noqa: CCR001
    new_model: type["Model"], model_fields: dict
) -> None:
    """
    Sets all optional OrmarConfig values to its defaults
    and set model_fields that were already previously extracted.

    Here should live all options that are not overwritten/set for all models.

    Current options are:
    * constraints = []
    * abstract = False

    :param new_model: newly constructed Model
    :type new_model: Model class
    :param model_fields: dict of model fields
    :type model_fields: Union[dict[str, type], dict]
    """
    new_model.ormar_config.model_fields.update(model_fields)
    if any(is_field_an_forward_ref(field) for field in model_fields.values()):
        new_model.ormar_config.requires_ref_update = True

    new_model._json_fields = {
        name for name, field in model_fields.items() if field.__type__ == pydantic.Json
    }
    new_model._bytes_fields = {
        name for name, field in model_fields.items() if field.__type__ is bytes
    }
    new_model._onupdate_fields = {
        name for name, field in model_fields.items() if field.has_on_update()
    }

    new_model.__relation_map__ = None
    new_model.__ormar_fields_validators__ = None


def check_required_config_parameters(new_model: type["Model"]) -> None:
    """
    Verifies if ormar.Model has database and metadata set.

    Recreates Connection pool for sqlite3

    :param new_model: newly declared ormar Model
    :type new_model: Model class
    """
    if new_model.ormar_config.proxy and new_model.ormar_config.abstract:
        raise ormar.ModelDefinitionError(
            f"{new_model.__name__} cannot be both proxy and abstract."
        )

    skip_db_metadata_checks = (
        new_model.ormar_config.abstract or new_model.ormar_config.proxy
    )
    if new_model.ormar_config.database is None and not skip_db_metadata_checks:
        raise ormar.ModelDefinitionError(
            f"{new_model.__name__} does not have database defined."
        )

    if new_model.ormar_config.metadata is None and not skip_db_metadata_checks:
        raise ormar.ModelDefinitionError(
            f"{new_model.__name__} does not have metadata defined."
        )


def extract_annotations_and_default_vals(attrs: dict) -> tuple[dict, dict]:
    """
    Extracts annotations from class namespace dict and triggers
    extraction of ormar model_fields.

    :param attrs: namespace of the class created
    :type attrs: dict
    :return: namespace of the class updated, dict of extracted model_fields
    :rtype: tuple[dict, dict]
    """
    key = "__annotations__"
    attrs[key] = attrs.get(key, {})
    attrs, model_fields = populate_pydantic_default_values(attrs)
    return attrs, model_fields


def group_related_list(list_: list) -> dict:
    """
    Translates the list of related strings into a dictionary.
    That way nested models are grouped to traverse them in a right order
    and to avoid repetition.

    Sample: ["people__houses", "people__cars__models", "people__cars__colors"]
    will become:
    {'people': {'houses': [], 'cars': ['models', 'colors']}}

    Result dictionary is sorted by length of the values and by key

    :param list_: list of related models used in select related
    :type list_: list[str]
    :return: list converted to dictionary to avoid repetition and group nested models
    :rtype: dict[str, list]
    """
    result_dict: dict[str, Any] = dict()
    list_.sort(key=lambda x: x.split("__")[0])
    grouped = itertools.groupby(list_, key=lambda x: x.split("__")[0])
    for key, group in grouped:
        group_list = list(group)
        new = sorted(
            ["__".join(x.split("__")[1:]) for x in group_list if len(x.split("__")) > 1]
        )
        if any("__" in x for x in new):
            result_dict[key] = group_related_list(new)
        else:
            result_dict.setdefault(key, []).extend(new)
    return dict(sorted(result_dict.items(), key=lambda item: len(item[1])))


def config_field_not_set(model: type["Model"], field_name: str) -> bool:
    """
    Checks if field with given name is already present in model.OrmarConfig.
    Then check if it's set to something truthful
    (in practice meaning not None, as it's non or ormar Field only).

    :param model: newly constructed model
    :type model: Model class
    :param field_name: name of the ormar field
    :type field_name: str
    :return: result of the check
    :rtype: bool
    """
    return not getattr(model.ormar_config, field_name)
