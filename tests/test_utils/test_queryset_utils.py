from typing import Optional

import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.order_action import OrderAction
from ormar.queryset.utils import (
    SliceBounds,
    normalize_slice,
    subtract_dict,
    translate_list_to_dict,
    update,
    update_dict_from_list,
)
from tests.settings import create_config

base_ormar_config = create_config()


class _SliceModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="slice_utils_model")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=50, nullable=True)


def test_normalize_slice_positive_int_and_slice():
    assert normalize_slice(5) == SliceBounds(limit=1, offset=5, reverse=False)
    assert normalize_slice(slice(2, 8)) == SliceBounds(limit=6, offset=2, reverse=False)
    assert normalize_slice(slice(None, 8)) == SliceBounds(
        limit=8, offset=0, reverse=False
    )
    assert normalize_slice(slice(2, None)) == SliceBounds(
        limit=None, offset=2, reverse=False
    )
    assert normalize_slice(slice(None, None)) == SliceBounds(
        limit=None, offset=0, reverse=False
    )
    assert normalize_slice(slice(8, 2)) == SliceBounds(limit=0, offset=8, reverse=False)


def test_normalize_slice_negative_int_and_slice():
    assert normalize_slice(-1) == SliceBounds(limit=1, offset=0, reverse=True)
    assert normalize_slice(-3) == SliceBounds(limit=1, offset=2, reverse=True)
    assert normalize_slice(slice(-3, None)) == SliceBounds(
        limit=3, offset=0, reverse=True
    )
    assert normalize_slice(slice(-5, -2)) == SliceBounds(
        limit=3, offset=2, reverse=True
    )
    assert normalize_slice(slice(-2, -5)) == SliceBounds(
        limit=0, offset=0, reverse=True
    )


def test_normalize_slice_rejects_invalid_input():
    with pytest.raises(QueryDefinitionError):
        normalize_slice("five")  # type: ignore[arg-type]

    with pytest.raises(QueryDefinitionError):
        normalize_slice(slice(None, None, 2))

    with pytest.raises(QueryDefinitionError):
        normalize_slice(slice(None, -5))

    with pytest.raises(QueryDefinitionError):
        normalize_slice(slice(3, -5))

    with pytest.raises(QueryDefinitionError):
        normalize_slice(slice(-3, 5))


def test_order_action_flipped_direction_and_nulls():
    asc_first = OrderAction(
        order_str="name",
        model_cls=_SliceModel,
        nulls_ordering="first",
    )
    desc_last = asc_first.flipped()
    assert desc_last.direction == "desc"
    assert desc_last.nulls_ordering == "last"

    desc_first = OrderAction(
        order_str="-name",
        model_cls=_SliceModel,
        nulls_ordering="last",
    )
    asc_last = desc_first.flipped()
    assert asc_last.direction == ""
    assert asc_last.nulls_ordering == "first"

    plain = OrderAction(order_str="name", model_cls=_SliceModel)
    assert plain.flipped().nulls_ordering is None


def test_list_to_dict_translation():
    tet_list = ["aa", "bb", "cc__aa", "cc__bb", "cc__aa__xx", "cc__aa__yy"]
    test = translate_list_to_dict(tet_list)
    assert test == {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx": Ellipsis, "yy": Ellipsis}, "bb": Ellipsis},
    }


def test_list_to_dict_translation_with_default():
    tet_list = ["aa", "aa__inner", "bb"]
    testshallow = translate_list_to_dict(tet_list, default={})
    assert testshallow == {"aa": {"inner": {}}, "bb": {}}

    tet_list = ["aa", "aa__inner", "bb"]
    testdeep = translate_list_to_dict(tet_list, default={"foo": {}})
    assert testdeep == {"aa": {"foo": {}, "inner": {"foo": {}}}, "bb": {"foo": {}}}
    testdeep["aa"]["foo"]["bar"] = 1234
    assert testdeep == {
        "aa": {"foo": {"bar": 1234}, "inner": {"foo": {}}},
        "bb": {"foo": {}},
    }


def test_updating_dict_with_list():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx": Ellipsis, "yy": Ellipsis}, "bb": Ellipsis},
    }
    list_to_update = ["ee", "bb__cc", "cc__aa__xx__oo", "cc__aa__oo"]
    test = update_dict_from_list(curr_dict, list_to_update)
    assert test == {
        "aa": Ellipsis,
        "bb": {"cc": Ellipsis},
        "cc": {
            "aa": {"xx": {"oo": Ellipsis}, "yy": Ellipsis, "oo": Ellipsis},
            "bb": Ellipsis,
        },
        "ee": Ellipsis,
    }


def test_updating_dict_inc_set_with_list():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx", "yy"}, "bb": Ellipsis},
    }
    list_to_update = ["uu", "bb__cc", "cc__aa__xx__oo", "cc__aa__oo"]
    test = update_dict_from_list(curr_dict, list_to_update)
    assert test == {
        "aa": Ellipsis,
        "bb": {"cc": Ellipsis},
        "cc": {
            "aa": {"xx": {"oo": Ellipsis}, "yy": Ellipsis, "oo": Ellipsis},
            "bb": Ellipsis,
        },
        "uu": Ellipsis,
    }


def test_updating_dict_inc_set_with_dict():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx", "yy"}, "bb": Ellipsis},
    }
    dict_to_update = {
        "uu": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {"aa": {"xx": {"oo": Ellipsis}, "oo": Ellipsis}},
    }
    test = update(curr_dict, dict_to_update)
    assert test == {
        "aa": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {
            "aa": {"xx": {"oo": Ellipsis}, "yy": Ellipsis, "oo": Ellipsis},
            "bb": Ellipsis,
        },
        "uu": Ellipsis,
    }


def test_subtracting_dict_inc_set_with_dict():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx", "yy"}, "bb": Ellipsis},
    }
    dict_to_update = {
        "uu": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {"aa": {"xx": {"oo": Ellipsis}}, "bb": Ellipsis},
    }
    test = subtract_dict(curr_dict, dict_to_update)
    assert test == {"aa": Ellipsis, "cc": {"aa": {"yy": Ellipsis}}}


def test_updating_dict_inc_set_with_dict_inc_set():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx", "yy"}, "bb": Ellipsis},
    }
    dict_to_update = {
        "uu": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {"aa": {"xx", "oo", "zz", "ii"}},
    }
    test = update(curr_dict, dict_to_update)
    assert test == {
        "aa": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {"aa": {"xx", "yy", "oo", "zz", "ii"}, "bb": Ellipsis},
        "uu": Ellipsis,
    }


def test_subtracting_dict_inc_set_with_dict_inc_set():
    curr_dict = {
        "aa": Ellipsis,
        "bb": Ellipsis,
        "cc": {"aa": {"xx", "yy"}, "bb": Ellipsis},
        "dd": {"aa", "bb"},
    }
    dict_to_update = {
        "aa": Ellipsis,
        "bb": {"cc", "dd"},
        "cc": {"aa": {"xx", "oo", "zz", "ii"}},
        "dd": {"aa", "bb"},
    }
    test = subtract_dict(curr_dict, dict_to_update)
    assert test == {"cc": {"aa": {"yy"}, "bb": Ellipsis}}


def test_subtracting_with_set_and_dict():
    curr_dict = {
        "translation": {
            "filters": {
                "values": Ellipsis,
                "reports": {"report": {"charts": {"chart": Ellipsis}}},
            },
            "translations": {"language": Ellipsis},
            "filtervalues": {
                "filter": {"reports": {"report": {"charts": {"chart": Ellipsis}}}}
            },
        },
        "chart": {
            "reports": {
                "report": {
                    "filters": {
                        "filter": {
                            "translation": {
                                "translations": {"language": Ellipsis},
                                "filtervalues": Ellipsis,
                            },
                            "values": {
                                "translation": {"translations": {"language": Ellipsis}}
                            },
                        }
                    }
                }
            }
        },
    }
    dict_to_update = {
        "chart": Ellipsis,
        "translation": {"filters", "filtervalues", "chartcolumns"},
    }
    test = subtract_dict(curr_dict, dict_to_update)
    assert test == {"translation": {"translations": {"language": Ellipsis}}}


def test_subtracting_dict_does_not_modify_the_input():
    curr_dict = {"aa": {"bb": Ellipsis, "cc": {"dd": Ellipsis}}, "ee": Ellipsis}
    snapshot = {"aa": {"bb": Ellipsis, "cc": {"dd": Ellipsis}}, "ee": Ellipsis}
    test = subtract_dict(
        current_dict=curr_dict,
        updating_dict={"aa": {"cc": {"dd": Ellipsis}}, "ee": Ellipsis},
    )
    assert test == {"aa": {"bb": Ellipsis}}
    assert curr_dict == snapshot
