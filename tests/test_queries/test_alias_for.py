from ormar.relations.aliases import alias_for


def test_root_path_has_no_alias():
    assert alias_for("") == ""


def test_alias_is_stable_and_path_specific():
    assert alias_for("aseller__city") == alias_for("aseller__city")
    assert alias_for("aseller__city") != alias_for("zbuyer__city")


def test_alias_is_a_short_identifier_without_underscores():
    alias = alias_for("a__very__long__relation__path__that__keeps__going")
    assert len(alias) == 7
    assert alias.startswith("t")
    assert "_" not in alias
    int(alias[1:], 16)
