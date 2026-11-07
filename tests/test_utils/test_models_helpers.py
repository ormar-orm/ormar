from ormar.models.helpers.models import group_related_list


def test_group_related_list():
    given = [
        "friends__least_favourite_game",
        "least_favourite_game",
        "friends",
        "favourite_game",
        "friends__favourite_game",
    ]
    expected = {
        "least_favourite_game": [],
        "favourite_game": [],
        "friends": ["favourite_game", "least_favourite_game"],
    }
    assert group_related_list(given) == expected
