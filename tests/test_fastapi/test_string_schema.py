from typing import Literal

import ormar

from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class StringChoicesExample(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="string_choices_example")

    id: int = ormar.Integer(primary_key=True)
    mode: Literal["user", "manager", "admin"] = ormar.String(
        max_length=32, choices=["user", "manager", "admin"]
    )
    optional_mode: Literal["user", "manager", "admin"] | None = ormar.String(
        max_length=32,
        choices=["user", "manager", "admin"],
        nullable=True,
    )


create_test_database = init_tests(base_ormar_config)


def test_string_choices_schema():
    schema = StringChoicesExample.model_json_schema()

    assert schema["properties"]["mode"] == {
        "enum": ["user", "manager", "admin"],
        "maxLength": 32,
        "title": "Mode",
        "type": "string",
    }
    assert schema["properties"]["optional_mode"] == {
        "anyOf": [
            {
                "enum": ["user", "manager", "admin"],
                "maxLength": 32,
                "type": "string",
            },
            {"type": "null"},
        ],
        "default": None,
        "title": "Optional Mode",
    }
