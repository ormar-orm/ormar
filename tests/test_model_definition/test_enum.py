from enum import Enum

import ormar
from tests.settings import create_config

base_ormar_config = create_config()


class IntCountry(int, Enum):
    UK = 1
    US = 2


class IntCountryModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="int_country")

    id: int = ormar.Integer(primary_key=True)
    country: IntCountry = ormar.Enum(
        enum_class=IntCountry,
        values_callable=lambda enum_class: [str(member.value) for member in enum_class],
        length=1,
    )


def test_enum_values_callable_uses_enum_values():
    field = IntCountryModel.ormar_config.model_fields["country"]
    assert field.column_type.enums == ["1", "2"]
