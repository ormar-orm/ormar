import datetime
import decimal
import uuid
from enum import Enum as E
from enum import EnumMeta
from typing import Any, Optional

import pydantic
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy.sql.base import SchemaEventTarget

import ormar  # noqa I101
from ormar import ModelDefinitionError  # noqa I101
from ormar.fields import sqlalchemy_uuid
from ormar.fields.base import BaseField  # noqa I101
from ormar.fields.sqlalchemy_encrypted import EncryptBackends

try:
    from typing import Self  # type: ignore
except ImportError:  # pragma: no cover
    from typing_extensions import Self  # type: ignore


def is_field_nullable(
    nullable: Optional[bool],
    default: Any,
    server_default: Any,
) -> bool:
    """
    Checks if the given field should be nullable/ optional based on parameters given.

    :param nullable: flag explicit setting a column as nullable
    :type nullable: Optional[bool]
    :param default: value or function to be called as default in python
    :type default: Any
    :param server_default: function to be called as default by sql server
    :type server_default: Any
    :return: result of the check
    :rtype: bool
    """
    if nullable is None:
        return default is not None or server_default is not None
    return nullable


def is_auto_primary_key(primary_key: bool, autoincrement: bool) -> bool:
    """
    Checks if field is an autoincrement pk -> if yes it's optional.

    :param primary_key: flag if field is a pk field
    :type primary_key: bool
    :param autoincrement: flag if field should be autoincrement
    :type autoincrement: bool
    :return: result of the check
    :rtype: bool
    """
    return primary_key and autoincrement


class ModelFieldFactory:
    """
    Default field factory that construct Field classes and populated their values.
    """

    _bases: Any = (BaseField,)
    _type: Any = None
    _sample: Any = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # type: ignore
        cls.validate(**kwargs)

        default = kwargs.pop("default", None)
        server_default = kwargs.pop("server_default", None)
        nullable = kwargs.pop("nullable", None)
        sql_nullable = kwargs.pop("sql_nullable", None)

        primary_key = kwargs.pop("primary_key", False)
        autoincrement_explicit = kwargs.pop(
            "autoincrement_explicit", "autoincrement" in kwargs
        )
        autoincrement = kwargs.pop("autoincrement", False)

        encrypt_secret = kwargs.pop("encrypt_secret", None)
        encrypt_backend = kwargs.pop("encrypt_backend", EncryptBackends.NONE)
        encrypt_custom_backend = kwargs.pop("encrypt_custom_backend", None)

        overwrite_pydantic_type = kwargs.pop("overwrite_pydantic_type", None)

        nullable = is_field_nullable(
            nullable, default, server_default
        ) or is_auto_primary_key(primary_key, autoincrement)
        sql_nullable = (
            False
            if primary_key
            else (nullable if sql_nullable is None else sql_nullable)
        )

        enum_class = kwargs.pop("enum_class", None)
        field_type = cls._type if enum_class is None else enum_class

        namespace = dict(
            __type__=field_type,
            __pydantic_type__=(
                overwrite_pydantic_type
                if overwrite_pydantic_type is not None
                else field_type
            ),
            __sample__=cls._sample,
            alias=kwargs.pop("name", None),
            name=None,
            primary_key=primary_key,
            default=default,
            server_default=server_default,
            nullable=nullable,
            annotation=field_type,
            sql_nullable=sql_nullable,
            index=kwargs.pop("index", False),
            unique=kwargs.pop("unique", False),
            autoincrement=autoincrement,
            autoincrement_explicit=autoincrement_explicit,
            column_type=cls.get_column_type(
                **kwargs, sql_nullable=sql_nullable, enum_class=enum_class
            ),
            encrypt_secret=encrypt_secret,
            encrypt_backend=encrypt_backend,
            encrypt_custom_backend=encrypt_custom_backend,
            **kwargs,
        )
        Field = type(cls.__name__, cls._bases, {})
        return Field(**namespace)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:  # pragma no cover
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return None

    @classmethod
    def validate(cls, **kwargs: Any) -> None:  # pragma no cover
        """
        Used to validate if all required parameters on a given field type are set.
        :param kwargs: all params passed during construction
        :type kwargs: Any
        """


class String(ModelFieldFactory, str):
    """
    String field factory that construct Field classes and populated their values.
    """

    _type = str
    _sample = "string"

    def __new__(  # type: ignore # noqa CFQ002
        cls,
        *,
        max_length: int,
        min_length: Optional[int] = None,
        regex: Optional[str] = None,
        **kwargs: Any,
    ) -> Self:  # type: ignore
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.String(length=kwargs.get("max_length"))

    @classmethod
    def validate(cls, **kwargs: Any) -> None:
        """
        Used to validate if all required parameters on a given field type are set.
        :param kwargs: all params passed during construction
        :type kwargs: Any
        """
        max_length = kwargs.get("max_length", -1)
        if max_length <= 0:
            raise ModelDefinitionError(
                "Parameter max_length is required for field String"
            )


def unsigned_on_mysql(column_type: Any, mysql_type: Any, unsigned: bool) -> Any:
    """
    Return the integer column type, made ``UNSIGNED`` on MySQL and MariaDB when
    ``unsigned`` is set. Other databases have no unsigned integers and keep the
    plain type::

        unsigned_on_mysql(sqlalchemy.Integer(), mysql.INTEGER, True)
        # MySQL: INTEGER UNSIGNED, PostgreSQL / SQLite: INTEGER

    :param column_type: integer column type
    :type column_type: Any
    :param mysql_type: MySQL integer type of the same size
    :type mysql_type: Any
    :param unsigned: whether the column is unsigned
    :type unsigned: bool
    :return: column type
    :rtype: Any
    """
    if not unsigned:
        return column_type
    return column_type.with_variant(mysql_type(unsigned=True), "mysql", "mariadb")


class Integer(ModelFieldFactory, int):
    """
    Integer field factory that construct Field classes and populated their values.
    """

    _type = int
    _sample = 0

    def __new__(  # type: ignore
        cls,
        *,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
        multiple_of: Optional[int] = None,
        unsigned: bool = False,
        **kwargs: Any,
    ) -> Self:
        if unsigned and minimum is None:
            minimum = 0
        autoincrement = kwargs.pop("autoincrement", None)
        autoincrement_explicit = autoincrement is not None
        autoincrement = (
            autoincrement
            if autoincrement is not None
            else kwargs.get("primary_key", False)
        )
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        kwargs["ge"] = kwargs["minimum"]
        kwargs["le"] = kwargs["maximum"]
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return unsigned_on_mysql(
            sqlalchemy.Integer(), mysql.INTEGER, kwargs.get("unsigned", False)
        )


class Text(ModelFieldFactory, str):
    """
    Text field factory that construct Field classes and populated their values.
    """

    _type = str
    _sample = "text"

    def __new__(cls, **kwargs: Any) -> Self:  # type: ignore
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.Text()


class Float(ModelFieldFactory, float):
    """
    Float field factory that construct Field classes and populated their values.
    """

    _type = float
    _sample = 0.0

    def __new__(  # type: ignore
        cls,
        *,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        multiple_of: Optional[int] = None,
        **kwargs: Any,
    ) -> Self:
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        kwargs["ge"] = kwargs["minimum"]
        kwargs["le"] = kwargs["maximum"]
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.Float()


class Boolean(ModelFieldFactory, int):
    """
    Boolean field factory that construct Field classes and populated their values.
    """

    _type = bool
    _sample = True

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.Boolean()


class DateTime(ModelFieldFactory, datetime.datetime):
    """
    DateTime field factory that construct Field classes and populated their values.
    """

    _type = datetime.datetime
    _sample = "datetime"

    def __new__(  # type: ignore # noqa CFQ002
        cls, *, timezone: bool = False, **kwargs: Any
    ) -> Self:  # type: ignore
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.DateTime(timezone=kwargs.get("timezone", False))


class Date(ModelFieldFactory, datetime.date):
    """
    Date field factory that construct Field classes and populated their values.
    """

    _type = datetime.date
    _sample = "date"

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.Date()


class Time(ModelFieldFactory, datetime.time):
    """
    Time field factory that construct Field classes and populated their values.
    """

    _type = datetime.time
    _sample = "time"

    def __new__(  # type: ignore # noqa CFQ002
        cls, *, timezone: bool = False, **kwargs: Any
    ) -> Self:  # type: ignore
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.Time(timezone=kwargs.get("timezone", False))


class JSON(ModelFieldFactory, pydantic.Json):
    """
    JSON field factory that construct Field classes and populated their values.
    """

    _type = pydantic.Json
    _sample = '{"json": "json"}'

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.JSON(none_as_null=kwargs.get("sql_nullable", False))


class LargeBinary(ModelFieldFactory, bytes):
    """
    LargeBinary field factory that construct Field classes
    and populated their values.
    """

    _type = bytes
    _sample = "bytes"

    def __new__(  # type: ignore # noqa CFQ002
        cls, *, max_length: int, represent_as_base64_str: bool = False, **kwargs: Any
    ) -> Self:  # type: ignore
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return sqlalchemy.LargeBinary(length=kwargs.get("max_length"))

    @classmethod
    def validate(cls, **kwargs: Any) -> None:
        """
        Used to validate if all required parameters on a given field type are set.
        :param kwargs: all params passed during construction
        :type kwargs: Any
        """
        max_length = kwargs.get("max_length", None)
        if max_length <= 0:
            raise ModelDefinitionError(
                "Parameter max_length is required for field LargeBinary"
            )


class BigInteger(Integer, int):
    """
    BigInteger field factory that construct Field classes and populated their values.
    """

    _type = int
    _sample = 0

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return unsigned_on_mysql(
            sqlalchemy.BigInteger(), mysql.BIGINT, kwargs.get("unsigned", False)
        )


class SmallInteger(Integer, int):
    """
    SmallInteger field factory that construct Field classes and populated their values.
    """

    _type = int
    _sample = 0

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        return unsigned_on_mysql(
            sqlalchemy.SmallInteger(), mysql.SMALLINT, kwargs.get("unsigned", False)
        )


class Decimal(ModelFieldFactory, decimal.Decimal):
    """
    Decimal field factory that construct Field classes and populated their values.
    """

    _type = decimal.Decimal
    _sample = 0.0

    def __new__(  # type: ignore # noqa CFQ002
        cls,
        *,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        multiple_of: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        max_digits: Optional[int] = None,
        decimal_places: Optional[int] = None,
        **kwargs: Any,
    ) -> Self:
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        kwargs["ge"] = kwargs["minimum"]
        kwargs["le"] = kwargs["maximum"]

        if kwargs.get("max_digits"):
            kwargs["precision"] = kwargs["max_digits"]
        elif kwargs.get("precision"):
            kwargs["max_digits"] = kwargs["precision"]

        if kwargs.get("decimal_places"):
            kwargs["scale"] = kwargs["decimal_places"]
        elif kwargs.get("scale"):
            kwargs["decimal_places"] = kwargs["scale"]

        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        precision = kwargs.get("precision")
        scale = kwargs.get("scale")
        return sqlalchemy.DECIMAL(precision=precision, scale=scale)

    @classmethod
    def validate(cls, **kwargs: Any) -> None:
        """
        Used to validate if all required parameters on a given field type are set.
        :param kwargs: all params passed during construction
        :type kwargs: Any
        """
        precision = kwargs.get("precision")
        scale = kwargs.get("scale")
        if precision is None or precision < 0 or scale is None or scale < 0:
            raise ModelDefinitionError(
                "Parameters scale and precision are required for field Decimal"
            )


class UUID(ModelFieldFactory, uuid.UUID):
    """
    UUID field factory that construct Field classes and populated their values.
    """

    _type = uuid.UUID
    _sample = "uuid"

    def __new__(  # type: ignore # noqa CFQ002
        cls, *, uuid_format: str = "hex", **kwargs: Any
    ) -> Self:
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }

        return super().__new__(cls, **kwargs)

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return proper type of db column for given field type.
        Accepts required and optional parameters that each column type accepts.

        :param kwargs: key, value pairs of sqlalchemy options
        :type kwargs: Any
        :return: initialized column with proper options
        :rtype: sqlalchemy Column
        """
        uuid_format = kwargs.get("uuid_format", "hex")
        return sqlalchemy_uuid.UUID(uuid_format=uuid_format)


# ``name`` is not forwarded: ormar uses it for the column name
_SQLALCHEMY_ENUM_KWARGS = frozenset(
    {
        "native_enum",
        "create_constraint",
        "values_callable",
        "validate_strings",
        "length",
        "schema",
        "metadata",
        "inherit_schema",
        "quote",
    }
)


class IntegerEnum(sqlalchemy.types.TypeDecorator, SchemaEventTarget):
    """
    Integer column of an ``int`` enum: stores the member's value and loads it
    back as the member, with a CHECK constraint limiting the column to the
    enum's values::

        country INTEGER CHECK (country IN (1, 2))
    """

    impl = sqlalchemy.Integer
    cache_ok = True

    def __init__(self, enum_class: type[E]) -> None:
        super().__init__()
        self.enum_class = enum_class

    def _set_parent(self, parent: Any, **kwargs: Any) -> None:
        sqlalchemy.event.listen(parent, "after_parent_attach", self._add_check)

    def _add_check(self, column: sqlalchemy.Column, table: sqlalchemy.Table) -> None:
        values = [int(member) for member in self.enum_class]
        table.append_constraint(sqlalchemy.CheckConstraint(column.in_(values)))

    def process_bind_param(self, value: Any, dialect: Any) -> Optional[int]:
        return None if value is None else int(value)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        return None if value is None else self.enum_class(value)


class Enum(ModelFieldFactory):
    """
    Enum field factory that construct Field classes and populated their values.
    """

    _type = E
    _sample = None

    def __new__(  # type: ignore # noqa CFQ002
        cls, *, enum_class: type[E], store_values: bool = False, **kwargs: Any
    ) -> Self:
        kwargs = {
            **kwargs,
            **{
                k: v
                for k, v in locals().items()
                if k not in ["cls", "__class__", "kwargs"]
            },
        }
        return super().__new__(cls, **kwargs)

    @classmethod
    def validate(cls, **kwargs: Any) -> None:
        enum_class = kwargs.get("enum_class")
        if enum_class is None or not isinstance(enum_class, EnumMeta):
            raise ModelDefinitionError("Enum Field choices must be EnumType")
        if kwargs.get("store_values") and kwargs.get("values_callable"):
            raise ModelDefinitionError(
                "Enum Field accepts store_values or values_callable, not both"
            )

    @classmethod
    def get_column_type(cls, **kwargs: Any) -> Any:
        """
        Return the column type of the enum.

        SQLAlchemy Enum options, such as ``values_callable``, are forwarded
        so callers can store enum values instead of enum names.

        With ``store_values=True`` the column stores member values: an ``int``
        enum gets an ``INTEGER`` column (see ``IntegerEnum``), any other enum
        a ``sqlalchemy.Enum`` of its values as strings.
        """
        enum_cls = kwargs.get("enum_class")
        if kwargs.get("store_values") and issubclass(enum_cls, int):
            return IntegerEnum(enum_cls)
        enum_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in _SQLALCHEMY_ENUM_KWARGS
        }
        if kwargs.get("store_values"):
            enum_kwargs["values_callable"] = lambda enum: [
                str(member.value) for member in enum
            ]
        return sqlalchemy.Enum(enum_cls, **enum_kwargs)
