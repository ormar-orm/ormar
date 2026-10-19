# inspired by sqlalchemy-utils (https://github.com/kvesteri/sqlalchemy-utils)
import abc
import base64
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

import sqlalchemy.types as types
from sqlalchemy.engine import Dialect

import ormar  # noqa: I100, I202
from ormar import ModelDefinitionError  # noqa: I202, I100
from ormar.fields.parsers import ADDITIONAL_PARAMETERS_MAP

cryptography = None
try:  # pragma: nocover
    import cryptography  # type: ignore
    from cryptography.fernet import Fernet
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
except ImportError:  # pragma: nocover
    pass

if TYPE_CHECKING:  # pragma: nocover
    from ormar import BaseField


class EncryptBackend(abc.ABC):
    def _refresh(self, key: Union[str, bytes]) -> None:
        if isinstance(key, str):
            key = key.encode()
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(key)
        engine_key = digest.finalize()
        self._initialize_backend(engine_key)

    @abc.abstractmethod
    def _initialize_backend(self, secret_key: bytes) -> None:  # pragma: nocover
        pass

    @abc.abstractmethod
    def encrypt(self, value: Any) -> str:  # pragma: nocover
        pass

    @abc.abstractmethod
    def decrypt(self, value: Any) -> str:  # pragma: nocover
        pass


class HashBackend(EncryptBackend):
    """
    One-way hashing - in example for passwords, no way to decrypt the value!
    """

    def _initialize_backend(self, secret_key: bytes) -> None:
        self.secret_key = base64.urlsafe_b64encode(secret_key)

    def encrypt(self, value: Any) -> str:
        if not isinstance(value, str):  # pragma: nocover
            value = repr(value)
        value = value.encode()
        digest = hashes.Hash(hashes.SHA512(), backend=default_backend())
        digest.update(self.secret_key)
        digest.update(value)
        hashed_value = digest.finalize()
        return hashed_value.hex()

    def decrypt(self, value: Any) -> str:
        if not isinstance(value, str):  # pragma: nocover
            value = str(value)
        return value


class FernetBackend(EncryptBackend):
    """
    Two-way encryption, data stored in db are encrypted but decrypted during query.
    """

    def _initialize_backend(self, secret_key: bytes) -> None:
        self.secret_key = base64.urlsafe_b64encode(secret_key)
        self.fernet = Fernet(self.secret_key)

    def encrypt(self, value: Any) -> str:
        if not isinstance(value, str):
            value = repr(value)
        value = value.encode()
        encrypted = self.fernet.encrypt(value)
        return encrypted.decode("utf-8")

    def decrypt(self, value: Any) -> str:
        if not isinstance(value, str):  # pragma: nocover
            value = str(value)
        decrypted: Union[str, bytes] = self.fernet.decrypt(value.encode())
        if not isinstance(decrypted, str):
            decrypted = decrypted.decode("utf-8")
        return decrypted


class EncryptBackends(Enum):
    NONE = 0
    FERNET = 1
    HASH = 2
    CUSTOM = 3


BACKENDS_MAP = {
    EncryptBackends.FERNET: FernetBackend,
    EncryptBackends.HASH: HashBackend,
}


class EncryptedString(types.TypeDecorator):
    """
    Used to store encrypted values in a database
    """

    impl = types.TypeEngine

    cache_ok = True

    def __init__(
        self,
        encrypt_secret: Union[str, Callable],
        encrypt_backend: EncryptBackends = EncryptBackends.FERNET,
        encrypt_custom_backend: Optional[type[EncryptBackend]] = None,
        **kwargs: Any,
    ) -> None:
        _field_type = kwargs.pop("_field_type")
        super().__init__()
        if not cryptography:  # pragma: nocover
            raise ModelDefinitionError(
                "In order to encrypt a column 'cryptography' is required!"
            )
        backend = BACKENDS_MAP.get(encrypt_backend, encrypt_custom_backend)
        if (
            not backend
            or not isinstance(backend, type)
            or not issubclass(backend, EncryptBackend)
        ):
            raise ModelDefinitionError("Wrong or no encrypt backend provided!")

        self.backend: EncryptBackend = backend()
        self._field_type: "BaseField" = _field_type
        self._underlying_type: Any = _field_type.column_type
        self._key: Union[str, Callable] = encrypt_secret
        type_ = self._field_type.__type__
        if type_ is None:  # pragma: nocover
            raise ModelDefinitionError(
                f"Improperly configured field {self._field_type.name}"
            )
        self.type_: Any = type_

    def __repr__(self) -> str:  # pragma: nocover
        return "TEXT()"

    def load_dialect_impl(self, dialect: Dialect) -> Any:
        return dialect.type_descriptor(types.TEXT())

    def _refresh(self) -> None:
        key = self._key() if callable(self._key) else self._key
        self.backend._refresh(key)

    def process_bind_param(self, value: Any, dialect: Dialect) -> Optional[str]:
        if value is None:
            return value
        self._refresh()
        if isinstance(value, Enum):
            value = value.name
        try:
            value = self._underlying_type.process_bind_param(value, dialect)
        except AttributeError:
            encoder, additional_parameter = self._get_coder_type_and_params(
                coders=ormar.SQL_ENCODERS_MAP
            )
            if encoder is not None:
                params = [value] + (
                    [additional_parameter] if additional_parameter else []
                )
                value = encoder(*params)

        encrypted_value = self.backend.encrypt(value)
        return encrypted_value

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:
        if value is None:  # pragma: no cover
            return value
        self._refresh()
        decrypted_value = self.backend.decrypt(value)
        try:
            return self._underlying_type.process_result_value(decrypted_value, dialect)
        except AttributeError:
            decoder, additional_parameter = self._get_coder_type_and_params(
                coders=ormar.DECODERS_MAP
            )
            if decoder is not None:
                params = [decrypted_value] + (
                    [additional_parameter] if additional_parameter else []
                )
                return decoder(*params)  # type: ignore
            if issubclass(self.type_, Enum):
                return self.type_[decrypted_value]

            return self._field_type.__type__(decrypted_value)  # type: ignore

    def _get_coder_type_and_params(
        self, coders: dict[type, Callable]
    ) -> tuple[Optional[Callable], Optional[str]]:
        coder = coders.get(self.type_, None)
        additional_parameter: Optional[str] = None
        if self.type_ in ADDITIONAL_PARAMETERS_MAP:
            additional_parameter = getattr(
                self._field_type, ADDITIONAL_PARAMETERS_MAP[self.type_]
            )
        return coder, additional_parameter
