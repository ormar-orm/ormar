# Adopted from pydantic
from typing import Optional


class OrmarDeprecationWarning(DeprecationWarning):
    """A Pydantic specific deprecation warning.

    This warning is raised when using deprecated functionality in Ormar.
    It provides information on when the deprecation was introduced and
    the expected version in which the corresponding functionality will be removed.

    Attributes:
        message: Description of the warning
        since: Ormar version in what the deprecation was introduced
        expected_removal: Ormar version in what the functionality will be removed
    """

    message: str
    since: tuple[int, int]
    expected_removal: tuple[int, int]

    def __init__(
        self,
        message: str,
        *args: object,
        since: tuple[int, int],
        expected_removal: Optional[tuple[int, int]] = None,
    ) -> None:  # pragma: no cover
        super().__init__(message, *args)
        self.message = message.rstrip(".")
        self.since = since
        self.expected_removal = (
            expected_removal if expected_removal is not None else (since[0] + 1, 0)
        )

    def __str__(self) -> str:  # pragma: no cover
        message = (
            f"{self.message}. Deprecated in Ormar V{self.since[0]}.{self.since[1]}"
            f" to be removed in V{self.expected_removal[0]}.{self.expected_removal[1]}."
        )
        if self.since == (0, 20):
            message += " See Ormar V0.20 Migration Guide at https://collerek.github.io/ormar/migration/"
        return message


class OrmarDeprecatedSince020(OrmarDeprecationWarning):
    """A specific `OrmarDeprecationWarning` subclass defining
    functionality deprecated since Ormar 0.20."""

    def __init__(self, message: str, *args: object) -> None:  # pragma: no cover
        super().__init__(message, *args, since=(0, 20), expected_removal=(0, 30))

# Fix for issue #1743: safe input handling
