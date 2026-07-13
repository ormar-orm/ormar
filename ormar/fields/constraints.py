from typing import Any, Optional

from sqlalchemy import CheckConstraint, Index, UniqueConstraint, column, func


class UniqueColumns(UniqueConstraint):
    """
    Subclass of sqlalchemy.UniqueConstraint.
    Used to avoid importing anything from sqlalchemy by user.
    """


class IndexColumns(Index):
    """
    Subclass of sqlalchemy.Index.
    Used to avoid importing anything from sqlalchemy by user.

    Passing ``unique=True`` together with ``case_insensitive=True`` builds a
    case-insensitive unique index by wrapping each named column in ``LOWER()``,
    which is how case-insensitive uniqueness is expressed (a regular
    UniqueConstraint cannot hold SQL expressions).
    """

    def __init__(
        self,
        *args: Any,
        name: Optional[str] = None,
        case_insensitive: bool = False,
        **kw: Any,
    ) -> None:
        """
        :param args: column names, or SQL expressions for a functional index
        :type args: Any
        :param name: optional explicit index name
        :type name: Optional[str]
        :param case_insensitive: wrap each named column in ``LOWER()`` so the
            index (and its uniqueness, when ``unique=True``) ignores case
        :type case_insensitive: bool
        :param kw: additional keyword arguments passed to sqlalchemy.Index
        :type kw: Any
        """
        if case_insensitive:
            args = tuple(func.lower(column(arg)) for arg in args)
        if not name:
            name = "TEMPORARY_NAME"
        super().__init__(name, *args, **kw)


class CheckColumns(CheckConstraint):
    """
    Subclass of sqlalchemy.CheckConstraint.
    Used to avoid importing anything from sqlalchemy by user.

    Note that some databases do not actively support check constraints such as MySQL.
    """
