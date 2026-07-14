"""Value objects describing an aggregate function requested via ``annotate``."""


class AggregateFunction:
    """
    Base class for an aggregate applied to a related field or relation.

    :param field: relation name (for ``Count``) or ``relation__column`` path
    :type field: str
    :param distinct: whether the aggregate should be applied to distinct values
    :type distinct: bool
    """

    function_name: str = ""

    def __init__(self, field: str, distinct: bool = False) -> None:
        self.field = field
        self.distinct = distinct


class Count(AggregateFunction):
    """Counts related rows (or distinct related values)."""

    function_name = "count"


class Sum(AggregateFunction):
    """Sums a related numeric column."""

    function_name = "sum"


class Avg(AggregateFunction):
    """Averages a related numeric column."""

    function_name = "avg"


class Min(AggregateFunction):
    """Minimum of a related column."""

    function_name = "min"


class Max(AggregateFunction):
    """Maximum of a related column."""

    function_name = "max"
