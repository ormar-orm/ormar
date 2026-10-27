"""
Kinds of relation fields: which side of a relation a field is and how many
related models it holds. Code that needs to know whether a relation holds a
list or a single model asks the field's kind instead of combining the
``virtual`` / ``is_multi`` / ``is_through`` flags itself.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RelationKind:
    """
    Kind of a relation field.

    :ivar name: kind name, for messages and debugging
    :ivar many: whether the field holds a list of related models
    """

    name: str
    many: bool


FORWARD_FOREIGN_KEY = RelationKind(name="forward foreign key", many=False)
REVERSE_FOREIGN_KEY = RelationKind(name="reverse foreign key", many=True)
FORWARD_ONE_TO_ONE = RelationKind(name="forward one to one", many=False)
REVERSE_ONE_TO_ONE = RelationKind(name="reverse one to one", many=False)
MANY_TO_MANY = RelationKind(name="many to many", many=True)
THROUGH = RelationKind(name="through", many=False)
