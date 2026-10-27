import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from ormar.models.primary_key import CompositeKey, SingleColumnKey, build_primary_key
from tests.settings import create_config

base_ormar_config = create_config()


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_books")

    id: int = ormar.Integer(primary_key=True, name="book_id")
    title: str = ormar.String(max_length=50)


def test_single_key_shape():
    key = build_primary_key(owner=Book, fields=("id",))
    assert isinstance(key, SingleColumnKey)
    assert key.is_composite is False
    assert key.fields == ("id",)
    assert key.column_aliases() == ("book_id",)
    assert key.columns() == (Book.ormar_config.table.c.book_id,)
    assert key.python_type() is int
    assert key.autoincrement is True


def test_single_key_values():
    key = build_primary_key(owner=Book, fields=("id",))
    book = Book(id=3, title="a")
    assert key.value(book) == 3
    assert key.to_dict(3) == {"id": 3}
    assert key.getter(("book_id",))({"book_id": 7, "x": 1}) == 7
    assert key.bind_slots() == ("ormar_pk",)
    assert key.bind_params(5) == {"ormar_pk": 5}


def test_single_key_where_clause_compiles_like_before():
    key = build_primary_key(owner=Book, fields=("id",))
    clause = key.where_clause(slots=key.bind_slots())
    assert str(clause) == "pk_books.book_id = :ormar_pk"
    assert (
        clause.right.type._type_affinity
        is Book.ormar_config.table.c.book_id.type._type_affinity
    )
    untyped = key.where_clause(slots=("new_book_id",), typed=False)
    assert str(untyped) == "pk_books.book_id = :new_book_id"


def test_rebind_changes_owner_only():
    key = build_primary_key(owner=Book, fields=("id",))
    rebound = key.rebind(owner=Book)
    assert rebound is not key
    assert rebound.fields == key.fields
    assert rebound.owner is Book


def test_expand_pk_kwarg_and_lookup():
    key = build_primary_key(owner=Book, fields=("id",))
    assert key.expand_pk_kwarg({"pk": 4, "title": "a"}) == {"id": 4, "title": "a"}
    assert key.expand_pk_kwarg({"title": "a"}) == {"title": "a"}
    assert key.lookup({"id": 4, "title": "a"}) == {"id": 4}
    assert key.lookup({"id": None, "title": "a"}) is None
    assert key.lookup({"title": "a"}) is None


def test_value_from_mapping():
    key = build_primary_key(owner=Book, fields=("id",))
    assert key.value_from({"id": 4, "title": "a"}) == 4
    assert key.value_from({"title": "a"}) is None


def composite_key() -> "CompositeKey":
    return CompositeKey(owner=Book, fields=("id", "title"))


def test_composite_key_shape():
    key = composite_key()
    assert key.is_composite is True
    assert key.column_aliases() == ("book_id", "title")
    assert key.python_type() is tuple
    assert build_primary_key(owner=Book, fields=("id", "title")).is_composite


def test_composite_value_none_only_when_part_none():
    key = composite_key()
    assert key.value(Book(id=0, title="")) == (0, "")
    assert key.value(Book(id=3, title="a")) == (3, "a")
    assert key.value(Book(title="a")) is None
    assert key.value_from({"id": 3, "title": "a"}) == (3, "a")
    assert key.value_from({"id": 3}) is None


def test_composite_to_dict_accepts_tuple_list_and_dict():
    key = composite_key()
    assert key.to_dict((3, "a")) == {"id": 3, "title": "a"}
    assert key.to_dict([3, "a"]) == {"id": 3, "title": "a"}
    assert key.to_dict({"title": "a", "id": 3}) == {"id": 3, "title": "a"}


@pytest.mark.parametrize(
    "value", [(1,), (1, "a", 2), {"id": 1}, {"id": 1, "bogus": 2}, 5, "ab"]
)
def test_composite_to_dict_rejects_wrong_shape(value):
    with pytest.raises(QueryDefinitionError, match="primary key has fields"):
        composite_key().to_dict(value)


def test_composite_getter_bind_and_where():
    key = composite_key()
    getter = key.getter(("b_id", "b_title"))
    assert getter({"b_id": 1, "b_title": "x", "other": 2}) == (1, "x")
    assert getter({"b_id": None, "b_title": "x"}) is None
    assert key.bind_slots() == ("ormar_pk_0", "ormar_pk_1")
    assert key.bind_params((1, "x")) == {"ormar_pk_0": 1, "ormar_pk_1": "x"}
    assert str(key.where_clause(slots=key.bind_slots())) == (
        "pk_books.book_id = :ormar_pk_0 AND pk_books.title = :ormar_pk_1"
    )
    assert key.lookup({"id": 1, "title": "x"}) == {"id": 1, "title": "x"}
    assert key.expand_pk_kwarg({"pk": (1, "x")}) == {"id": 1, "title": "x"}
