import pytest

import ormar
from ormar.models.primary_key import SingleColumnKey, build_primary_key
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


def test_two_fields_raise_until_phase_two():
    with pytest.raises(ormar.ModelDefinitionError, match="Only one primary key"):
        build_primary_key(owner=Book, fields=("id", "title"))


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
