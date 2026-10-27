import copy
import pickle

import pytest

import ormar
from tests.settings import create_config

base_ormar_config = create_config()


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pkc_books")

    id: int = ormar.Integer(primary_key=True, name="book_id")
    title: str = ormar.String(max_length=50)


class ProxyBook(Book):
    ormar_config = base_ormar_config.copy(proxy=True)


class Audit(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True, name="audit_id")


class Child(Audit):
    ormar_config = base_ormar_config.copy(tablename="pkc_children")

    name: str = ormar.String(max_length=50)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pkc_students")

    id: int = ormar.Integer(primary_key=True)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pkc_courses")

    id: int = ormar.Integer(primary_key=True)
    students = ormar.ManyToMany(Student)


def test_config_has_key_for_plain_model():
    assert Book.ormar_config.pk.fields == ("id",)
    assert Book.ormar_config.pk.owner is Book


def test_inherited_and_proxy_models_have_own_key():
    assert Child.ormar_config.pk.owner is Child
    assert Child.ormar_config.pk.column_aliases() == ("audit_id",)
    assert ProxyBook.ormar_config.pk.owner is ProxyBook
    assert ProxyBook.ormar_config.pk.column_aliases() == ("book_id",)


def test_through_models_have_key():
    through = Course.ormar_config.model_fields["students"].through
    assert through.ormar_config.pk.fields == ("id",)
    assert through.ormar_config.pk.owner is through


def test_pk_descriptor_uses_key():
    book = Book(title="a")
    book.pk = 9
    assert book.id == 9
    assert book.pk == 9
    assert book.saved is False


def test_key_survives_pickle_and_deepcopy():
    book = Book(id=1, title="a")
    assert pickle.loads(pickle.dumps(book)).pk == 1
    key = copy.deepcopy(Book.ormar_config.pk)
    assert key.fields == ("id",)
    assert key.owner is Book
    assert key.column_aliases() == ("book_id",)


def test_missing_pk_still_raises():
    with pytest.raises(
        ormar.ModelDefinitionError, match="Table has to have a primary key"
    ):

        class NoPk(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="pkc_no_pk")

            name: str = ormar.String(max_length=10, primary_key=False)
