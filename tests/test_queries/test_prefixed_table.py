from typing import Optional

import pytest

import ormar
from ormar.fields.key_only import key_dict_type
from ormar.models.model_row import build_shared_row_plan
from ormar.queryset.actions.filter_action import FilterAction
from ormar.relations.aliases import build_prefixed_columns, prefixed_table
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="prefixed_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="prefixed_books")

    id: int = ormar.Integer(primary_key=True)
    author: Optional[Author] = ormar.ForeignKey(Author)
    title: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


def author_action() -> FilterAction:
    return FilterAction(filter_str="author__name", value=None, model_cls=Book)


def test_prefixed_table_rejects_positional_arguments() -> None:
    action = author_action()
    with pytest.raises(TypeError):
        prefixed_table(action.table_prefix, action.column.table)  # type: ignore[misc]


def test_prefixed_table_returns_the_same_object_for_keyword_calls() -> None:
    action = author_action()
    first = prefixed_table(alias=action.table_prefix, table=action.column.table)
    second = prefixed_table(alias=action.table_prefix, table=action.column.table)
    assert first is second


def test_cached_helpers_reject_positional_arguments() -> None:
    table = Author.ormar_config.table
    with pytest.raises(TypeError):
        build_prefixed_columns("", table, None)  # type: ignore[misc]
    with pytest.raises(TypeError):
        build_shared_row_plan(  # type: ignore[misc]
            Author, "", frozenset(), frozenset(), False
        )
    with pytest.raises(TypeError):
        key_dict_type(Author, ("id",))  # type: ignore[misc]


@pytest.mark.asyncio
async def test_custom_where_on_prefixed_table_of_select_related() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            ann = await Author.objects.create(name="Ann")
            bob = await Author.objects.create(name="Bob")
            await Book.objects.create(author=ann, title="First")
            await Book.objects.create(author=bob, title="Second")

            action = author_action()
            table = prefixed_table(alias=action.table_prefix, table=action.column.table)
            expr = (
                Book.objects.select_related("author")
                .build_select_expression()
                .where(table.c.name == "Ann")
            )
            async with base_ormar_config.database.get_query_executor() as executor:
                rows = await executor.fetch_all(query=expr)
            assert [row["title"] for row in rows] == ["First"]
