from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config(force_rollback=True)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories: Optional[list[Category]] = ormar.ManyToMany(Category)
    authors: Optional[list[Author]] = ormar.ManyToMany(
        Author, through=ForwardRef("AuthorXPosts")
    )


class AuthorXPosts(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="authors_x_posts", constraints=[ormar.UniqueColumns("author", "post")]
    )
    id: int = ormar.Integer(primary_key=True)
    author: Optional[int] = ormar.Integer(default=None)
    post: Optional[int] = ormar.Integer(default=None)


Post.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def cleanup():
    yield
    async with base_ormar_config.database:
        await Post.ormar_config.model_fields["categories"].through.objects.delete(
            each=True
        )
        await Post.ormar_config.model_fields["authors"].through.objects.delete(
            each=True
        )
        await Post.objects.delete(each=True)
        await Category.objects.delete(each=True)
        await Author.objects.delete(each=True)


@pytest.mark.asyncio
async def test_adding_same_m2m_model_twice():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post.objects.create(title="Hello, M2M")
            news = await Category(name="News").save()

            await post.categories.add(news)
            await post.categories.add(news)

            categories = await post.categories.all()
            assert categories == [news]


@pytest.mark.asyncio
async def test_adding_same_m2m_model_twice_with_unique():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post.objects.create(title="Hello, M2M")
            redactor = await Author(name="News").save()

            await post.authors.add(redactor)
            await post.authors.add(redactor)

            assert await AuthorXPosts.objects.count() == 1
            assert len(post.authors) == 1
