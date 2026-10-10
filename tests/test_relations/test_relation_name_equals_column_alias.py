import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rna_authors")

    id: int = ormar.Integer(primary_key=True)
    nick: str = ormar.String(max_length=50, name="writer")


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rna_posts")

    id: int = ormar.Integer(primary_key=True)
    writer: Author = ormar.ForeignKey(Author)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_select_related_when_target_column_alias_equals_relation_name():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create(nick="x")
            await Post.objects.create(writer=author)
            post = await Post.objects.select_related("writer").get()
            assert isinstance(post.writer, Author)
            assert post.writer.nick == "x"
