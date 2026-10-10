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
    title: str = ormar.String(max_length=50, name="posts", default="t")
    writer: Author = ormar.ForeignKey(Author)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rna_tags")

    id: int = ormar.Integer(primary_key=True)
    label: str = ormar.String(max_length=50, name="blogs", default="l")


class Blog(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rna_blogs")

    id: int = ormar.Integer(primary_key=True)
    blogs: list[Tag] = ormar.ManyToMany(Tag, related_name="tagged")


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


@pytest.mark.asyncio
async def test_select_related_reverse_when_target_column_alias_equals_relation_name():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create(nick="x")
            await Post.objects.create(writer=author)
            loaded = await Author.objects.select_related("posts").get()
            assert len(loaded.posts) == 1
            assert isinstance(loaded.posts[0], Post)
            assert loaded.posts[0].title == "t"


@pytest.mark.asyncio
async def test_select_related_m2m_when_target_column_alias_equals_relation_name():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            blog = await Blog.objects.create()
            await blog.blogs.add(await Tag.objects.create())
            loaded = await Blog.objects.select_related("blogs").get()
            assert len(loaded.blogs) == 1
            assert isinstance(loaded.blogs[0], Tag)
            assert loaded.blogs[0].label == "l"
