from typing import Any, ForwardRef, Optional

import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config(force_rollback=True)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id = ormar.Integer(primary_key=True)
    name = ormar.String(max_length=40)


class PostCategory(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts_x_categories")

    id: int = ormar.Integer(primary_key=True)
    sort_order: Optional[int] = ormar.Integer(nullable=True)
    param_name: str = ormar.String(default="Name", max_length=200)


class Blog(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories = ormar.ManyToMany(Category, through=PostCategory)
    blog = ormar.ForeignKey(Blog)


create_test_database = init_tests(base_ormar_config)


class PostCategory2(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts_x_categories2")

    id: int = ormar.Integer(primary_key=True)
    sort_order: Optional[int] = ormar.Integer(nullable=True)


class Post2(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories = ormar.ManyToMany(Category, through=ForwardRef("PostCategory2"))


@pytest.mark.asyncio
async def test_forward_ref_is_updated():
    async with base_ormar_config.database:
        assert Post2.ormar_config.requires_ref_update
        Post2.update_forward_refs()

        assert Post2.ormar_config.model_fields["postcategory2"].to == PostCategory2


@pytest.mark.asyncio
async def test_setting_fields_on_through_model():
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        category = await Category(name="Test category").save()
        await post.categories.add(category)

        assert hasattr(post.categories[0], "postcategory")
        assert post.categories[0].postcategory is None


@pytest.mark.asyncio
async def test_setting_additional_fields_on_through_model_in_add():
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        category = await Category(name="Test category").save()
        await post.categories.add(category, sort_order=1)
        postcat = await PostCategory.objects.get()
        assert postcat.sort_order == 1


@pytest.mark.asyncio
async def test_setting_additional_fields_on_through_model_in_create():
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 2}
        )
        postcat = await PostCategory.objects.get()
        assert postcat.sort_order == 2


@pytest.mark.asyncio
async def test_getting_additional_fields_from_queryset() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1", postcategory={"sort_order": 1}
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 2}
        )

        await post.categories.all()
        assert post.postcategory is None
        assert post.categories[0].postcategory.sort_order == 1
        assert post.categories[1].postcategory.sort_order == 2

        post2 = await Post.objects.select_related("categories").get(
            categories__name="Test category2"
        )
        assert post2.categories[0].postcategory.sort_order == 2


@pytest.mark.asyncio
async def test_only_one_side_has_through() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1", postcategory={"sort_order": 1}
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 2}
        )

        post2 = await Post.objects.select_related("categories").get()
        assert post2.postcategory is None
        assert post2.categories[0].postcategory is not None

        await post2.categories.all()
        assert post2.postcategory is None
        assert post2.categories[0].postcategory is not None

        categories = await Category.objects.select_related("posts").all()
        assert isinstance(categories[0], Category)
        assert categories[0].postcategory is None
        assert categories[0].posts[0].postcategory is not None


@pytest.mark.asyncio
async def test_filtering_by_through_model() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 1, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 2, "param_name": "area"}
        )

        post2 = (
            await Post.objects.select_related("categories")
            .filter(postcategory__sort_order__gt=1)
            .get()
        )
        assert len(post2.categories) == 1
        assert post2.categories[0].postcategory.sort_order == 2

        post3 = await Post.objects.filter(
            categories__postcategory__param_name="volume"
        ).get()
        assert len(post3.categories) == 1
        assert post3.categories[0].postcategory.param_name == "volume"


@pytest.mark.asyncio
async def test_deep_filtering_by_through_model() -> Any:
    async with base_ormar_config.database:
        blog = await Blog(title="My Blog").save()
        post = await Post(title="Test post", blog=blog).save()

        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 1, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 2, "param_name": "area"}
        )

        blog2 = (
            await Blog.objects.select_related("posts__categories")
            .filter(posts__postcategory__sort_order__gt=1)
            .get()
        )
        assert len(blog2.posts) == 1
        assert len(blog2.posts[0].categories) == 1
        assert blog2.posts[0].categories[0].postcategory.sort_order == 2

        blog3 = await Blog.objects.filter(
            posts__categories__postcategory__param_name="volume"
        ).get()
        assert len(blog3.posts) == 1
        assert len(blog3.posts[0].categories) == 1
        assert blog3.posts[0].categories[0].postcategory.param_name == "volume"


@pytest.mark.asyncio
async def test_ordering_by_through_model() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 2, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 1, "param_name": "area"}
        )
        await post.categories.create(
            name="Test category3",
            postcategory={"sort_order": 3, "param_name": "velocity"},
        )

        post2 = (
            await Post.objects.select_related("categories")
            .order_by("-postcategory__sort_order")
            .get()
        )
        assert len(post2.categories) == 3
        assert post2.categories[0].name == "Test category3"
        assert post2.categories[2].name == "Test category2"

        post3 = (
            await Post.objects.select_related("categories")
            .order_by("categories__postcategory__param_name")
            .get()
        )
        assert len(post3.categories) == 3
        assert post3.categories[0].postcategory.param_name == "area"
        assert post3.categories[2].postcategory.param_name == "volume"


@pytest.mark.asyncio
async def test_update_through_models_from_queryset_on_through() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 2, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 1, "param_name": "area"}
        )
        await post.categories.create(
            name="Test category3",
            postcategory={"sort_order": 3, "param_name": "velocity"},
        )

        await PostCategory.objects.filter(param_name="volume", post=post.id).update(
            sort_order=4
        )
        post2 = (
            await Post.objects.select_related("categories")
            .order_by("-postcategory__sort_order")
            .get()
        )
        assert len(post2.categories) == 3
        assert post2.categories[0].postcategory.param_name == "volume"
        assert post2.categories[2].postcategory.param_name == "area"


@pytest.mark.asyncio
async def test_update_through_model_after_load() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 2, "param_name": "volume"},
        )
        post2 = await Post.objects.select_related("categories").get()
        assert len(post2.categories) == 1

        await post2.categories[0].postcategory.load()
        await post2.categories[0].postcategory.update(sort_order=3)

        post3 = await Post.objects.select_related("categories").get()
        assert len(post3.categories) == 1
        assert post3.categories[0].postcategory.sort_order == 3


@pytest.mark.asyncio
async def test_update_through_from_related() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 2, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 1, "param_name": "area"}
        )
        await post.categories.create(
            name="Test category3",
            postcategory={"sort_order": 3, "param_name": "velocity"},
        )

        await post.categories.filter(name="Test category3").update(
            postcategory={"sort_order": 4}
        )

        post2 = (
            await Post.objects.select_related("categories")
            .order_by("postcategory__sort_order")
            .get()
        )
        assert len(post2.categories) == 3
        assert post2.categories[2].postcategory.sort_order == 4


@pytest.mark.asyncio
async def test_excluding_fields_on_through_model() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        await post.categories.create(
            name="Test category1",
            postcategory={"sort_order": 2, "param_name": "volume"},
        )
        await post.categories.create(
            name="Test category2", postcategory={"sort_order": 1, "param_name": "area"}
        )
        await post.categories.create(
            name="Test category3",
            postcategory={"sort_order": 3, "param_name": "velocity"},
        )

        post2 = (
            await Post.objects.select_related("categories")
            .exclude_fields("postcategory__param_name")
            .order_by("postcategory__sort_order")
            .get()
        )
        assert len(post2.categories) == 3
        assert post2.categories[0].postcategory.param_name is None
        assert post2.categories[0].postcategory.sort_order == 1

        assert post2.categories[2].postcategory.param_name is None
        assert post2.categories[2].postcategory.sort_order == 3

        post3 = (
            await Post.objects.select_related("categories")
            .fields({"postcategory": ..., "title": ...})
            .exclude_fields({"postcategory": {"param_name", "sort_order"}})
            .get()
        )
        assert len(post3.categories) == 3
        for category in post3.categories:
            assert category.postcategory.param_name is None
            assert category.postcategory.sort_order is None


@pytest.mark.asyncio
async def test_implicit_through_paths_in_chains_proxies_and_prefetch() -> Any:
    async with base_ormar_config.database:
        first_blog = await Blog(title="first").save()
        second_blog = await Blog(title="second").save()
        post = await Post(title="Test post", blog=first_blog).save()
        other_post = await Post(title="Other post", blog=second_blog).save()
        for name, sort_order in [("a", 3), ("b", 1), ("c", 2)]:
            await post.categories.create(
                name=name, postcategory={"sort_order": sort_order}
            )
        await other_post.categories.create(name="d", postcategory={"sort_order": 5})

        blogs = await Blog.objects.filter(posts__postcategory__sort_order__gt=4).all()
        assert [blog.title for blog in blogs] == ["second"]

        ascending = await post.categories.order_by("postcategory__sort_order").all()
        assert [category.name for category in ascending] == ["b", "c", "a"]
        descending = await post.categories.order_by("-postcategory__sort_order").all()
        assert [category.name for category in descending] == ["a", "c", "b"]

        with pytest.raises(QueryDefinitionError, match="many to many"):
            await Post.objects.prefetch_related(
                ormar.Prefetch(
                    "categories",
                    queryset=Category.objects.order_by("postcategory__sort_order"),
                )
            ).all()


@pytest.mark.asyncio
async def test_aggregates_on_through_fields_keep_requested_keys() -> Any:
    async with base_ormar_config.database:
        post = await Post(title="Test post").save()
        for name, sort_order in [("a", 3), ("b", 7)]:
            await post.categories.create(
                name=name, postcategory={"sort_order": sort_order}
            )
        queryset = Post.objects.select_related("categories")

        assert await queryset.max("postcategory__sort_order") == 7
        assert await queryset.min("categories__postcategory__sort_order") == 3
        assert await queryset.max(["postcategory__sort_order", "title"]) == {
            "postcategory__sort_order": 7,
            "title": "Test post",
        }
