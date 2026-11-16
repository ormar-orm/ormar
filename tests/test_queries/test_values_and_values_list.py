from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class User(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Role(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    users: list[User] = ormar.ManyToMany(User)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)
    sort_order: Optional[int] = ormar.Integer(nullable=True)
    created_by: Optional[User] = ormar.ForeignKey(User, related_name="categories")


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=200)
    category: Optional[Category] = ormar.ForeignKey(Category)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="module")
async def sample_data():
    async with base_ormar_config.database:
        creator = await User(name="Anonymous").save()
        admin = await Role(name="admin").save()
        editor = await Role(name="editor").save()
        await creator.roles.add(admin)
        await creator.roles.add(editor)
        news = await Category(name="News", sort_order=0, created_by=creator).save()
        await Post(name="Ormar strikes again!", category=news).save()
        await Post(name="Why don't you use ormar yet?", category=news).save()
        await Post(name="Check this out, ormar now for free", category=news).save()


@pytest.mark.asyncio
async def test_simple_queryset_values():
    async with base_ormar_config.database:
        posts = await Post.objects.values()
        assert posts == [
            {"id": 1, "name": "Ormar strikes again!", "category": 1},
            {"id": 2, "name": "Why don't you use ormar yet?", "category": 1},
            {"id": 3, "name": "Check this out, ormar now for free", "category": 1},
        ]


@pytest.mark.asyncio
async def test_queryset_values_nested_relation():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category__created_by").values()
        assert posts == [
            {
                "id": 1,
                "name": "Ormar strikes again!",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
                "category__created_by__id": 1,
                "category__created_by__name": "Anonymous",
            },
            {
                "category": 1,
                "id": 2,
                "name": "Why don't you use ormar yet?",
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
                "category__created_by__id": 1,
                "category__created_by__name": "Anonymous",
            },
            {
                "id": 3,
                "name": "Check this out, ormar now for free",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
                "category__created_by__id": 1,
                "category__created_by__name": "Anonymous",
            },
        ]


@pytest.mark.asyncio
async def test_queryset_values_nested_relation_subset_of_fields():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category__created_by").values(
            ["name", "category__name", "category__created_by__name"]
        )
        assert posts == [
            {
                "name": "Ormar strikes again!",
                "category__name": "News",
                "category__created_by__name": "Anonymous",
            },
            {
                "name": "Why don't you use ormar yet?",
                "category__name": "News",
                "category__created_by__name": "Anonymous",
            },
            {
                "name": "Check this out, ormar now for free",
                "category__name": "News",
                "category__created_by__name": "Anonymous",
            },
        ]


@pytest.mark.asyncio
async def test_queryset_simple_values_list():
    async with base_ormar_config.database:
        posts = await Post.objects.values_list()
        assert posts == [
            (1, "Ormar strikes again!", 1),
            (2, "Why don't you use ormar yet?", 1),
            (3, "Check this out, ormar now for free", 1),
        ]


@pytest.mark.asyncio
async def test_queryset_nested_relation_values_list():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category__created_by").values_list()
        assert posts == [
            (1, "Ormar strikes again!", 1, 1, "News", 0, 1, 1, "Anonymous"),
            (2, "Why don't you use ormar yet?", 1, 1, "News", 0, 1, 1, "Anonymous"),
            (
                3,
                "Check this out, ormar now for free",
                1,
                1,
                "News",
                0,
                1,
                1,
                "Anonymous",
            ),
        ]


@pytest.mark.asyncio
async def test_queryset_nested_relation_subset_of_fields_values_list():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category__created_by").values_list(
            ["name", "category__name", "category__created_by__name"]
        )
        assert posts == [
            ("Ormar strikes again!", "News", "Anonymous"),
            ("Why don't you use ormar yet?", "News", "Anonymous"),
            ("Check this out, ormar now for free", "News", "Anonymous"),
        ]


@pytest.mark.asyncio
async def test_m2m_values():
    async with base_ormar_config.database:
        user = await User.objects.select_related("roles").values()
        assert user == [
            {
                "id": 1,
                "name": "Anonymous",
                "roleuser__id": 1,
                "roleuser__role": 1,
                "roleuser__user": 1,
                "roles__id": 1,
                "roles__name": "admin",
            },
            {
                "id": 1,
                "name": "Anonymous",
                "roleuser__id": 2,
                "roleuser__role": 2,
                "roleuser__user": 1,
                "roles__id": 2,
                "roles__name": "editor",
            },
        ]


@pytest.mark.asyncio
async def test_nested_m2m_values():
    async with base_ormar_config.database:
        user = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .values()
        )
        assert user == [
            {
                "id": 1,
                "name": "admin",
                "roleuser__id": 1,
                "roleuser__role": 1,
                "roleuser__user": 1,
                "users__id": 1,
                "users__name": "Anonymous",
                "users__categories__id": 1,
                "users__categories__name": "News",
                "users__categories__sort_order": 0,
                "users__categories__created_by": 1,
            }
        ]


@pytest.mark.asyncio
async def test_nested_m2m_values_without_through_explicit():
    async with base_ormar_config.database:
        user = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
            .exclude_fields("roleuser")
            .values()
        )
        assert user == [
            {
                "name": "admin",
                "users__name": "Anonymous",
                "users__categories__name": "News",
            }
        ]


@pytest.mark.asyncio
async def test_nested_m2m_values_without_through_param():
    async with base_ormar_config.database:
        user = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
            .values(exclude_through=True)
        )
        assert user == [
            {
                "name": "admin",
                "users__name": "Anonymous",
                "users__categories__name": "News",
            }
        ]


@pytest.mark.asyncio
async def test_nested_m2m_values_no_through_and_m2m_models_but_keep_end_model():
    async with base_ormar_config.database:
        user = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
            .exclude_fields(["roleuser", "users"])
            .values()
        )
        assert user == [{"name": "admin", "users__categories__name": "News"}]


@pytest.mark.asyncio
async def test_nested_flatten_and_exception():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError):
            (await Role.objects.fields({"name", "id"}).values_list(flatten=True))

        roles = await Role.objects.fields("name").values_list(flatten=True)
        assert roles == ["admin", "editor"]


@pytest.mark.asyncio
async def test_empty_result():
    async with base_ormar_config.database:
        roles = await Role.objects.filter(Role.name == "test").values_list()
        roles2 = await Role.objects.filter(Role.name == "test").values()
        assert roles == roles2 == []


@pytest.mark.asyncio
async def test_queryset_values_multiple_select_related():
    async with base_ormar_config.database:
        posts = (
            await Category.objects.select_related(["created_by__roles", "posts"])
            .filter(Category.created_by.roles.name == "editor")
            .values(
                ["name", "posts__name", "created_by__name", "created_by__roles__name"],
                exclude_through=True,
            )
        )
        assert posts == [
            {
                "name": "News",
                "created_by__name": "Anonymous",
                "created_by__roles__name": "editor",
                "posts__name": "Ormar strikes again!",
            },
            {
                "name": "News",
                "created_by__name": "Anonymous",
                "created_by__roles__name": "editor",
                "posts__name": "Why don't you use ormar yet?",
            },
            {
                "name": "News",
                "created_by__name": "Anonymous",
                "created_by__roles__name": "editor",
                "posts__name": "Check this out, ormar now for free",
            },
        ]


@pytest.mark.asyncio
async def test_querysetproxy_values():
    async with base_ormar_config.database:
        role = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .get()
        )
        user = await role.users.values()
        assert user == [
            {
                "id": 1,
                "name": "Anonymous",
                "roles__id": 1,
                "roles__name": "admin",
                "roleuser__id": 1,
                "roleuser__role": 1,
                "roleuser__user": 1,
            }
        ]

        user = (
            await role.users.filter(name="Anonymous")
            .select_related("categories")
            .fields({"name": ..., "categories": {"name"}})
            .values(exclude_through=True)
        )
        assert user == [{"name": "Anonymous", "categories__name": "News"}]

        user = (
            await role.users.filter(name="Anonymous")
            .select_related("categories")
            .fields({"name": ..., "categories": {"name"}})
            .exclude_fields("roles")
            .values(exclude_through=True)
        )
        assert user == [{"name": "Anonymous", "categories__name": "News"}]


@pytest.mark.asyncio
async def test_querysetproxy_values_list():
    async with base_ormar_config.database:
        role = (
            await Role.objects.select_related("users__categories")
            .filter(name="admin")
            .get()
        )
        user = await role.users.values_list()
        assert user == [(1, "Anonymous", 1, 1, 1, 1, "admin")]

        user = (
            await role.users.filter(name="Anonymous")
            .select_related("categories")
            .fields({"name": ..., "categories": {"name"}})
            .values_list(exclude_through=True)
        )
        assert user == [("Anonymous", "News")]

        user = (
            await role.users.filter(name="Anonymous")
            .select_related("categories")
            .fields({"name": ..., "categories": {"name"}})
            .exclude_fields("roles")
            .values_list(exclude_through=True)
        )
        assert user == [("Anonymous", "News")]

        user = (
            await role.users.filter(name="Anonymous")
            .select_related("categories")
            .fields({"name"})
            .exclude_fields("roles")
            .values_list(exclude_through=True, flatten=True)
        )
        assert user == ["Anonymous"]


@pytest.mark.asyncio
async def test_filter_related_does_not_leak_columns_in_values_list():
    async with base_ormar_config.database:
        posts = await Post.objects.filter(category__id=1).fields(["name"]).values_list()
        assert posts == [
            ("Ormar strikes again!",),
            ("Why don't you use ormar yet?",),
            ("Check this out, ormar now for free",),
        ]


@pytest.mark.asyncio
async def test_values_list_fields_kwarg_does_not_leak_related_columns():
    async with base_ormar_config.database:
        posts = await Post.objects.filter(category__id=1).values_list(fields=["name"])
        assert posts == [
            ("Ormar strikes again!",),
            ("Why don't you use ormar yet?",),
            ("Check this out, ormar now for free",),
        ]


@pytest.mark.asyncio
async def test_filter_related_does_not_leak_columns_in_values():
    async with base_ormar_config.database:
        posts = await Post.objects.filter(category__id=1).fields(["name"]).values()
        assert posts == [
            {"name": "Ormar strikes again!"},
            {"name": "Why don't you use ormar yet?"},
            {"name": "Check this out, ormar now for free"},
        ]


@pytest.mark.asyncio
async def test_explicit_select_related_with_only_main_fields_excludes_related():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category").fields(["name"]).values()
        assert posts == [
            {"name": "Ormar strikes again!"},
            {"name": "Why don't you use ormar yet?"},
            {"name": "Check this out, ormar now for free"},
        ]


@pytest.mark.asyncio
async def test_nested_fields_keep_referenced_relation():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category")
            .fields(["name", "category__name"])
            .values()
        )
        assert posts == [
            {"name": "Ormar strikes again!", "category__name": "News"},
            {"name": "Why don't you use ormar yet?", "category__name": "News"},
            {"name": "Check this out, ormar now for free", "category__name": "News"},
        ]


@pytest.mark.asyncio
async def test_m2m_relation_and_through_excluded_when_only_main_fields():
    async with base_ormar_config.database:
        roles = await (
            Role.objects.select_related("users__categories").fields(["name"]).values()
        )
        assert roles == [{"name": "admin"}, {"name": "editor"}]


@pytest.mark.asyncio
async def test_relation_name_in_fields_keeps_relation_columns():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category")
            .fields({"name", "category"})
            .values()
        )
        assert posts == [
            {
                "name": "Ormar strikes again!",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
            {
                "name": "Why don't you use ormar yet?",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
            {
                "name": "Check this out, ormar now for free",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
        ]


@pytest.mark.asyncio
async def test_filter_related_without_fields_keeps_all_columns():
    async with base_ormar_config.database:
        posts = await Post.objects.filter(category__id=1).values()
        assert posts == [
            {
                "id": 1,
                "name": "Ormar strikes again!",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
            {
                "id": 2,
                "name": "Why don't you use ormar yet?",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
            {
                "id": 3,
                "name": "Check this out, ormar now for free",
                "category": 1,
                "category__id": 1,
                "category__name": "News",
                "category__sort_order": 0,
                "category__created_by": 1,
            },
        ]


@pytest.mark.asyncio
async def test_nested_only_fields_drops_main_columns_in_values():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category").fields(["category__name"]).values()
        )
        assert posts == [
            {"category__name": "News"},
            {"category__name": "News"},
            {"category__name": "News"},
        ]


@pytest.mark.asyncio
async def test_nested_only_fields_drops_main_columns_in_values_list():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category")
            .fields(["category__name"])
            .values_list()
        )
        assert posts == [("News",), ("News",), ("News",)]


@pytest.mark.asyncio
async def test_nested_only_fields_flatten_returns_nested_value():
    """
    Reproducer for issue #1060. Without the fix, flatten=True picks up
    Post.id (the first leaked main-model column) instead of the nested
    value the caller actually requested.
    """
    async with base_ormar_config.database:
        names = await (
            Post.objects.select_related("category")
            .fields(["category__name"])
            .values_list(flatten=True)
        )
        assert names == ["News", "News", "News"]


@pytest.mark.asyncio
async def test_deeper_nested_only_fields_drops_main_columns():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category__created_by")
            .fields(["category__created_by__name"])
            .values()
        )
        assert posts == [
            {"category__created_by__name": "Anonymous"},
            {"category__created_by__name": "Anonymous"},
            {"category__created_by__name": "Anonymous"},
        ]


@pytest.mark.asyncio
async def test_main_referenced_keeps_main_columns():
    async with base_ormar_config.database:
        posts = await (
            Post.objects.select_related("category")
            .fields(["name", "category__name"])
            .values()
        )
        assert posts == [
            {"name": "Ormar strikes again!", "category__name": "News"},
            {"name": "Why don't you use ormar yet?", "category__name": "News"},
            {"name": "Check this out, ormar now for free", "category__name": "News"},
        ]


POST_NAMES = [
    "Ormar strikes again!",
    "Why don't you use ormar yet?",
    "Check this out, ormar now for free",
]


@pytest.mark.asyncio
async def test_values_related_fields_join_relation_automatically():
    async with base_ormar_config.database:
        posts = await Post.objects.values(["category__name", "category__sort_order"])
        assert posts == [{"category__name": "News", "category__sort_order": 0}] * 3


@pytest.mark.asyncio
async def test_values_own_and_related_fields_join_relation_automatically():
    async with base_ormar_config.database:
        posts = await Post.objects.values(["name", "category__name"])
        assert posts == [
            {"name": post_name, "category__name": "News"} for post_name in POST_NAMES
        ]


@pytest.mark.asyncio
async def test_values_list_related_fields_join_relation_automatically():
    async with base_ormar_config.database:
        posts = await Post.objects.values_list(
            ["category__name", "category__sort_order"]
        )
        assert posts == [("News", 0)] * 3


@pytest.mark.asyncio
async def test_values_list_flatten_related_field_joins_relation_automatically():
    async with base_ormar_config.database:
        names = await Post.objects.values_list("category__name", flatten=True)
        assert names == ["News"] * 3


@pytest.mark.asyncio
async def test_values_nested_related_path_joins_whole_path_automatically():
    async with base_ormar_config.database:
        posts = await Post.objects.values(["name", "category__created_by__name"])
        assert posts == [
            {"name": post_name, "category__created_by__name": "Anonymous"}
            for post_name in POST_NAMES
        ]


@pytest.mark.asyncio
async def test_values_dict_and_set_specs_join_relations_automatically():
    async with base_ormar_config.database:
        from_dict = await Post.objects.values(
            {"name": ..., "category": {"name": ..., "created_by": {"name"}}}
        )
        from_set = await Post.objects.values(
            {"name", "category__name", "category__created_by__name"}
        )
        expected = [
            {
                "name": post_name,
                "category__name": "News",
                "category__created_by__name": "Anonymous",
            }
            for post_name in POST_NAMES
        ]
        assert from_dict == from_set == expected


@pytest.mark.asyncio
async def test_values_related_fields_with_relation_already_selected():
    async with base_ormar_config.database:
        posts = await Post.objects.select_related("category__created_by").values(
            ["name", "category__name"]
        )
        assert posts == [
            {"name": post_name, "category__name": "News"} for post_name in POST_NAMES
        ]


@pytest.mark.asyncio
async def test_values_related_fields_with_filter_on_same_relation():
    async with base_ormar_config.database:
        posts = await Post.objects.filter(category__name="News").values(
            ["name", "category__sort_order"]
        )
        assert posts == [
            {"name": post_name, "category__sort_order": 0} for post_name in POST_NAMES
        ]
        assert (
            await Post.objects.filter(category__name="Sports").values(
                ["category__name"]
            )
            == []
        )


@pytest.mark.asyncio
async def test_fields_with_related_paths_then_values_joins_relation():
    async with base_ormar_config.database:
        posts = await Post.objects.fields(["name", "category__name"]).values()
        assert posts == [
            {"name": post_name, "category__name": "News"} for post_name in POST_NAMES
        ]
        names = await Post.objects.fields("category__name").values_list(flatten=True)
        assert names == ["News"] * 3


@pytest.mark.asyncio
async def test_exclude_fields_with_related_path_does_not_join_relation():
    async with base_ormar_config.database:
        posts = await Post.objects.exclude_fields("category__name").values()
        assert posts == [
            {"id": post_id, "name": post_name, "category": 1}
            for post_id, post_name in enumerate(POST_NAMES, start=1)
        ]


@pytest.mark.asyncio
async def test_values_reverse_relation_field_returns_row_per_related_model():
    async with base_ormar_config.database:
        categories = await Category.objects.values(["name", "posts__name"])
        assert categories == [
            {"name": "News", "posts__name": post_name} for post_name in POST_NAMES
        ]


@pytest.mark.asyncio
async def test_values_m2m_relation_field_returns_row_per_related_model():
    async with base_ormar_config.database:
        users = await User.objects.values(["name", "roles__name"], exclude_through=True)
        assert users == [
            {"name": "Anonymous", "roles__name": "admin"},
            {"name": "Anonymous", "roles__name": "editor"},
        ]


@pytest.mark.asyncio
async def test_values_relation_name_without_join_returns_foreign_key():
    async with base_ormar_config.database:
        posts = await Post.objects.values(["name", "category"])
        assert posts == [{"name": post_name, "category": 1} for post_name in POST_NAMES]


@pytest.mark.asyncio
async def test_querysetproxy_values_related_field_joins_relation_automatically():
    async with base_ormar_config.database:
        category = await Category.objects.get(name="News")
        posts = await category.posts.values_list(["name", "category__sort_order"])
        assert posts == [(post_name, 0) for post_name in POST_NAMES]
