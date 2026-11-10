from typing import Optional, assert_type

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import insert_statement
from ormar.queryset.templates.writes import build_conflict_insert
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oc_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, unique=True)
    score: int = ormar.Integer(default=0)
    note: Optional[str] = ormar.String(max_length=50, nullable=True)
    is_dirty: bool = ormar.Boolean(default=False, on_update=True)


class Pair(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oc_pairs")

    a: int = ormar.Integer(primary_key=True, autoincrement=False)
    b: int = ormar.Integer(primary_key=True, autoincrement=False)
    value: int = ormar.Integer(default=0)


class Booking(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="oc_bookings",
        constraints=[ormar.UniqueColumns("p_a", "p_b", "slot")],
    )

    id: int = ormar.Integer(primary_key=True)
    p: Optional[Pair] = ormar.ForeignKey(
        Pair, columns={"a": "p_a", "b": "p_b"}, related_name="bookings"
    )
    slot: int = ormar.Integer()
    label: str = ormar.String(max_length=20)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oc_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oc_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oc_posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50, unique=True)
    author: Optional[Author] = ormar.ForeignKey(Author, related_name="posts")
    categories: Optional[list[Category]] = ormar.ManyToMany(Category)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_create_ignore_returns_none_on_conflict() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Tag.objects.create(name="a", on_conflict="ignore")
            assert_type(first, Optional[Tag])
            assert_type(await Tag.objects.create(name="plain"), Tag)
            assert first is not None and first.pk is not None and first.saved
            assert await Tag.objects.create(name="a", on_conflict="ignore") is None
            same_pk = await Tag.objects.create(
                id=first.pk, name="other", on_conflict="ignore"
            )
            assert same_pk is None
            # the transaction stays usable after an ignored conflict
            second = await Tag.objects.create(name="b", on_conflict="ignore")
            assert second is not None and second.pk != first.pk
            assert await Tag.objects.count() == 3


@pytest.mark.asyncio
async def test_create_ignore_signals():
    calls: list[str] = []

    @ormar.pre_save(Tag)
    async def before(sender, instance, **kwargs):
        calls.append("pre")

    @ormar.post_save(Tag)
    async def after(sender, instance, **kwargs):
        calls.append("post")

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await Tag.objects.create(name="s", on_conflict="ignore")
                await Tag.objects.create(name="s", on_conflict="ignore")
                assert calls == ["pre", "post", "pre"]
                await Tag.objects.bulk_create(
                    [Tag(name="s"), Tag(name="t")], on_conflict="ignore"
                )
                assert calls == ["pre", "post", "pre"]
    finally:
        Tag.ormar_config.signals.pre_save.disconnect(before)
        Tag.ormar_config.signals.post_save.disconnect(after)


@pytest.mark.asyncio
async def test_bulk_create_ignore():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Tag.objects.bulk_create([Tag(name=n, score=1) for n in "abc"])
            existing = await Tag.objects.get(name="a")
            objects = [
                Tag(name="a", score=5),
                Tag(name="b", score=5),
                Tag(name="d", score=5),
                Tag(id=existing.pk, name="zz", score=5),
                Tag(name="e", score=5),
            ]
            await Tag.objects.bulk_create(objects, on_conflict="ignore")
            assert all(obj.saved for obj in objects)
            rows = await Tag.objects.order_by("name").all()
            assert [(t.name, t.score) for t in rows] == [
                ("a", 1),
                ("b", 1),
                ("c", 1),
                ("d", 5),
                ("e", 5),
            ]
            await Tag.objects.bulk_create([Tag(name="c")], on_conflict="ignore")
            assert await Tag.objects.count() == 5


@pytest.mark.asyncio
async def test_bulk_create_update_on_unique_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Tag.objects.bulk_create(
                [Tag(name="a", score=1, note="keep"), Tag(name="b", score=1)]
            )
            await Tag.objects.bulk_create(
                [
                    Tag(name="a", score=7, note="new"),
                    Tag(name="c", score=3),
                    Tag(name="c", score=4),
                ],
                on_conflict="update",
                conflict_target=["name"],
                update_fields=["score"],
            )
            rows = await Tag.objects.order_by("name").all()
            assert [(t.name, t.score, t.note, t.is_dirty) for t in rows] == [
                ("a", 7, "keep", True),
                ("b", 1, None, False),
                # duplicates in one batch: the last one wins
                ("c", 4, None, True),
            ]


@pytest.mark.asyncio
async def test_bulk_create_update_defaults_to_pk_and_all_columns():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Pair.objects.bulk_create([Pair(a=1, b=1, value=1)])
            await Pair.objects.bulk_create(
                [Pair(a=1, b=1, value=9), Pair(a=1, b=2, value=2)],
                on_conflict="update",
            )
            rows = await Pair.objects.order_by(["a", "b"]).all()
            assert [(p.a, p.b, p.value) for p in rows] == [(1, 1, 9), (1, 2, 2)]
            tag = await Tag.objects.create(name="x", score=1)
            await Tag.objects.bulk_create(
                [Tag(id=tag.pk, name="y", score=2, is_dirty=False)],
                on_conflict="update",
                update_fields=["is_dirty"],
            )
            reloaded = await Tag.objects.get(pk=tag.pk)
            assert (reloaded.name, reloaded.score, reloaded.is_dirty) == (
                "x",
                1,
                False,
            )


@pytest.mark.asyncio
async def test_bulk_create_update_expands_multi_column_foreign_key_target():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            pair = await Pair.objects.create(a=1, b=2)
            await Booking.objects.create(p=pair, slot=1, label="old")
            await Booking.objects.bulk_create(
                [
                    Booking(p=pair, slot=1, label="new"),
                    Booking(p=pair, slot=2, label="two"),
                ],
                on_conflict="update",
                conflict_target=["p", "slot"],
                update_fields=["label"],
            )
            rows = await Booking.objects.order_by("slot").all()
            assert [(b.slot, b.label) for b in rows] == [(1, "new"), (2, "two")]


@pytest.mark.asyncio
async def test_conflict_options_need_update():
    for kwargs in (
        {"on_conflict": "ignore", "conflict_target": ["name"]},
        {"on_conflict": "ignore", "update_fields": ["score"]},
        {"update_fields": ["score"]},
    ):
        with pytest.raises(QueryDefinitionError):
            await Tag.objects.bulk_create([Tag(name="a")], **kwargs)


def test_conflict_statements_per_dialect():
    table = Tag.ormar_config.table
    names = ("name", "score")
    common = dict(table=table, names=names)
    ignore = dict(on_conflict="ignore", target=("id",), update_names=(), on_update=())
    update = dict(
        on_conflict="update",
        target=("name",),
        update_names=("score",),
        on_update=("is_dirty",),
    )
    pg = str(
        build_conflict_insert(
            dialect_name="postgresql", returning=True, **common, **ignore
        ).compile(dialect=postgresql.dialect())
    )
    assert "ON CONFLICT DO NOTHING RETURNING oc_tags.id" in pg
    lite = str(
        build_conflict_insert(
            dialect_name="sqlite", returning=False, **common, **update
        ).compile(dialect=sqlite.dialect())
    )
    assert "ON CONFLICT (name) DO UPDATE SET score = excluded.score" in lite
    assert lite.endswith("score = excluded.score, is_dirty = ?")
    my_ignore = str(
        build_conflict_insert(
            dialect_name="mysql", returning=False, **common, **ignore
        ).compile(dialect=mysql.dialect())
    )
    assert "ON DUPLICATE KEY UPDATE id = oc_tags.id" in my_ignore
    my_update = str(
        build_conflict_insert(
            dialect_name="mysql", returning=False, **common, **update
        ).compile(dialect=mysql.dialect())
    )
    assert "ON DUPLICATE KEY UPDATE score = VALUES(score)" in my_update
    assert "is_dirty = %s" in my_update


@pytest.mark.asyncio
async def test_conflict_statement_cache():
    names = ("name", "score")
    async with base_ormar_config.database:
        ignore = insert_statement(
            model_cls=Tag, names=names, on_conflict="ignore", returning=True
        )
        assert ignore is insert_statement(
            model_cls=Tag, names=names, on_conflict="ignore", returning=True
        )
        assert ignore is not insert_statement(model_cls=Tag, names=names)
        assert ignore is not insert_statement(
            model_cls=Tag, names=names, on_conflict="ignore"
        )
        assert ignore is not insert_statement(
            model_cls=Tag, names=names, on_conflict="update"
        )


@pytest.mark.asyncio
async def test_proxy_create_ignore():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create(name="A")
            other = await Author.objects.create(name="B")
            post = await author.posts.create(title="p", on_conflict="ignore")
            assert post is not None and post.author.pk == author.pk
            assert await other.posts.create(title="p", on_conflict="ignore") is None
            assert await Post.objects.filter(author=other).count() == 0
            with pytest.raises(QueryDefinitionError):
                await post.categories.create(name="c", on_conflict="ignore")
