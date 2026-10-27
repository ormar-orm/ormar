from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar import Prefetch
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pq_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pq_books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    year: int = ormar.Integer()
    author: Optional[Author] = ormar.ForeignKey(Author, related_name="books")


class Review(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pq_reviews")

    id: int = ormar.Integer(primary_key=True)
    stars: int = ormar.Integer()
    book: Optional[Book] = ormar.ForeignKey(Book, related_name="reviews")


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pq_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pq_posts")

    id: int = ormar.Integer(primary_key=True)
    tags = ormar.ManyToMany(Tag, related_name="posts")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (Review, Book, Author, Post, Tag):
            await model.objects.delete(each=True)
        ann = await Author.objects.create(name="Ann")
        bob = await Author.objects.create(name="Bob")
        old = await Book.objects.create(title="old", year=1990, author=ann)
        new = await Book.objects.create(title="new", year=2010, author=ann)
        await Book.objects.create(title="newer", year=2020, author=ann)
        await Book.objects.create(title="bob", year=2015, author=bob)
        await Review.objects.create(stars=5, book=new)
        await Review.objects.create(stars=1, book=new)
        await Review.objects.create(stars=4, book=old)
        post = await Post.objects.create()
        for name in ("beta", "alpha", "other"):
            await post.tags.add(await Tag.objects.create(name=name))
        yield


@pytest.mark.asyncio
async def test_filtered_and_ordered_reverse_prefetch():
    recent = Book.objects.filter(year__gte=2000).order_by("-year")
    authors = (
        await Author.objects.prefetch_related(Prefetch("books", queryset=recent))
        .order_by("name")
        .all()
    )
    assert [b.title for b in authors[0].books] == ["newer", "new"]
    assert [b.title for b in authors[1].books] == ["bob"]


@pytest.mark.asyncio
async def test_prefetch_queryset_fields_and_related_filters():
    titles_only = Book.objects.fields(["id", "title"]).filter(author__name="Ann")
    ann = await Author.objects.prefetch_related(
        Prefetch("books", queryset=titles_only)
    ).get(name="Ann")
    assert sorted(b.title for b in ann.books) == ["new", "newer", "old"]
    assert all(b.year is None for b in ann.books)


@pytest.mark.asyncio
async def test_forward_and_nested_prefetch():
    books = (
        await Book.objects.prefetch_related(
            [
                Prefetch("author", queryset=Author.objects.filter(name="Ann")),
                Prefetch("reviews", queryset=Review.objects.filter(stars__gte=4)),
            ]
        )
        .order_by("title")
        .all()
    )
    by_title = {b.title: b for b in books}
    assert by_title["new"].author.name == "Ann"
    assert by_title["bob"].author.name is None
    assert [r.stars for r in by_title["new"].reviews] == [5]
    nested = await Author.objects.prefetch_related(
        ["books", Prefetch("books__reviews", queryset=Review.objects.order_by("stars"))]
    ).get(name="Ann")
    stars = {b.title: [r.stars for r in b.reviews] for b in nested.books}
    assert stars == {"old": [4], "new": [1, 5], "newer": []}


@pytest.mark.asyncio
async def test_many_to_many_prefetch_with_queryset():
    picked = Tag.objects.filter(name__in=["alpha", "beta"]).order_by("name")
    post = await Post.objects.prefetch_related(Prefetch("tags", queryset=picked)).get()
    assert [t.name for t in post.tags] == ["alpha", "beta"]


@pytest.mark.asyncio
async def test_invalid_prefetch_querysets_raise():
    with pytest.raises(QueryDefinitionError, match="Book"):
        await Author.objects.prefetch_related(
            Prefetch("books", queryset=Author.objects.filter())
        ).all()
    for queryset in (
        Book.objects.limit(1),
        Book.objects.offset(1),
        Book.objects.prefetch_related("reviews"),
    ):
        with pytest.raises(QueryDefinitionError, match="limit"):
            await Author.objects.prefetch_related(
                Prefetch("books", queryset=queryset)
            ).all()
    with pytest.raises(QueryDefinitionError, match="many to many"):
        await Post.objects.prefetch_related(
            Prefetch("tags", queryset=Tag.objects.filter(posts__id=1))
        ).all()


@pytest.mark.asyncio
async def test_prefetch_queryset_exclude_fields_and_groups():
    ann = await Author.objects.prefetch_related(
        Prefetch("books", queryset=Book.objects.exclude_fields(["year"]))
    ).get(name="Ann")
    assert all(b.year is None and b.title for b in ann.books)
    either = Tag.objects.filter(ormar.or_(name="alpha", name__startswith="o"))
    post = await Post.objects.prefetch_related(Prefetch("tags", queryset=either)).get()
    assert sorted(t.name for t in post.tags) == ["alpha", "other"]


@pytest.mark.asyncio
async def test_prefetch_querysets_do_not_change_the_base_queryset():
    base = Author.objects.order_by("name")
    narrowed = Prefetch("books", queryset=Book.objects.fields(["id", "title"]))
    await base.prefetch_related(narrowed).all()
    authors = await base.prefetch_related("books").all()
    assert all(b.year is not None for a in authors for b in a.books)
    ann = authors[0]
    await ann.books.prefetch_related(
        Prefetch("reviews", queryset=Review.objects.fields(["id"]))
    ).all()
    books = await ann.books.prefetch_related("reviews").all()
    assert all(r.stars is not None for b in books for r in b.reviews)


@pytest.mark.asyncio
async def test_prefetch_queryset_on_a_select_related_relation_raises():
    with pytest.raises(QueryDefinitionError, match="select_related"):
        await (
            Author.objects.select_related("books")
            .prefetch_related(
                Prefetch("books", queryset=Book.objects.filter(year__gte=2015))
            )
            .all()
        )


@pytest.mark.asyncio
async def test_prefetch_queryset_ordered_through_a_relation():
    reviews = (
        await Review.objects.prefetch_related(
            Prefetch("book", queryset=Book.objects.order_by("author__name"))
        )
        .order_by("stars")
        .all()
    )
    assert [r.book.title for r in reviews] == ["new", "old", "new"]


@pytest.mark.asyncio
async def test_prefetch_relation_as_accessor_and_wrong_queryset_type():
    ann = await Author.objects.prefetch_related(
        Prefetch(Author.books, queryset=Book.objects.filter(year__gte=2015))
    ).get(name="Ann")
    assert [b.title for b in ann.books] == ["newer"]
    with pytest.raises(QueryDefinitionError, match="QuerySet"):
        Author.objects.prefetch_related(Prefetch("books", queryset=[]))
