from typing import Any, Optional

import pytest
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import Coalesce, F, Greatest, Inserted, Random
from ormar.databases.dialects import MysqlRules, PostgresRules, SqliteRules
from ormar.databases.query_executor import QueryExecutor
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import (
    build_update_assignment,
    insert_statement,
    template_cache_for,
)
from ormar.queryset.templates.writes import build_conflict_insert
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="upx_tracks")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Progress(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="upx_progress")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=20, unique=True)
    completed: int = ormar.Integer(default=1)
    best: int = ormar.Integer(default=0, name="best_score")
    last_read: Optional[int] = ormar.Integer(nullable=True)
    label: str = ormar.String(max_length=20, default="")
    revision: int = ormar.Integer(default=0, on_update=7)
    track: Optional[Track] = ormar.ForeignKey(
        Track, nullable=True, related_name="progress"
    )


create_test_database = init_tests(base_ormar_config)

BY_CODE = dict(on_conflict="update", conflict_target=["code"])


@pytest.fixture()
def executed(monkeypatch):
    calls: list[str] = []
    for method_name in ("execute", "execute_many"):
        original = getattr(QueryExecutor, method_name)

        def recording(self, *args, _original=original, _name=method_name, **kwargs):
            calls.append(_name)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(QueryExecutor, method_name, recording)
    return calls


async def stored(code: str) -> Progress:
    return await Progress.objects.get(code=code)


@pytest.mark.asyncio
async def test_counter_increments_on_every_conflict():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            for expected in (1, 2, 3):
                progress = await Progress.objects.create(
                    code="a",
                    completed=1,
                    label=f"call {expected}",
                    update_values={"completed": F("completed") + 1},
                    **BY_CODE,
                )
                assert progress.completed == expected
                assert (await stored("a")).completed == expected
            # update_fields default to none: the label stays the inserted one,
            # revision gets its on_update value
            assert (progress.label, progress.revision) == ("call 1", 7)
            accessor = await Progress.objects.create(
                code="a", update_values={"completed": Progress.completed * 2}, **BY_CODE
            )
            assert accessor.completed == 6


@pytest.mark.asyncio
async def test_greatest_of_existing_and_inserted():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            never_back = {"best": Greatest(F("best"), Inserted("best"))}
            for best, expected in ((5, 5), (3, 5), (8, 8)):
                progress = await Progress.objects.create(
                    code="a", best=best, update_values=never_back, **BY_CODE
                )
                assert progress.best == expected
            assert (await stored("a")).best == 8


@pytest.mark.asyncio
async def test_coalesce_keeps_the_first_value():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first_read = {"last_read": Coalesce(F("last_read"), Inserted("last_read"))}
            for last_read, expected in ((None, None), (4, 4), (9, 4)):
                progress = await Progress.objects.create(
                    code="a", last_read=last_read, update_values=first_read, **BY_CODE
                )
                assert progress.last_read == expected
            counted = await Progress.objects.create(
                code="b",
                update_values={"last_read": Coalesce(F("last_read"), 0) + 1},
                **BY_CODE,
            )
            assert counted.last_read is None
            counted = await Progress.objects.create(
                code="b",
                update_values={"last_read": Coalesce(F("last_read"), 0) + 1},
                **BY_CODE,
            )
            assert counted.last_read == 1


@pytest.mark.asyncio
async def test_update_fields_values_and_on_update_together():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Progress.objects.create(code="a", completed=2, best=1, label="old")
            progress = await Progress.objects.create(
                code="a",
                completed=3,
                best=9,
                label="new",
                last_read=5,
                update_fields=["label"],
                update_values={
                    "completed": F("completed") + Inserted("completed") * 10,
                    "last_read": 11,
                },
                **BY_CODE,
            )
            assert (
                progress.completed,
                progress.best,
                progress.label,
                progress.last_read,
                progress.revision,
            ) == (32, 1, "new", 11, 7)
            # a field with on_update named in update_values takes that value
            progress = await Progress.objects.create(
                code="a",
                update_values={"revision": F("revision") + 100, "best": Random() * 0},
                **BY_CODE,
            )
            assert (progress.revision, progress.best, progress.label) == (107, 0, "new")


@pytest.mark.asyncio
async def test_bulk_create_with_expressions(executed):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Progress.objects.bulk_create(
                [Progress(code=code, completed=1, best=5) for code in ("a", "b")]
            )
            executed.clear()
            await Progress.objects.bulk_create(
                [
                    Progress(code="a", completed=2, best=3),
                    Progress(code="b", completed=4, best=9),
                    Progress(code="c", completed=6, best=1),
                ],
                update_values={
                    "completed": F("completed") + Inserted("completed") + 100,
                    "best": Greatest(F("best"), Inserted("best"), 4),
                    "label": "merged",
                },
                **BY_CODE,
            )
            assert executed.count("execute_many") == 1
            # MySQL sets the bound update values in user variables first
            assert executed.count("execute") == (
                1 if DATABASE_URL.startswith("mysql") else 0
            )
            rows = await Progress.objects.order_by("code").values_list(
                ["code", "completed", "best", "label", "revision"]
            )
            assert rows == [
                ("a", 103, 5, "merged", 7),
                ("b", 105, 9, "merged", 7),
                ("c", 6, 1, "", 0),
            ]


@pytest.mark.asyncio
async def test_bulk_create_with_expressions_and_mixed_key_sets():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Progress.objects.create(code="a")
            await Progress.objects.create(code="b")
            await Progress.objects.bulk_create(
                [
                    Progress(id=first.pk, code="a"),
                    Progress(code="b"),
                    Progress(code="c"),
                ],
                update_values={"completed": F("completed") + 10},
                **BY_CODE,
            )
            rows = await Progress.objects.order_by("code").values_list(
                ["code", "completed", "revision"]
            )
            assert rows == [("a", 11, 7), ("b", 11, 7), ("c", 1, 0)]


@pytest.mark.asyncio
async def test_proxy_create_with_update_values():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Track.objects.create(name="first")
            second = await Track.objects.create(name="second")
            increment = {"completed": F("completed") + 1}
            progress = await first.progress.create(
                code="a", update_values=increment, **BY_CODE
            )
            assert progress.track.pk == first.pk and progress.completed == 1
            progress = await first.progress.create(
                code="a", update_values=increment, **BY_CODE
            )
            assert progress.completed == 2
            # a conflicting row of another owner moves to this one
            moved = await second.progress.create(
                code="a", update_values=increment, **BY_CODE
            )
            assert moved.completed == 3 and moved.track.pk == second.pk
            assert (await stored("a")).track.pk == second.pk
            assert await first.progress.count() == 0


def inserts(cache) -> list:
    return [key for key in cache.entries if key[0] == "insert"]


@pytest.mark.asyncio
async def test_statement_cached_per_expression_shape():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            cache = template_cache_for(model_cls=Progress)
            cache.clear()
            for step in (1, 5, 9):
                await Progress.objects.create(
                    code="a",
                    update_values={"completed": F("completed") + step},
                    **BY_CODE,
                )
            assert len(inserts(cache)) == 1
            assert (await stored("a")).completed == 15
            await Progress.objects.create(
                code="a", update_values={"completed": F("completed") * 2}, **BY_CODE
            )
            await Progress.objects.create(
                code="a", update_values={"completed": 3}, **BY_CODE
            )
            await Progress.objects.create(
                code="a", update_values={"completed": 4}, **BY_CODE
            )
            assert len(inserts(cache)) == 3
            assert (await stored("a")).completed == 4

    names = ("code", "completed")
    increment = {"completed": F("completed") + 1}
    statement = insert_statement(
        model_cls=Progress, names=names, update_values=increment, **BY_CODE
    )
    assert statement is insert_statement(
        model_cls=Progress,
        names=names,
        update_values={"completed": F("completed") + 2},
        **BY_CODE,
    )
    assert statement is not insert_statement(
        model_cls=Progress,
        names=names,
        update_values={"completed": F("completed") + 2.5},
        **BY_CODE,
    )
    assert statement is not insert_statement(
        model_cls=Progress,
        names=names,
        update_values={"completed": F("best") + 1},
        **BY_CODE,
    )
    assert statement is not insert_statement(
        model_cls=Progress, names=names, update_fields=["completed"], **BY_CODE
    )


def compiled(rules, dialect, update_values, update_variables=False) -> str:
    statement: Any = build_conflict_insert(
        table=Progress.ormar_config.table,
        names=("code", "completed"),
        rules=rules,
        on_conflict="update",
        target=("code",),
        update_names=(),
        on_update=("revision",),
        returning=(),
        model=Progress,
        update_values=update_values,
        update_variables=update_variables,
    )
    return str(statement.compile(dialect=dialect))


def test_expressions_compile_against_the_table_and_the_inserted_row():
    update_values = {
        "best_score": Greatest(F("best"), Inserted("best")),
        "completed": F("completed") + 1,
        "label": "x",
    }
    sql = compiled(
        PostgresRules(backend_name="postgresql"), postgresql.dialect(), update_values
    )
    assert (
        "DO UPDATE SET completed = (upx_progress.completed + %(ormar_u1_0)s::INTEGER), "
        "best_score = greatest(upx_progress.best_score, excluded.best_score), "
        "label = %(ormar_u2)s::VARCHAR, "
        "revision = %(ormar_on_update_revision)s::INTEGER"
    ) in sql
    sql = compiled(SqliteRules(backend_name="sqlite"), sqlite.dialect(), update_values)
    assert "best_score = max(upx_progress.best_score, excluded.best_score)" in sql
    sql = compiled(MysqlRules(backend_name="mysql"), mysql.dialect(), update_values)
    assert sql.endswith(
        "ON DUPLICATE KEY UPDATE completed = (upx_progress.completed + %s), "
        "best_score = greatest(upx_progress.best_score, VALUES(best_score)), "
        "label = %s, revision = %s"
    )
    sql = compiled(
        MysqlRules(backend_name="mysql"),
        mysql.dialect(),
        update_values,
        update_variables=True,
    )
    assert sql.endswith(
        "ON DUPLICATE KEY UPDATE completed = (upx_progress.completed + @ormar_u1_0), "
        "best_score = greatest(upx_progress.best_score, VALUES(best_score)), "
        "label = @ormar_u2, revision = @ormar_on_update_0"
    )
    assignment = build_update_assignment(
        model=Progress, on_update=("revision",), update_values=update_values
    )
    assert str(assignment.compile(dialect=mysql.dialect())) == (
        "SET @ormar_on_update_0 = %s, @ormar_u1_0 = %s, @ormar_u2 = %s"
    )


@pytest.mark.asyncio
async def test_update_values_errors():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            track = await Track.objects.create(name="t")
            invalid = [
                ({"nope": 1}, None, r"does not have: \['nope'\]"),
                ({"label": "x"}, ["label"], r"both update_fields and update_values"),
                ({"completed": F("track__name")}, None, "reads a related model"),
                ({"completed": Inserted("track__id")}, None, "reads a related model"),
                ({"completed": F("nope") + 1}, None, "non-relation field"),
                ({"completed": Inserted("track")}, None, "non-relation field"),
                ({"track": F("id")}, None, "set no relations"),
                ({"completed": F("best")}, ["best"], "read no fields set"),
                ({"completed": F("revision")}, None, "read no fields set"),
            ]
            for update_values, update_fields, message in invalid:
                with pytest.raises(QueryDefinitionError, match=message):
                    await Progress.objects.create(
                        code="a",
                        update_values=update_values,
                        update_fields=update_fields,
                        **BY_CODE,
                    )
                with pytest.raises(QueryDefinitionError, match=message):
                    await Progress.objects.bulk_create(
                        [Progress(code="a")],
                        update_values=update_values,
                        update_fields=update_fields,
                        **BY_CODE,
                    )
            assert await Progress.objects.count() == 0

            with pytest.raises(QueryDefinitionError, match="need on_conflict"):
                await Progress.objects.create(
                    code="a", update_values={"completed": 1}, on_conflict="ignore"
                )
            with pytest.raises(QueryDefinitionError, match="need on_conflict"):
                await Progress.objects.bulk_create(
                    [Progress(code="a")], update_values={"completed": 1}
                )
            with pytest.raises(QueryDefinitionError, match="need on_conflict"):
                await track.progress.create(code="a", update_values={"completed": 1})

            outside = "only allowed in update_values"
            with pytest.raises(QueryDefinitionError, match=outside):
                await Progress.objects.filter(completed=Inserted("best")).all()
            with pytest.raises(QueryDefinitionError, match=outside):
                await Progress.objects.update(each=True, completed=Inserted("best"))
            # a literal operand goes on the right, as in update()
            with pytest.raises(TypeError):
                1 + F("completed")  # type: ignore[operator]
