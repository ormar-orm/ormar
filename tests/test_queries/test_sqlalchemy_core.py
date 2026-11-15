"""
Runs the examples of docs/queries/sqlalchemy-core.md: SQLAlchemy core
statements on ormar's connection through ``database.get_query_executor()``.
"""

import pytest
import sqlalchemy as sa

import ormar
from ormar.databases.connection import DatabaseConnection
from tests.lifespan import init_tests
from tests.settings import ASYNC_DATABASE_URL, create_config

base_ormar_config = create_config()


class Account(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="core_accounts")

    id: int = ormar.Integer(primary_key=True)
    owner: str = ormar.String(max_length=100, name="owner_name")
    balance: int = ormar.Integer(default=0)


create_test_database = init_tests(base_ormar_config)

database = Account.ormar_config.database
accounts = Account.ormar_config.table


async def add_accounts() -> None:
    await Account.objects.bulk_create(
        [
            Account(owner="Alice", balance=100),
            Account(owner="Bob", balance=0),
            Account(owner="Carol", balance=50),
        ]
    )


@pytest.mark.asyncio
async def test_reading_rows():
    async with database:
        async with database.transaction(force_rollback=True):
            await add_accounts()
            async with database.get_query_executor() as executor:
                rows = await executor.fetch_all(
                    sa.select(accounts.c.id, accounts.c.owner_name)
                    .where(accounts.c.balance > 0)
                    .order_by(accounts.c.id)
                )
                assert [row["owner_name"] for row in rows] == ["Alice", "Carol"]
                assert rows[0][accounts.c.owner_name] == "Alice"
                with pytest.raises(KeyError):
                    rows[0][0]

                row = await executor.fetch_one(
                    sa.select(
                        sa.func.count().label("total"),
                        sa.func.sum(accounts.c.balance).label("balance_sum"),
                    )
                )
                assert row is not None
                assert (row["total"], row["balance_sum"]) == (3, 150)
                assert (
                    await executor.fetch_one(
                        sa.select(accounts).where(accounts.c.owner_name == "Nobody")
                    )
                    is None
                )

                query = sa.select(accounts.c.id, accounts.c.owner_name).where(
                    accounts.c.owner_name == "Bob"
                )
                assert await executor.fetch_val(query, column=1) == "Bob"
                assert (
                    await executor.fetch_val(sa.select(sa.func.max(accounts.c.balance)))
                    == 100
                )


@pytest.mark.asyncio
async def test_fetch_val_column_without_native_execution():
    async with DatabaseConnection(
        ASYNC_DATABASE_URL, native_execution=False
    ) as fallback_database:
        async with fallback_database.get_query_executor() as executor:
            query = sa.select(sa.literal(1).label("one"), sa.literal("b").label("two"))
            assert await executor.fetch_val(query, column=1) == "b"
            assert await executor.fetch_val(query) == 1
            assert (
                await executor.fetch_val(
                    sa.select(accounts.c.id).where(accounts.c.id < 0), column=0
                )
                is None
            )


@pytest.mark.asyncio
async def test_writing_rows():
    async with database:
        async with database.transaction(force_rollback=True):
            async with database.get_query_executor() as executor:
                new_id = await executor.execute(
                    accounts.insert().values(owner_name="Dave", balance=10)
                )
                await executor.execute(
                    accounts.insert().values(owner_name="Erin", balance=10)
                )
                changed = await executor.execute(
                    accounts.update()
                    .where(accounts.c.balance == 10)
                    .values(balance=accounts.c.balance + 5)
                )
                assert changed == 2
                unchanged = await executor.execute(
                    accounts.update().where(accounts.c.id == new_id).values(balance=15)
                )
                assert unchanged == 1

                await executor.execute_many(
                    accounts.update()
                    .where(accounts.c.id == sa.bindparam("account_id"))
                    .values(balance=sa.bindparam("new_balance")),
                    [
                        {"account_id": new_id, "new_balance": 1},
                        {"account_id": new_id + 1, "new_balance": 2},
                    ],
                )

            dave = await Account.objects.get(id=new_id)
            assert (dave.owner, dave.balance) == ("Dave", 1)


@pytest.mark.asyncio
async def test_iterate():
    async with database:
        async with database.transaction(force_rollback=True):
            await add_accounts()
            owners = []
            async with database.get_query_executor(transactional=True) as executor:
                async for row in executor.iterate(
                    sa.select(accounts.c.owner_name).order_by(accounts.c.id)
                ):
                    owners.append(row["owner_name"])
            assert owners == ["Alice", "Bob", "Carol"]


async def withdraw(account_id: int, amount: int) -> None:
    async with database.transaction():
        async with database.get_query_executor() as executor:
            balance = await executor.fetch_val(
                sa.select(accounts.c.balance)
                .where(accounts.c.id == account_id)
                .with_for_update()
            )
        if balance < amount:
            raise ValueError("insufficient funds")
        await Account.objects.filter(id=account_id).update(balance=balance - amount)


@pytest.mark.asyncio
async def test_core_and_ormar_in_one_transaction():
    async with database:
        await add_accounts()
        alice = await Account.objects.get(owner="Alice")

        await withdraw(account_id=alice.id, amount=30)
        assert (await Account.objects.get(id=alice.id)).balance == 70

        with pytest.raises(ValueError):
            async with database.transaction():
                async with database.get_query_executor() as executor:
                    await executor.execute(
                        accounts.insert().values(owner_name="Temp", balance=1)
                    )
                await Account.objects.filter(id=alice.id).update(balance=0)
                raise ValueError("roll back both")
        assert await Account.objects.filter(owner="Temp").count() == 0
        assert (await Account.objects.get(id=alice.id)).balance == 70

        with pytest.raises(ValueError, match="insufficient funds"):
            await withdraw(account_id=alice.id, amount=1000)

        await Account.objects.delete(each=True)


@pytest.mark.asyncio
async def test_rows_back_to_models():
    async with database:
        async with database.transaction(force_rollback=True):
            await add_accounts()
            async with database.get_query_executor() as executor:
                rows = await executor.fetch_all(
                    sa.select(accounts)
                    .where(accounts.c.balance > 0)
                    .order_by(accounts.c.id)
                )
            models = [
                Account(**Account.translate_row_to_fields(dict(row))) for row in rows
            ]
            assert [account.owner for account in models] == ["Alice", "Carol"]

            ids = [row["id"] for row in rows]
            loaded = await Account.objects.filter(id__in=ids).order_by("id").all()
            assert loaded == models

            values = (
                await Account.objects.filter(balance__gt=0)
                .order_by("id")
                .values(["owner", "balance"])
            )
            assert values == [
                {"owner": "Alice", "balance": 100},
                {"owner": "Carol", "balance": 50},
            ]
