# SQLAlchemy core statements

When a query is easier to write in SQLAlchemy core than with the `QuerySet` API,
run it on ormar's own connection with `database.get_query_executor()`. The statement
runs on the same connection pool and, inside `database.transaction()`, on the same
connection and in the same transaction as ormar's queries.

The examples below use this model:

```python
import sqlalchemy as sa
import ormar


class Account(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="accounts")

    id: int = ormar.Integer(primary_key=True)
    owner: str = ormar.String(max_length=100, name="owner_name")
    balance: int = ormar.Integer(default=0)


database = Account.ormar_config.database
accounts = Account.ormar_config.table
```

## The model's table

`Model.ormar_config.table` is the `sqlalchemy.Table` of the model. Its columns are
named like the columns in the database, so a field declared with `name=` is reached
by that name: the `owner` field above is `accounts.c.owner_name`.

## Query executor

`get_query_executor()` is an async context manager that yields an executor bound to
a connection:

```python
async with database.get_query_executor() as executor:
    rows = await executor.fetch_all(
        sa.select(accounts.c.id, accounts.c.owner_name)
        .where(accounts.c.balance > 0)
        .order_by(accounts.c.id)
    )
```

Where the statements run:

* inside `database.transaction()`: on the transaction's connection, so they see the
  transaction's uncommitted changes and are committed or rolled back with it,
* outside a transaction: on a pooled connection in autocommit mode, each statement is
  committed on its own,
* with `get_query_executor(transactional=True)` outside a transaction: in a
  transaction opened for the `async with` block (inside a transaction the parameter
  changes nothing).

Every method takes a SQLAlchemy statement and optional `params`, values of named
`bindparam()`s of the statement.

| Method | Returns |
| ------ | ------- |
| `fetch_all(query, params=None)` | list of rows |
| `fetch_one(query, params=None)` | first row or `None` |
| `fetch_val(query, column=0, params=None)` | value of the column at position `column` of the first row, or `None` without rows |
| `execute(query, params=None)` | for an `INSERT` the primary key of the inserted row, for `UPDATE` / `DELETE` the number of matched rows (also those already holding the new values) |
| `execute_many(query, values)` | `None`; runs the statement once per mapping in `values` |
| `iterate(query, params=None)` | async iterator over the rows, fetched in batches |

### Reading rows

Rows are mappings keyed by the result labels: the column name or the name given with
`.label()`. The selected column objects work as keys too. Rows are not tuples, so
`row[0]` raises a `KeyError`; to read a column by position use
`fetch_val(query, column=...)`.

```python
async with database.get_query_executor() as executor:
    rows = await executor.fetch_all(
        sa.select(accounts.c.id, accounts.c.owner_name).order_by(accounts.c.id)
    )
    rows[0]["owner_name"]  # 'Alice'
    rows[0][accounts.c.owner_name]  # 'Alice'

    row = await executor.fetch_one(
        sa.select(
            sa.func.count().label("total"),
            sa.func.sum(accounts.c.balance).label("balance_sum"),
        )
    )
    row["total"], row["balance_sum"]  # (3, 150)

    query = sa.select(accounts.c.id, accounts.c.owner_name).where(
        accounts.c.owner_name == "Bob"
    )
    await executor.fetch_val(query, column=1)  # 'Bob'
    await executor.fetch_val(sa.select(sa.func.max(accounts.c.balance)))  # 100
```

Treat rows as read-only, use `dict(row)` for a copy you can change.

### Writing rows

```python
async with database.get_query_executor() as executor:
    new_id = await executor.execute(
        accounts.insert().values(owner_name="Dave", balance=10)
    )
    changed = await executor.execute(
        accounts.update()
        .where(accounts.c.balance == 10)
        .values(balance=accounts.c.balance + 5)
    )

async with database.get_query_executor(transactional=True) as executor:
    await executor.execute_many(
        accounts.update()
        .where(accounts.c.id == sa.bindparam("account_id"))
        .values(balance=sa.bindparam("new_balance")),
        [
            {"account_id": 1, "new_balance": 1},
            {"account_id": 2, "new_balance": 2},
        ],
    )
```

Outside a transaction each row of `execute_many()` can be committed on its own, use
`transactional=True` (or `database.transaction()`) to make the batch atomic.

!!!warning
    Statements run through the executor skip what ormar does in Python: field
    validation, `default` and `on_update` values of fields and signals. Of the
    defaults only `server_default` applies.

### Iterating

`iterate()` fetches the rows in batches instead of loading them all at once. Some
drivers (asyncpg) stream only inside a transaction, so open one:

```python
async with database.get_query_executor(transactional=True) as executor:
    async for row in executor.iterate(
        sa.select(accounts.c.owner_name).order_by(accounts.c.id)
    ):
        print(row["owner_name"])
```

## In one transaction with ormar

Inside `database.transaction()` core statements and ormar queries share the
connection, so you can lock a row with core and update it with ormar, and both are
committed or rolled back together:

```python
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
```

The `ValueError` rolls back the transaction. For row locks on ormar's own queries see
[select_for_update](../transactions.md#row-locks-with-select_for_update).

## Rows back to models

A row with all columns of the model's table becomes a model with
`translate_row_to_fields()`, which maps the column names to field names:

```python
async with database.get_query_executor() as executor:
    rows = await executor.fetch_all(
        sa.select(accounts).where(accounts.c.balance > 0).order_by(accounts.c.id)
    )
models = [Account(**Account.translate_row_to_fields(dict(row))) for row in rows]
```

Or select only the primary keys with core and load the models with ormar, which also
runs `select_related()` and the other `QuerySet` options:

```python
ids = [row["id"] for row in rows]
models = await Account.objects.filter(id__in=ids).order_by("id").all()
```

When you need plain data keyed by field names rather than models, `values()` and
`values_list()` skip core altogether, see [raw data](./raw-data.md):

```python
await Account.objects.filter(balance__gt=0).order_by("id").values(["owner", "balance"])
# [{'owner': 'Alice', 'balance': 100}, {'owner': 'Carol', 'balance': 50}]
```
