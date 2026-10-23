#!/bin/sh
# Prints the coverage omit patterns for the current DATABASE_URL.
# Each native driver adapter is measured only on its own database's run.

NATIVE="*/ormar/databases/native"
TESTS="*/tests/test_databases"

pg="$NATIVE/adapters/asyncpg.py,$TESTS/test_native_adapter_asyncpg.py"
mysql="$NATIVE/adapters/asyncmy.py,$TESTS/test_native_adapter_asyncmy.py"
sqlite="$NATIVE/adapters/aiosqlite.py,$TESTS/test_native_adapter_aiosqlite.py"

case "${DATABASE_URL:-sqlite}" in
    postgresql*) echo "$mysql,$sqlite" ;;
    mysql*) echo "$pg,$sqlite" ;;
    *) echo "$pg,$mysql" ;;
esac
