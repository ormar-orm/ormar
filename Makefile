test_all: test_pg test_mysql test_mysql_aiomysql test_sqlite

test_pg: export DATABASE_URL=postgresql://username:password@localhost:5432/testsuite
test_pg:
	docker-compose -f scripts/docker-compose.yml up -d postgres
	bash scripts/test.sh -svv
	docker-compose -f scripts/docker-compose.yml stop postgres

test_mysql: export DATABASE_URL=mysql://username:password@127.0.0.1:3306/testsuite
test_mysql:
	docker-compose -f "scripts/docker-compose.yml" up -d mysql
	bash scripts/test.sh -svv
	docker-compose -f scripts/docker-compose.yml stop mysql

test_mysql_aiomysql: export DATABASE_URL=mysql://username:password@127.0.0.1:3306/testsuite
test_mysql_aiomysql: export MYSQL_DRIVER=aiomysql
test_mysql_aiomysql:
	docker-compose -f "scripts/docker-compose.yml" up -d mysql
	bash scripts/test.sh -svv
	docker-compose -f scripts/docker-compose.yml stop mysql

test_sqlite:
	bash scripts/test.sh -svv

test_docs:
	bash scripts/test_docs.sh -svv

test:
	pytest -svv tests/

coverage:
	pytest --cov=ormar --cov=tests --cov-fail-under=100 --cov-report=term-missing tests

type_check:
	mkdir -p .mypy_cache && poetry run python -m mypy ormar tests --ignore-missing-imports --install-types --non-interactive

lint:
	poetry run python -m ruff check . --fix

fmt:
	poetry run python -m ruff format .

pre-commit: fmt lint type_check