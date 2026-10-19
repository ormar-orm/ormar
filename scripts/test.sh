#!/bin/sh -e

PACKAGE="ormar"

PREFIX=""
if [ -d 'venv' ] ; then
    PREFIX="venv/bin/"
fi

ORMAR_COV_OMIT="$(sh scripts/coverage_omit.sh)"
export ORMAR_COV_OMIT
# Every query template hit is also built fresh and compared (safety net).
export ORMAR_TEMPLATE_CROSS_CHECK=1

set -x

PYTHONPATH=. ${PREFIX}pytest --ignore venv --cov=${PACKAGE} --cov=tests --cov-report=xml --cov-fail-under=100 --cov-report=term-missing tests/ "${@}"
