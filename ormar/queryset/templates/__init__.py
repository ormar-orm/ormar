"""
Statement templates: a statement built once per cache key and reused with
new values on every later call.

Most of the cost of a simple read is building the statement: ormar walks the
filters, joins, selected fields and ordering, and SQLAlchemy assembles the
expression tree. Two calls that differ only in their values build the same
tree, so ``QuerySet`` builds it once with named bind parameters ("slots") in
place of the values and keeps it in a cache of the model::

    await Author.objects.filter(name="a").limit(5).all()
    # built and cached: SELECT ... WHERE authors.name = :ormar_p0 LIMIT :ormar_limit
    # bound this call:  {"ormar_p0": "a", "ormar_limit": 5}

    await Author.objects.filter(name="b").limit(10).all()
    # same cache key, statement reused: {"ormar_p0": "b", "ormar_limit": 10}

The cache key (see ``QuerySet.select_cache_key``) holds everything that
changes the SQL text and none of the bound values: filtered columns,
operators, and/or/not nesting, ``select_related``, selected fields, ordering
and whether a limit or offset is present. Values SQLAlchemy writes into the
SQL text instead of binding (``None`` as ``IS NULL``, booleans, ``isnull``)
are part of the key as well.

Writes are cached the same way. ``Model.save()``, ``update()``, ``delete()``,
``load()`` and ``upsert()`` run one statement per model and set of written
columns, binding the values as ``ormar_v{i}`` (in sorted column order) and the
primary key as ``ormar_pk``::

    await Author(name="a", score=1).save()
    # INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
    # {"ormar_v0": "a", "ormar_v1": 1}

``QuerySet.update()`` and ``delete()`` bind both filter and value slots, and
``bulk_create`` and ``bulk_update`` reuse one statement per set of columns for
all their rows.

Running the same statement object every time also lets the native compiler
reuse its plan for it instead of compiling the statement again.

The package is split by role:

- ``cache`` - the per-model template cache
- ``cache_keys`` - what makes two querysets share a statement
- ``slots`` - named bind parameters and the values collected per call
- ``reads`` - statements wrapped around a select, clean-up of templates
- ``writes`` - insert and upsert templates (updates, deletes and selects by
  primary key: ``ormar.queryset.write_statements``)
"""

from ormar.queryset.templates.cache import (
    StatementTemplate,
    TemplateCache,
    cached_statement,
    template_cache_for,
)
from ormar.queryset.templates.cache_keys import (
    action_cache_key,
    clauses_cache_key,
    excludable_cache_key,
    order_cache_key,
    value_cache_key,
)
from ormar.queryset.templates.reads import (
    aggregate_statement,
    count_statement,
    exists_statement,
    release_values,
)
from ormar.queryset.templates.slots import (
    LIMIT_SLOT,
    OFFSET_SLOT,
    PK_SLOT,
    PKS_SLOT,
    SlotBinder,
    action_params,
    canonical_actions,
    iter_actions,
    template_params,
    value_binds,
    value_params,
)
from ormar.queryset.templates.writes import (
    build_insert,
    build_update_assignment,
    conflict_target_columns,
    insert_statement,
    insert_template,
    update_assignment_params,
    update_list_keys,
    update_values_key,
    update_values_params,
)

__all__ = [
    "LIMIT_SLOT",
    "OFFSET_SLOT",
    "PK_SLOT",
    "PKS_SLOT",
    "SlotBinder",
    "StatementTemplate",
    "TemplateCache",
    "action_cache_key",
    "action_params",
    "aggregate_statement",
    "build_insert",
    "build_update_assignment",
    "conflict_target_columns",
    "cached_statement",
    "canonical_actions",
    "clauses_cache_key",
    "count_statement",
    "excludable_cache_key",
    "exists_statement",
    "insert_statement",
    "insert_template",
    "iter_actions",
    "order_cache_key",
    "release_values",
    "template_cache_for",
    "template_params",
    "update_assignment_params",
    "update_list_keys",
    "update_values_key",
    "update_values_params",
    "value_binds",
    "value_cache_key",
    "value_params",
]
