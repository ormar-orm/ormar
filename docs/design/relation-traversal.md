# Relation traversal without a precomputed relation map

Status: proposal, prototype on branch `proposal/instance-traversal`.

## Current state

ormar registers every relation on both sides (`Book.author` and `Author.books`),
so the relation graph of any real schema has cycles. Four features need to know
which relations to follow without looping:

| feature | uses |
|---|---|
| `model_dump()` (and FastAPI responses) | `_relation_tree()` as the default `relation_map` |
| `save_related(follow=True)` | `_relation_tree()` |
| `get_pydantic()` | `_relation_tree()` |
| `select_all(follow=True)`, `load_all(follow=True)` | `_iterate_related_models()` (list of `select_related` paths) |

SQL aliases do not use any of this; the `AliasManager` keys aliases per
(table, relation).

Both maps are computed per model class and cached on it (`__relation_tree__`,
`__relation_map__`). The rule is "follow a relation unless its target model is
already on the path above the current model" (a direct self relation is followed
once). #1996 made `_relation_tree()` small and fast by sharing subtrees, but it
kept an older bug of the memo it reuses:

- a `(model, relation)` subtree is built once, under the ancestry it met first, and
  reused under every other ancestry. In graphs of 4+ mutually related models some
  valid paths are missing and some invalid ones (with a model twice on the path)
  are added;
- the memo key is the string `"<model>_<relation>"`, which can collide
  (`a_b` + `c` vs `a` + `b_c`).

`get_pydantic()` has a related bug: its cache key `(cls, include, exclude, flatten)`
ignores the relation map, so the nested schema built first for a class is reused
everywhere. `Author.get_pydantic()` followed by `Book.get_pydantic()` returns a
`Book` model without its `author` field.

### Why a type-level map cannot be both correct and cheap

The correct semantics depend on the whole ancestry, so the subtree of
`(model, relation)` cannot be shared between ancestries. Measured on a real
73-model application graph (Shotris):

| | value |
|---|---|
| correct paths, worst model | 1.32 M |
| correct paths, all models | 40 M |
| building the correct tree for one model, with memo per ancestry | 8 s, 113 MB |
| #1996 shared tree, all 73 models | 233 ms, 1.84 MB retained (but wrong) |
| `_iterate_related_models()` for `follow=True`, all 73 models | 11.6 s, **807 MB retained**, 4.4 M paths (249 k for the worst model) |

So the map is either wrong (shared subtrees) or exponential (per ancestry). The
`follow=True` path list is already exponential today: one `load_all(follow=True)`
on the worst model would ask for ~250 k joins.

## How other libraries handle it

Checked against the sources and docs (versions pinned in the links).

**SQLAlchemy 2.0.** No built-in to-dict. The FAQ "walk all related objects"
recipe keeps a `seen` set of instances
([faq/sessions](https://docs.sqlalchemy.org/en/20/faq/sessions.html#how-do-i-walk-all-objects-that-are-related-to-a-given-object)).
Relationships are lazy by default; eager loading is opt-in per path
(`selectinload`, `joinedload`, wildcard `"*"`). The wildcard is *not* one level:
its loader key `relationship:_sa_default` is not path-qualified
(`PropRegistry._default_path_loader_key` in `orm/path_registry.py`,
`_get_context_loader` in `orm/interfaces.py`), so it applies to relationships at
every depth. Checked on 2.0.55: `select(Chapter).options(joinedload("*"))` emits 3
joins (Chapter→Book→Writer→Country) and stops at `Writer.books`, because Book is
already on the path; `selectinload("*")` runs 4 statements. It follows reverse
collections too. Eager loading configured on the
relationship stops when the loader path already contains the target mapper, or
at `join_depth` for self-referential relationships
(`orm/strategies.py`, `JoinedLoader.setup_query`,
[self_referential](https://docs.sqlalchemy.org/en/20/orm/self_referential.html)).
Cascades walk instances with a visited set of `InstanceState`s
(`Mapper.cascade_iterator`, `orm/mapper.py`). Nothing is precomputed per class
beyond the mapper.

**Tortoise ORM 1.1.** `pydantic_model_creator` walks the model types with a stack
and returns no submodel when the model is already on the stack
(`allow_cycles=False`, the default) or when `max_recursion` (default 3, set in
`PydanticMeta`) is reached; the field is then dropped. Generated models are cached
by a hash of the arguments
([creator.py](https://github.com/tortoise/tortoise-orm/blob/1.1.9/tortoise/contrib/pydantic/creator.py)).

**Peewee 3.17.** `playhouse.shortcuts.model_to_dict` walks the loaded instances.
It cuts cycles per field, not per instance: `seen` is a set of `ForeignKeyField`s
already followed, a backref child excludes the FK pointing back, and `max_depth`
limits depth (unlimited by default). It does not follow back-references unless
`backrefs=True` (default `False`), and many to many only with `manytomany=True`
(default `False`)
([shortcuts.py](https://github.com/coleifer/peewee/blob/3.17.8/playhouse/shortcuts.py)).

**Django + DRF.** Django has no nested serialization. `select_related()` without
arguments follows only non-null forward FKs (`select_related_descend`:
`if not restricted: return not field.null`) and stops at depth 5
(`Query.max_depth = 5`, "arbitrary limit … to prevent infinite recursion";
`SQLCompiler.get_related_selections` bails when `cur_depth > max_depth`).
Reverse relations are only walked when named explicitly, and only unique ones
(reverse one-to-one); many to many never. `select_related` is "limited to
single-valued relationships"
([docs](https://docs.djangoproject.com/en/5.1/ref/models/querysets/#select-related)).
DRF `ModelSerializer.Meta.depth` generates nested serializers for forward
relations only (reverse relations must be listed explicitly), depth 0–10
([serializers](https://www.django-rest-framework.org/api-guide/serializers/#specifying-nested-serialization),
`build_nested_field`). Recursive structures need explicit serializers.

**Pydantic / FastAPI / SQLModel.** Recursive models are emitted as `$defs`/`$ref`,
and FastAPI turns them into `components/schemas`. Pydantic serialization of a
cyclic instance graph raises `Circular reference detected (id repeated)`; the
guard is per path (`pydantic-core`, `Extra::recursion_guard`). SQLModel documents
separate read models per direction (`HeroPublicWithTeam`, `TeamPublicWithHeroes`)
([docs](https://sqlmodel.tiangolo.com/tutorial/fastapi/relationships/)).

Common pattern: nobody precomputes a path tree per class. Instance walks keep a
visited set or the current path; schema generators cut at a model type on the path
and/or a depth limit; loaders follow explicit paths or a depth limit, and the "load everything"
defaults of Django and Peewee stay on single-valued (forward) relations.
SQLAlchemy's wildcard is the exception: every depth, every relationship, cut only
at a mapper already on the path.

## Proposed design

### `model_dump()` and `save_related()`: walk the loaded instances

The decision "follow this relation?" only needs the current model class and the
*set* of model classes above it on the path, and the walk only descends where an
instance is actually loaded. So instead of a cached tree the default
`relation_map` becomes a `PathRelationMap` node for (model, models above).

- The result is exactly the correct semantics (no shared-ancestry bug, no key
  collision).
- A node depends only on that key, so nodes are shared between rows and dumps:
  each class keeps at most `PATH_NODE_CACHE_SIZE = 256` of them in a plain dict
  (oldest dropped first). A dump of a row creates no objects for the traversal,
  only dict lookups.
- The node is a `dict` subclass whose keys are the followed relations, so
  `relation in node`, `bool(node)` and `len(node)` run at plain dict speed; only
  `node[relation]` (once per descent) is Python code. A leaf is `...` like in the
  old tree, so the existing dump code, include/exclude, `flatten_fields`,
  `exclude_list`, through models and an explicit `relation_map=` are unchanged.
- Memory is bounded: at most 256 nodes per class whatever the data, plus one
  dict entry per relation for the relation targets. Nothing grows with rows or
  with the number of distinct paths beyond the cap; a workload wider than the cap
  rebuilds evicted nodes (correct, just slower).
- Invalidated with `RelationGraph.version` like the old caches.

`save_related(follow=True)` uses the same nodes, so it saves exactly what a dump
would show.

### `get_pydantic()`: named, recursive, reusable models

A schema is built per *variant*: (model, relation omitted because it points back
to the parent, include/exclude/flatten selector still in scope, remaining depth,
root or nested). A relation field points to the variant of its target through a
forward reference, so a cycle becomes a recursive model (`$ref` in OpenAPI)
instead of one copy of the schema per path. The number of models grows with the
(model, relation) pairs, not with the paths, and variants are cached and shared
between `get_pydantic()` calls.

- Names are readable and stable: `Book`, `Author_without_books`,
  `Book_without_author_2` for a variant with another selector.
- The nested schema keeps today's rule that the back-reference to the parent is
  left out (switchable, see below).
- A dump stops where a model is already on the path, while a recursive schema
  allows any depth, so relation fields of nested schemas are optional with a
  `None` default. The root keeps today's required/optional rules.
- `max_depth=` limits nesting (like Tortoise's `max_recursion`); with a limit the
  schema graph is acyclic.
- `omit_back_relations=False` gives one nested model per model with every relation
  optional, the cheapest form.
- Generated models live in one module (`ormar.generated_schemas`) with
  `defer_build=True`, so their forward references resolve whenever pydantic builds
  them.

This also fixes the cache bug above, because the variant key contains the omitted
relation and the selector.

The OpenAPI cost of the variants is the open point (numbers below): with
back-relation variants and no depth limit the schema graph of a dense application
is one big strongly connected component, and pydantic's JSON-schema generation
walks it depth first. On the Shotris graph that needs a recursion limit above
the default 1000 and takes ~50 s for 73 routes. Without back-relation variants, or
with `max_depth`, it is 0.3–7 s and works with the default limit.

### `follow=True`: forward foreign keys with a depth limit

A forward FK (or one-to-one) has one parent per row, so joining it never
multiplies rows; a reverse FK or a many to many does. Today `follow=True` follows
every relation, which on dense graphs is both exponential to plan and multiplies
rows.

Proposed default (prototype, instance mode): `follow=True` follows forward FKs
(nullable ones too) up to `max_depth` (default `DEFAULT_FOLLOW_DEPTH = 5`, like
Django). Reverse and many to many relations are loaded only by explicit
`select_related` / `prefetch_related` or with `follow_reverse=N`, which also follows
them in the first N levels (`True` = 1). The same path rule applies (no model twice
above).

Joins per root on the Shotris graph (73 models; a join = one distinct relation
path prefix):

| rule | joins, worst model | median | total, 73 roots | plan build, all 73 |
|---|---|---|---|---|
| forward FK, depth 1 | 4 | 1 | 108 | 4 ms |
| forward FK, depth 2 | 14 | 2 | 214 | 8 ms |
| forward FK, depth 3 | 26 | 3 | 316 | 12 ms |
| forward FK, depth 4 | 36 | 3 | 385 | 17 ms |
| **forward FK, depth 5** | **41** | **3** | **420** | **20 ms** |
| forward FK, depth 7 (saturated) | 44 | 3 | 431 | 22 ms |
| forward non-null FK (Django), depth 1–5 | 4 / 8 / 10 / 12 / 13 | 1 / 2 / 3 / 3 / 3 | 84 / 146 / 178 / 189 / 192 | 3–12 ms |
| forward FK depth 3 + reverse/M2M at level 1 | 115 | 4 | 666 | 20 ms |
| forward FK depth 5 + reverse/M2M at level 1 | 169 | 4 | 937 | 35 ms |
| forward FK depth 5 + reverse/M2M at levels 1–2 | 294 | 148 | 8 630 | 177 ms |
| all relations, depth 1 / 2 / 3 / 4 | 40 / 117 / 639 / 2 036 | 2 / 40 / 143 / 728 | 216 / 2 668 / 11 350 / 53 280 | 4 / 26 / 112 / 510 ms |
| all relations, today | 382 957 | 80 266 | 7 104 427 | 12.1 s, 207 MiB peak for one root |

Building the SQL for the worst root takes 4 ms for forward FK depth 5 and 85 ms
for all relations at depth 3. At depth 4 one all-relations plan cannot be built at
all: the `AliasManager` raises an alias collision
(`actor__training_invitations__training`), because aliases are per (table,
relation) and the same relation appears on two paths.

Rows: on a small graph (2 manuscripts, each with 2 FKs to the same writer and 3
labels) the raw SQL returns 2 rows for the base query, 2 rows for
`select_all(follow=True)` (forward FKs) and 6 with `follow_reverse=True`
(`test_forward_follow_does_not_multiply_rows`).

### Result

`__relation_map__`, `__relation_tree__`, `build_relation_tree`,
`_iterate_related_models`, `NodeList` and the relation-graph cache invalidation for
them go away, and with them the shared-ancestry bug and the key collision.

## Benchmarks (prototype)

Python 3.11, pydantic 2.14, shared and busy machine, `nice`; numbers are the best of
3 runs. `map` = today; `map + correct tree` = today's dump code with the correct
tree passed as an explicit `relation_map` (same output as `instance`, so the ratio
is the pure traversal cost); `instance` = shared path nodes.

`model_dump()` per row, instances built in memory:

| case | depth loaded | map µs | map + correct tree µs | instance µs | instance / map + correct | dicts map / new | µs per dict map / instance |
|---|---|---|---|---|---|---|---|
| chain Chapter→Book→Writer→Country | 1 | 14.9 | 13.9 | 14.9 | 1.07 | 2 / 2 | 7.5 / 7.5 |
| | 2 | 22.4 | 22.2 | 22.9 | 1.03 | 3 / 3 | 7.5 / 7.6 |
| | 3 | 29.1 | 28.3 | 28.8 | 1.02 | 4 / 4 | 7.3 / 7.2 |
| dense 6-model graph (FK + M2M to every other) | 1 | 145 | 145 | 151 | 1.04 | 6 / 6 | 24.2 / 25.2 |
| | 2 | 347 | 369 | 381 | 1.03 | 16 / 16 | 21.7 / 23.8 |
| | 3 | 346 | 560 | 567 | 1.01 | 16 / 26 | 21.6 / 21.8 |

On the dense graph the old map drops valid paths: at depth 2 it leaves out
relation keys of the third level (same 16 dicts, fewer keys), at depth 3 whole
nested dicts (16 instead of 26). That extra, correct output is the whole gap
between `map` and `instance` there; per produced dict `instance` is within 1–10 %.

An earlier version of the prototype (a new `Mapping` node per descent, an ABC
`isinstance` in `skip_ellipsis`) was 15–20 % slower on the chain and up to 2.3×
on the dense graph; profiling showed node allocation and the ABC check, both gone.

1,000,000 rows dumped one by one (pool of 10,000 pre-built rows: Book + author +
country + 3 chapters + 2 tags, 8 dicts per row), tracemalloc on:

| | map | instance |
|---|---|---|
| transient peak per row | 3.5 KB | 3.6 KB |
| retained growth from row 200k to 1M | 736 B | 736 B |
| wall time with tracemalloc | 265 s | 258 s |

The 736 B is the list of samples; nothing grows with the row count.

Per-class memory on the Shotris graph (73 models):

| | map | instance |
|---|---|---|
| default dump map | `_relation_tree()` 1.84 MB, 233 ms | relation index 58 KiB + node caches |
| node caches, every root walked 3 / 4 levels deep | | 2 463 / 8 485 nodes, 1.6 / 5.6 MiB |
| node caches at the cap (every root walked 7 levels, 63 classes full) | | 16 138 nodes, 17.4 MiB, the hard upper bound for this graph |
| `follow=True` paths | 807 MB, 11.6 s | nothing kept |

`get_pydantic()` for all 73 Shotris models, then FastAPI `openapi()` with one route
per model; every document validates with `openapi-spec-validator` (OpenAPI 3.1):

| variant | build | models | `openapi()` | components | size |
|---|---|---|---|---|---|
| map (today, shared cache, order-dependent shapes) | 0.17 s | 73 | 0.28 s | 74 | 99 KiB |
| map, cache cleared per root | 3.96 s | 4 081 | | | |
| back-relation variants, unlimited | 12.5 s | 289 | 52.7 s, needs recursion limit > 1000 | 289 | 895 KiB |
| back-relation variants, `max_depth=1` | 0.10 s | 136 | 0.28 s | 136 | 151 KiB |
| back-relation variants, `max_depth=2` | 0.67 s | 352 | 2.4 s | 352 | 907 KiB |
| back-relation variants, `max_depth=3` | 1.75 s | 553 | 5.7 s | 553 | 1.6 MiB |
| one nested model per model, unlimited | 1.25 s | 136 | 1.7 s | 136 | 176 KiB |
| one nested model per model, `max_depth=2` | 0.54 s | 199 | 0.54 s | 199 | 228 KiB |
| one nested model per model, `max_depth=3` | 1.22 s | 262 | 1.3 s | 262 | 304 KiB |

Known prototype gaps: nested generated models are built lazily (`defer_build`), so
introspecting one before use (e.g. `model_computed_fields`) shows it incomplete;
each new root rebuilds the variants it reaches, which is what makes the unlimited
variant build slow.

## Behaviour changes

1. **Dump / `save_related` on dense graphs.** Valid paths that the old map dropped
   appear, invalid ones with a model twice on the path disappear. On graphs where
   the old map was correct the output is identical: the whole test suite passes in
   instance mode except 2 tests that assert the tree cache itself is built and the
   `get_pydantic()` shape tests below.
2. **`get_pydantic()`.** Nested schemas are recursive instead of a fixed-depth copy,
   nested relation fields are optional, and model names change (no random
   suffix). Tests that introspect the exact nested schema shape change (5 in
   `test_geting_pydantic_models.py`).
   The order-dependent cache bug is gone.
3. **`follow=True`.** Loads forward FKs up to `max_depth` (default 5) instead of
   every relation without limit. Reverse and many to many relations are no longer
   loaded by `follow=True` alone; pass `follow_reverse=N` or name them in
   `select_related`. In the ormar suite this changes 4 tests that load reverse
   collections with `follow=True` (e.g. `Department.courses.students` needs
   `follow_reverse=2`).
4. **Private API.** `_relation_tree()`, `_iterate_related_models()` and the class
   attributes above are removed. `model_dump(relation_map=...)` stays.

## Compatibility flag and migration

- Prototype switch: `ORMAR_RELATION_TRAVERSAL=map|instance`
  (env), or `ormar.models.traversal.set_relation_traversal(mode=...)`.
- Proposal for releases:
    1. next minor: ship `instance` as opt-in, `map` stays the default; document the
       differences; `max_depth=` and `follow_reverse=` available in both modes
       (either one switches `follow=True` to the new rule). `follow=True` without
       them in `map` mode warns that reverse and many to many relations will stop
       being followed;
    2. following minor: `instance` becomes the default, `map` still selectable and
       deprecated (warning on use);
    3. after that: remove `map` and the tree code.
- Migration note for users: if a response relied on the old dump shape of a dense
  graph, pass an explicit `include=`/`exclude=` or `relation_map=`; if
  `select_all(follow=True)` / `load_all(follow=True)` must load reverse or many to
  many relations, pass `follow_reverse=N` or list them in `select_related`; for
  deeper forward chains pass `max_depth=`; if
  OpenAPI must show exactly one level of nesting, use `get_pydantic(max_depth=1)`.

## Open questions

1. `follow=True` default: forward FKs depth 5 (recommended; 41 joins worst, 95 % of
   the saturated plan) or depth 3 (26 joins)? Include nullable FKs (recommended,
   ormar FKs are nullable by default) or only non-null like Django?
2. `get_pydantic()` default: back-relation variants (closest to today, slow OpenAPI
   on dense graphs) or one nested model per model (`omit_back_relations=False`,
   fastest, back-reference shown as optional)? Or back-relation variants with a
   default `max_depth`?
3. Node cache size per class: 256 caps the Shotris graph at ~17 MiB in the worst
   case and ~1.6 MiB for 3-level dumps; is that the right trade-off?
4. Should the switch be per model (`OrmarConfig`) instead of process-wide?
