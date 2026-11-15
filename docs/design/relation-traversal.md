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
(`selectinload`, `joinedload`, wildcard `"*"`). Eager loading configured on the
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
limits depth (unlimited by default)
([shortcuts.py](https://github.com/coleifer/peewee/blob/3.17.8/playhouse/shortcuts.py)).

**Django + DRF.** Django has no nested serialization. `select_related()` without
arguments follows only non-null forward FKs and stops at depth 5
(`Query.max_depth`, "arbitrary limit … to prevent infinite recursion";
[docs](https://docs.djangoproject.com/en/5.1/ref/models/querysets/#select-related)).
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
and/or a depth limit; loaders follow explicit paths or a depth limit. I could not
verify from the docs how deep SQLAlchemy's `joinedload("*")` recurses.

## Proposed design

### `model_dump()` and `save_related()`: walk the loaded instances

The decision "follow this relation?" only needs the model classes on the current
path, and the walk only descends where an instance is actually loaded. So instead
of a cached tree the default `relation_map` becomes a `PathRelationMap(model,
ancestors)` node: a read-only `Mapping` that answers `relation in node`,
`node[relation]` and `bool(node)` from the classes on the path. A child node is
created only when the dump descends into a loaded related instance.

- No tree is built; memory is O(depth) per row, transient, never accumulated.
- The result is exactly the correct semantics (no shared-ancestry bug, no key
  collision).
- The existing dump code, include/exclude, `flatten_fields`, `exclude_list`,
  through models and the explicit `relation_map=` argument are unchanged, because
  the node behaves like the old dict (a leaf is `...`).
- The only per-class data is the list of relation targets (one dict entry per
  relation), invalidated with `RelationGraph.version`.

`save_related(follow=True)` uses the same node, so it saves exactly what a dump
would show.

Optional variant `instance_cached`: each node keeps the children it created and
the class keeps its root node, so the part of the correct tree that was actually
walked is built once. Faster (see benchmarks), memory grows with the number of
distinct paths that were dumped, never with the number of rows.

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

### `follow=True`: a depth limit

`select_all(follow=True)` and `load_all(follow=True)` get `max_depth=`. In instance
mode the default is `DEFAULT_FOLLOW_DEPTH = 3`, and the paths follow the same rule
as the dump. On the Shotris graph:

| depth | paths, worst model | paths, all 73 models |
|---|---|---|
| 1 | 40 | 216 |
| 2 | 87 | 2 473 |
| 3 | 565 | 9 206 |
| 4 | 1 617 | 44 542 |
| today (unlimited, buggy map) | 249 129 | 4 415 758 |

Even depth 3 is a lot of joins for dense models, so 2 may be the better default.

### Result

`__relation_map__`, `__relation_tree__`, `build_relation_tree`,
`_iterate_related_models`, `NodeList` and the relation-graph cache invalidation for
them go away, and with them the shared-ancestry bug and the key collision.

## Benchmarks (prototype)

Python 3.11, pydantic 2.14, shared and busy machine, `nice`; numbers are the best of
3 runs. `map` = today, `instance` = path nodes per dump, `instance_cached` = walked
nodes kept on the class.

`model_dump()` per row, instances built in memory:

| case | depth loaded | map µs | instance µs | instance_cached µs | dicts map / new |
|---|---|---|---|---|---|
| chain Chapter→Book→Writer→Country | 1 | 15.0 | 18.1 | 15.6 | 2 / 2 |
| | 2 | 22.6 | 27.3 | 24.1 | 3 / 3 |
| | 3 | 28.7 | 34.4 | 30.0 | 4 / 4 |
| dense 6-model graph (FK + M2M to every other) | 1 | 151 | 206 | 164 | 6 / 6 |
| | 2 | 348 | 547 | 413 | 16 / 16 |
| | 3 | 359 | 821 | 620 | 16 / 26 |

At depth 3 on the dense graph the old map drops valid paths, so the new dump does
more work (26 nested dicts instead of 16).

1,000,000 rows dumped one by one (pool of 10,000 pre-built rows: Book + author +
country + 3 chapters + 2 tags, 8 dicts per row), tracemalloc on:

| | map | instance | instance_cached |
|---|---|---|---|
| transient peak per row | 3.6 KB | 4.6 KB | 3.7 KB |
| retained growth from row 200k to 1M | 736 B | 736 B | 736 B |
| wall time with tracemalloc | 280 s | 320 s | 296 s |

The 736 B is the list of samples; nothing grows with the row count in any mode.

Per-class memory on the Shotris graph (73 models):

| | map | instance | instance_cached |
|---|---|---|---|
| default dump map | `_relation_tree()` 1.84 MB, 233 ms | relation index 55 KB (one entry per relation) | as instance + walked nodes (217 KB after one depth-3 dump of the dense graph) |
| `follow=True` paths | 807 MB, 11.6 s | nothing kept | nothing kept |

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
3. **`follow=True`.** Loads at most `max_depth` relations deep (default 3) instead
   of the full (exponential) path list.
4. **Private API.** `_relation_tree()`, `_iterate_related_models()` and the class
   attributes above are removed. `model_dump(relation_map=...)` stays.

## Compatibility flag and migration

- Prototype switch: `ORMAR_RELATION_TRAVERSAL=map|instance|instance_cached`
  (env), or `ormar.models.traversal.set_relation_traversal(mode=...)`.
- Proposal for releases:
    1. next minor: ship `instance` as opt-in, `map` stays the default; document the
       differences; `max_depth=` available in both modes;
    2. following minor: `instance` becomes the default, `map` still selectable and
       deprecated (warning on use);
    3. after that: remove `map` and the tree code.
- Migration note for users: if a response relied on the old dump shape of a dense
  graph, pass an explicit `include=`/`exclude=` or `relation_map=`; if
  `load_all(follow=True)` must go deeper than the default, pass `max_depth=`; if
  OpenAPI must show exactly one level of nesting, use `get_pydantic(max_depth=1)`.

## Open questions

1. Default `max_depth` for `follow=True`: 2 or 3?
2. `get_pydantic()` default: back-relation variants (closest to today, slow OpenAPI
   on dense graphs) or one nested model per model (`omit_back_relations=False`,
   fastest, back-reference shown as optional)? Or back-relation variants with a
   default `max_depth`?
3. `instance` vs `instance_cached`: is ~5–20 % dump speed worth memory that grows
   with the distinct paths dumped?
4. Should the switch be per model (`OrmarConfig`) instead of process-wide?
