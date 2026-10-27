"""
Per-model cache of statement templates.

Every model class gets its own least-recently-used cache, keyed by cache key
(see ``cache_keys``), holding statements built with named slots (see
``slots``). Reads and writes share it::

    cache = template_cache_for(model_cls=Author)
    cache.get(key)  # -> cached statement or None
"""

import weakref
from collections import OrderedDict
from typing import (
    Any,
    Callable,
    Dict,
    NamedTuple,
    Tuple,
    Type,
)

from sqlalchemy.sql import Executable

# cache keys kept per model before the least recently used is dropped
TEMPLATE_CACHE_SIZE = 512


class StatementTemplate(NamedTuple):
    """
    A statement from the template cache and the values to run it with for one
    call::

        template = queryset.filter(name="a").limit(5).build_select_template()
        template.params  # -> {"ormar_p0": "a", "ormar_limit": 5}
        await executor.fetch_all(template.statement, template.params)
    """

    statement: Executable
    params: Dict[str, Any]


class TemplateCache:
    """
    Least-recently-used cache of one model's statements, keyed by cache key.

    The size is bounded so an application that builds queries from many
    different filter combinations cannot grow memory without limit: once
    ``max_size`` keys are cached, adding one drops the key used longest ago.
    """

    def __init__(self, max_size: int = TEMPLATE_CACHE_SIZE) -> None:
        """
        :param max_size: number of cache keys kept
        :type max_size: int
        """
        self.max_size = max_size
        self.entries: "OrderedDict[Any, Any]" = OrderedDict()

    def get(self, key: Any) -> Any:
        """
        Return the statement cached for a key, or None, and mark it as the
        most recently used.

        Another thread using the same model may evict the entry between reading
        it and marking it, so that case is ignored: the statement is still
        valid for this call.

        :param key: cache key of the statement
        :type key: Any
        :return: cached statement or None
        :rtype: Any
        """
        statement = self.entries.get(key)
        if statement is not None:
            try:
                self.entries.move_to_end(key)
            except KeyError:  # pragma: no cover - evicted concurrently
                pass
        return statement

    def put(self, key: Any, statement: Any) -> None:
        """
        Cache the statement of a key. When the cache is full, the key used
        longest ago is dropped.

        :param key: cache key of the statement
        :type key: Any
        :param statement: statement to cache
        :type statement: Any
        """
        self.entries[key] = statement
        if len(self.entries) > self.max_size:
            self.entries.popitem(last=False)

    def clear(self) -> None:
        """Remove every cached statement."""
        self.entries.clear()


# one cache per model class; weak keys let model classes created at runtime
# (e.g. inside tests) be garbage collected together with their statements
TEMPLATE_CACHES: "weakref.WeakKeyDictionary[type, TemplateCache]" = (
    weakref.WeakKeyDictionary()
)


def template_cache_for(model_cls: Type[Any]) -> TemplateCache:
    """
    Return the statement template cache of a model class, creating it on the
    model's first read. ``setdefault`` keeps a single cache when two threads
    create it at the same time.

    :param model_cls: model class
    :type model_cls: Type[Any]
    :return: the model's template cache
    :rtype: TemplateCache
    """
    cache = TEMPLATE_CACHES.get(model_cls)
    if cache is None:
        cache = TEMPLATE_CACHES.setdefault(model_cls, TemplateCache())
    return cache


def cached_statement(
    model_cls: Type[Any],
    key: Tuple[Any, ...],
    builder: Callable[..., Executable],
    **builder_kwargs: Any,
) -> Executable:
    """
    Return the statement cached for a write cache key, building it on a miss.

    A write's cache key is small (the kind of statement and the written column
    names), so callers pass the key directly. The statement is only built on
    a miss, from ``builder(**builder_kwargs)``::

        cached_statement(
            model_cls=Author,
            key=("insert", ("name", "score")),
            builder=build_insert,
            table=Author.ormar_config.table,
            names=("name", "score"),
        )

    :param model_cls: model the statement writes
    :type model_cls: Type[Any]
    :param key: cache key of the write
    :type key: Tuple[Any, ...]
    :param builder: builds the statement from ``builder_kwargs``
    :type builder: Callable[..., Executable]
    :param builder_kwargs: keyword arguments of the builder
    :type builder_kwargs: Any
    :return: statement for the key
    :rtype: Executable
    """
    cache = template_cache_for(model_cls=model_cls)
    statement = cache.get(key)
    if statement is None:
        statement = builder(**builder_kwargs)
        cache.put(key=key, statement=statement)
    return statement
