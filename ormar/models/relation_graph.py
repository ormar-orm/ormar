"""
Version of the relation graph shared by all ormar models, used to invalidate the
per-class caches of relation paths when a relation is registered later.
"""


class RelationGraph:
    """
    Counts changes of the relation graph of all models.

    The relation paths of a model reach every model connected to it, so a
    relation registered on any model (a reverse relation of a model defined
    later, or a resolved forward reference) can change the paths of many other
    models. Their caches remember the version they were built at and are
    rebuilt when it changed::

        RelationGraph.version  # -> 7
        RelationGraph.changed()
        RelationGraph.version  # -> 8
    """

    version: int = 0

    @classmethod
    def changed(cls) -> None:
        """
        Mark the relation graph as changed, so cached relation paths of all
        models are rebuilt on next use.
        """
        cls.version += 1
