"""
Gathers all exceptions thrown by ormar.
"""


class AsyncOrmException(Exception):
    """
    Base ormar Exception
    """

    pass


class ModelDefinitionError(AsyncOrmException):
    """
    Raised for errors related to the model definition itself:

    * defining a Field without required parameters
    * defining a model with more than one primary_key
    * defining a model without primary_key
    """

    pass


class ReadOnlyModelError(ModelDefinitionError):
    """
    Raised when writing to a read-only model, e.g. one mapped to a database
    view (``OrmarConfig(view=True)``).
    """

    pass


class ModelError(AsyncOrmException):
    """
    Raised for initialization of model with non-existing field keyword.
    """

    pass


class NoMatch(AsyncOrmException):
    """
    Raised for database queries that has no matching result (empty result).
    """

    pass


class MultipleMatches(AsyncOrmException):
    """
    Raised for database queries that should return one row (i.e. get, first etc.)
    but has multiple matching results in response.
    """

    pass


class QueryDefinitionError(AsyncOrmException):
    """
    Raised for errors in query definition:

    * using contains or icontains filter with instance of the Model
    * using Queryset.update() without filter and setting each flag to True
    * using Queryset.delete() without filter and setting each flag to True
    """

    pass


class RelationshipInstanceError(AsyncOrmException):
    pass


class ModelPersistenceError(AsyncOrmException):
    """
    Raised for update of models without primary_key set (cannot retrieve from db)
    or for saving a model with relation to unsaved model (cannot extract fk value).
    """

    pass


class SignalDefinitionError(AsyncOrmException):
    """
    Raised when non callable receiver is passed as signal callback.
    """

    pass


class ModelListEmptyError(AsyncOrmException):
    """
    Raised for objects is empty when bulk_update
    """

    pass
