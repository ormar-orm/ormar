# Querying database with ormar

## QuerySet

Each Model is auto registered with a `QuerySet` that represents the underlying query,
and it's options.

Most of the methods are also available through many to many relations and on reverse
foreign key relations through `QuerysetProxy` interface.

!!!info
    To see which relations are supported and how to construct relations
    visit [relations][relations].

For simplicity available methods to fetch and save the data into the database are
divided into categories according to the function they fulfill.

Note that some functions/methods are in multiple categories.

For completeness, Model and relation methods are listed.

To read more about any specific section or function please refer to the details subpage.

###[Insert data into database](./create.md)

* `create(**kwargs) -> Model`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`
* `update_or_create(**kwargs) -> Model`
* `bulk_create(objects: list[Model]) -> None`


* `Model`
    * `Model.save()` method
    * `Model.upsert()` method
    * `Model.save_related()` method


* `QuerysetProxy`
    * `QuerysetProxy.create(**kwargs)` method
    * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs)` method
    * `QuerysetProxy.update_or_create(**kwargs)` method

!!!tip
    To read more about any or all of those functions visit [create](./create.md) section.

### [Read data from database](./read.md)

* `get(**kwargs) -> Model`
* `get_or_none(**kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`
* `first() -> Model`
* `all(**kwargs) -> list[Optional[Model]]`
* `select_for_update(nowait = False, skip_locked = False, read = False, no_key = False) -> QuerySet`


* `Model`
    * `Model.load()` method


* `QuerysetProxy`
    * `QuerysetProxy.get(**kwargs)` method
    * `QuerysetProxy.get_or_none(**kwargs)` method
    * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs)` method
    * `QuerysetProxy.first()` method
    * `QuerysetProxy.all(**kwargs)` method

!!!tip
    To read more about any or all of those functions visit [read](./read.md) section.

### [Read raw data from database](./raw-data.md)

Instead of ormar models return raw data in form list of dictionaries or tuples.

* `values(fields = None, exclude_through = False) -> list[dict]`
* `values_list(fields = None, exclude_through = False, flatten = False) -> list`


* `QuerysetProxy`
    * `QuerysetProxy.values(fields = None, exclude_through = False)` method
    * `QuerysetProxy.values_list(fields = None, exclude_through= False, flatten = False)` method

!!!tip
    To read more about any or all of those functions visit [raw data](./raw-data.md) section.

### [Update data in database](./update.md)

* `update(each: bool = False, **kwargs) -> int`
* `update_or_create(**kwargs) -> Model`
* `bulk_update(objects: list[Model], columns: list[str] = None) -> None`


* `Model`
    * `Model.update()` method
    * `Model.upsert()` method
    * `Model.save_related()` method


* `QuerysetProxy`
    * `QuerysetProxy.update_or_create(**kwargs)` method

!!!tip
    To read more about any or all of those functions visit [update](./update.md) section.

### [Delete data from database](./delete.md)

* `delete(each: bool = False, **kwargs) -> int`


* `Model`
    * `Model.delete()` method


* `QuerysetProxy`
    * `QuerysetProxy.remove()` method
    * `QuerysetProxy.clear()` method

!!!tip
    To read more about any or all of those functions visit [delete](./delete.md) section.

### [Joins and subqueries](./joins-and-subqueries.md)

* `select_related(related: Union[list, str]) -> QuerySet`
* `prefetch_related(related: Union[list, str]) -> QuerySet`


* `Model`
    * `Model.load()` method


* `QuerysetProxy`
    * `QuerysetProxy.select_related(related: Union[list, str])` method
    * `QuerysetProxy.prefetch_related(related: Union[list, str])` method

!!!tip
    To read more about any or all of those functions visit [joins and subqueries](./joins-and-subqueries.md) section.

### [Filtering and sorting](./filter-and-sort.md)

* `filter(**kwargs) -> QuerySet`
* `exclude(**kwargs) -> QuerySet`
* `order_by(columns:Union[list, str]) -> QuerySet`
* `get(**kwargs) -> Model`
* `get_or_none(**kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`
* `all(**kwargs) -> list[Optional[Model]]`


* `QuerysetProxy`
    * `QuerysetProxy.filter(**kwargs)` method
    * `QuerysetProxy.exclude(**kwargs)` method
    * `QuerysetProxy.order_by(columns:Union[list, str])` method
    * `QuerysetProxy.get(**kwargs)` method
    * `QuerysetProxy.get_or_none(**kwargs)` method
    * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs)` method
    * `QuerysetProxy.all(**kwargs)` method

!!!tip
    To read more about any or all of those functions visit [filtering and sorting](./filter-and-sort.md) section.

### [Selecting columns](./select-columns.md)

* `fields(columns: Union[list, str, set, dict]) -> QuerySet`
* `exclude_fields(columns: Union[list, str, set, dict]) -> QuerySet`


* `QuerysetProxy`
    * `QuerysetProxy.fields(columns: Union[list, str, set, dict])` method
    * `QuerysetProxy.exclude_fields(columns: Union[list, str, set, dict])` method

!!!tip
    To read more about any or all of those functions visit [selecting columns](./select-columns.md) section.

### [Pagination and rows number](./pagination-and-rows-number.md)

* `paginate(page: int) -> QuerySet`
* `limit(limit_count: int) -> QuerySet`
* `offset(offset: int) -> QuerySet`
* `get() -> Model`
* `first() -> Model`


* `QuerysetProxy`
    * `QuerysetProxy.paginate(page: int)` method
    * `QuerysetProxy.limit(limit_count: int)` method
    * `QuerysetProxy.offset(offset: int)` method

!!!tip
    To read more about any or all of those functions visit [pagination](./pagination-and-rows-number.md) section.

### [Aggregated functions](./aggregations.md)

* `count(distinct: bool = True) -> int`
* `exists() -> bool`


* `QuerysetProxy`
    * `QuerysetProxy.count(distinct=True)` method
    * `QuerysetProxy.exists()` method

!!!tip
    To read more about any or all of those functions visit [aggregations](./aggregations.md) section.


[relations]: ../relations/index.md
