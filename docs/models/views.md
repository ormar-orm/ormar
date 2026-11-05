# Database views

A model can be mapped to an existing database view with `view=True`. Such a model is
read-only: it can be queried, joined and prefetched like any other model, but every
write raises `ReadOnlyModelError`.

```python
class CustomerTotal(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="customer_totals", view=True)

    customer: Customer = ormar.ForeignKey(
        Customer, primary_key=True, name="customer_id", related_name="totals"
    )
    total: float = ormar.Float()


await CustomerTotal.objects.filter(total__gt=100).select_related("customer").all()
await Customer.objects.prefetch_related("totals").all()
await customer.totals.all()

await CustomerTotal(customer=customer, total=1).save()  # ReadOnlyModelError
```

* A view model still needs a primary key (it can be composite or made of foreign
  keys). ormar uses it to tell rows apart, so pick columns that are unique in the
  view.
* ormar doesn't create, alter or drop views. The view's table is kept out of
  `ormar_config.metadata`, so `metadata.create_all()` and Alembic autogenerate ignore
  it. Create the view yourself, e.g. in an Alembic migration with `op.execute()`.
* Foreign keys declared on a view model never create database constraints. A foreign
  key from a table **to** a view must pass `db_constraint=False`, as databases can't
  reference views.
* Foreign keys declared on a view model never create constraints, but a foreign key
  from one view to another still needs `db_constraint=False` to be accepted.
* A `ManyToMany` to or from a view works: the generated through model gets no
  database constraint to the view.
* Proxy models of a view, and models inheriting from an abstract model declared with
  `view=True`, are views too.
* Writes refused with `ReadOnlyModelError` (a subclass of `ModelDefinitionError`)
  before anything changes in memory: `save()`, `update()`, `upsert()`, `delete()`,
  `save_related()` called on a view, `QuerySet.create()` / `get_or_create()` /
  `update_or_create()` (when they would write), `bulk_create()`, `bulk_update()`,
  `QuerySet.update()`, `QuerySet.delete()`, and relation querysets' `create()`,
  `add()` / `remove()` / `clear()` when they would write a view's rows.
  `save_related()` of a table model skips views loaded in its relations.
