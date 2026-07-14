import ormar


def test_aggregate_value_objects():
    c = ormar.Count("tasks")
    assert c.function_name == "count"
    assert c.field == "tasks"
    assert c.distinct is False

    cd = ormar.Count("tasks", distinct=True)
    assert cd.distinct is True

    assert ormar.Sum("tasks__price").function_name == "sum"
    assert ormar.Avg("tasks__price").function_name == "avg"
    assert ormar.Min("tasks__price").function_name == "min"
    assert ormar.Max("tasks__price").function_name == "max"
    assert ormar.Sum("tasks__price").field == "tasks__price"
