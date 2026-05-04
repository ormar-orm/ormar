"""
Verifies how OrmarConfig.copy() handles the schema parameter.

Runs on every dialect — this only exercises Python attribute behaviour.
"""

from tests.settings import create_config


def test_copy_inherits_schema_by_default():
    base = create_config()
    child = base.copy(tablename="t1", schema="hr")
    grandchild = child.copy(tablename="t2")
    assert grandchild.schema == "hr"


def test_copy_with_explicit_none_clears_schema():
    base = create_config()
    child = base.copy(tablename="t1", schema="hr")
    grandchild = child.copy(tablename="t2", schema=None)
    assert grandchild.schema is None


def test_copy_with_explicit_value_overrides_schema():
    base = create_config()
    child = base.copy(tablename="t1", schema="hr")
    grandchild = child.copy(tablename="t2", schema="it")
    assert grandchild.schema == "it"


def test_default_config_has_no_schema():
    base = create_config()
    assert base.schema is None
    child = base.copy(tablename="t1")
    assert child.schema is None
