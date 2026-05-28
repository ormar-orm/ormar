"""Tests for ``OrmarConfig.comment`` forwarding to ``sqlalchemy.Table``."""

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class CommentedModel(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="commented_models",
        comment="Stores commented things.",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class UncommentedModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="uncommented_models")

    id: int = ormar.Integer(primary_key=True)


create_test_database = init_tests(base_ormar_config)


def test_comment_forwarded_to_sqlalchemy_table():
    assert CommentedModel.ormar_config.table.comment == "Stores commented things."


def test_comment_defaults_to_none():
    assert UncommentedModel.ormar_config.comment is None
    assert UncommentedModel.ormar_config.table.comment is None


def test_copy_inherits_parent_comment_when_omitted():
    parent = ormar.OrmarConfig(
        metadata=base_ormar_config.metadata,
        database=base_ormar_config.database,
        engine=base_ormar_config.engine,
        comment="parent comment",
    )
    child = parent.copy(tablename="child")
    assert child.comment == "parent comment"


def test_copy_can_explicitly_clear_parent_comment():
    parent = ormar.OrmarConfig(
        metadata=base_ormar_config.metadata,
        database=base_ormar_config.database,
        engine=base_ormar_config.engine,
        comment="parent comment",
    )
    child = parent.copy(tablename="child", comment=None)
    assert child.comment is None


def test_copy_can_override_parent_comment():
    parent = ormar.OrmarConfig(
        metadata=base_ormar_config.metadata,
        database=base_ormar_config.database,
        engine=base_ormar_config.engine,
        comment="parent comment",
    )
    child = parent.copy(tablename="child", comment="child comment")
    assert child.comment == "child comment"
