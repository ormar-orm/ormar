"""Test-suite wide settings."""

from collections.abc import Iterator

import pytest

from ormar.models.traversal import RelationTraversalSettings
from tests import cross_check

# Every template and trusted construction is also built the old way and compared.
cross_check.install()


@pytest.fixture
def map_traversal() -> Iterator[None]:
    """Runs a test with the deprecated ``map`` relation traversal."""
    previous = RelationTraversalSettings.mode
    RelationTraversalSettings.mode = "map"
    yield
    RelationTraversalSettings.mode = previous


@pytest.fixture
def instance_traversal() -> Iterator[None]:
    """Runs a test with the ``instance`` relation traversal."""
    previous = RelationTraversalSettings.mode
    RelationTraversalSettings.mode = "instance"
    yield
    RelationTraversalSettings.mode = previous
