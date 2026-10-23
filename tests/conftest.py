"""Test-suite wide settings."""

from tests import cross_check

# Every template and trusted construction is also built the old way and compared.
cross_check.install()
