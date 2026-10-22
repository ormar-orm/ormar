"""Test-suite wide settings."""

from ormar.queryset.templates import cross_check

# Every query template hit is also built without templates and compared.
cross_check.ENABLED = True
