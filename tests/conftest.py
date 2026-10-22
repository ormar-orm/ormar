"""Test-suite wide settings."""

from ormar.queryset import templates

# Every query template hit is also built without templates and compared.
templates.CROSS_CHECK = True
