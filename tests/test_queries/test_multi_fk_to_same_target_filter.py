import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Switch(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="switch")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Port(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="port")

    id: int = ormar.Integer(primary_key=True)
    switch: Switch = ormar.ForeignKey(Switch)


class Link(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="link")

    id: int = ormar.Integer(primary_key=True)
    a_port: Port = ormar.ForeignKey(Port, related_name="a_links")
    z_port: Port = ormar.ForeignKey(Port, related_name="z_links")


create_test_database = init_tests(base_ormar_config)


def from_clause(expression) -> str:
    """Extracts the FROM ... (up to WHERE) part of the compiled SQL."""
    sql = str(expression.compile(compile_kwargs={"literal_binds": True}))
    return sql.split("FROM", 1)[1].split("WHERE")[0]


def has_cartesian_product(queryset) -> bool:
    """A comma in the FROM clause means an unjoined (cross-product) table."""
    return "," in from_clause(queryset.build_select_expression())


@pytest.mark.asyncio
async def test_filter_on_two_fks_to_same_target_has_no_cartesian_product():
    async with base_ormar_config.database:
        queryset = Link.objects.filter(
            a_port__switch__name__in=["a0", "a1"],
            z_port__switch__name__in=["z0", "z1"],
        )
        assert not has_cartesian_product(queryset)


@pytest.mark.asyncio
async def test_repeated_query_not_corrupted_by_alias_manager_state():
    async with base_ormar_config.database:
        # Build the two-sided filter first; in the buggy version this pollutes
        # the global alias manager and corrupts the next, simpler query.
        Link.objects.filter(
            a_port__switch__name__in=["a0", "a1"],
            z_port__switch__name__in=["z0", "z1"],
        ).build_select_expression()

        queryset = Link.objects.filter(
            a_port__switch__in=[1, 2],
            z_port__switch__name__in=["z0", "z1"],
        )
        assert not has_cartesian_product(queryset)


@pytest.mark.asyncio
async def test_each_filter_targets_its_own_branch():
    async with base_ormar_config.database:
        switch_a = await Switch(name="switch_a").save()
        switch_z = await Switch(name="switch_z").save()
        port_a = await Port(switch=switch_a).save()
        port_z = await Port(switch=switch_z).save()
        other = await Port(switch=switch_z).save()

        wanted = await Link(a_port=port_a, z_port=port_z).save()
        await Link(a_port=other, z_port=port_z).save()

        result = await Link.objects.filter(
            a_port__switch__name="switch_a",
            z_port__switch__name="switch_z",
        ).all()

        assert len(result) == 1
        assert result[0].id == wanted.id
