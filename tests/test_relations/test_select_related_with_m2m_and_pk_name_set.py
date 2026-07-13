# type: ignore
from datetime import date
from typing import Optional, Union

import pytest
import sqlalchemy

import ormar
from ormar import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Role(ormar.Model):
    ormar_config = base_ormar_config.copy()

    name: str = ormar.String(primary_key=True, max_length=768)
    order: int = ormar.Integer(default=0, name="sort_order")
    description: str = ormar.Text()


class Company(ormar.Model):
    ormar_config = base_ormar_config.copy()

    name: str = ormar.String(primary_key=True, max_length=768)


class UserRoleCompany(ormar.Model):
    ormar_config = base_ormar_config.copy()


class User(ormar.Model):
    ormar_config = base_ormar_config.copy()

    registrationnumber: str = ormar.String(primary_key=True, max_length=768)
    company: Company = ormar.ForeignKey(Company)
    company2: Company = ormar.ForeignKey(Company, related_name="secondary_users")
    name: str = ormar.Text()
    role: Optional[Role] = ormar.ForeignKey(Role)
    roleforcompanies: Optional[Union[Company, list[Company]]] = ormar.ManyToMany(
        Company, through=UserRoleCompany, related_name="role_users"
    )
    lastupdate: date = ormar.DateTime(server_default=sqlalchemy.func.now())


create_test_database = init_tests(base_ormar_config)


def test_wrong_model():
    with pytest.raises(ModelDefinitionError):

        class User(ormar.Model):
            ormar_config = base_ormar_config.copy()

            registrationnumber: str = ormar.Text(primary_key=True)
            company: Company = ormar.ForeignKey(Company)
            company2: Company = ormar.ForeignKey(Company)


@pytest.mark.asyncio
async def test_create_primary_models():
    async with base_ormar_config.database:
        await Role.objects.create(
            name="user", order=0, description="no administration right"
        )
        role_1 = await Role.objects.create(
            name="admin", order=1, description="standard administration right"
        )
        await Role.objects.create(
            name="super_admin", order=2, description="super administration right"
        )
        assert await Role.objects.count() == 3

        company_0 = await Company.objects.create(name="Company")
        company_1 = await Company.objects.create(name="Subsidiary Company 1")
        company_2 = await Company.objects.create(name="Subsidiary Company 2")
        company_3 = await Company.objects.create(name="Subsidiary Company 3")
        assert await Company.objects.count() == 4

        user = await User.objects.create(
            registrationnumber="00-00000", company=company_0, name="admin", role=role_1
        )
        assert await User.objects.count() == 1

        await user.delete()
        assert await User.objects.count() == 0

        user = await User.objects.create(
            registrationnumber="00-00000",
            company=company_0,
            company2=company_3,
            name="admin",
            role=role_1,
        )
        await user.roleforcompanies.add(company_1)
        await user.roleforcompanies.add(company_2)

        users = await User.objects.select_related(
            ["company", "company2", "roleforcompanies"]
        ).all()
        assert len(users) == 1
        assert len(users[0].roleforcompanies) == 2
        assert len(users[0].roleforcompanies[0].role_users) == 1
        assert users[0].company.name == "Company"
        assert len(users[0].company.users) == 1
        assert users[0].company2.name == "Subsidiary Company 3"
        assert len(users[0].company2.secondary_users) == 1

        users = await User.objects.select_related("roleforcompanies").all()
        assert len(users) == 1
        assert len(users[0].roleforcompanies) == 2
