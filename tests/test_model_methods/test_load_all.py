from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Language(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="languages")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    level: str = ormar.String(max_length=150, default="Beginner")


class CringeLevel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="levels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    language = ormar.ForeignKey(Language)


class NickName(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nicks")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="hq_name")
    is_lame: Optional[bool] = ormar.Boolean(nullable=True)
    level: CringeLevel = ormar.ForeignKey(CringeLevel)


class HQ(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="hqs")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="hq_name")
    nicks: list[NickName] = ormar.ManyToMany(NickName)


class Company(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="companies")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="company_name")
    founded: Optional[int] = ormar.Integer(nullable=True)
    hq: HQ = ormar.ForeignKey(HQ, related_name="companies")


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_load_all_fk_rel():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            hq = await HQ.objects.create(name="Main")
            company = await Company.objects.create(name="Banzai", founded=1988, hq=hq)

            hq = await HQ.objects.get(name="Main")
            await hq.load_all()

            assert hq.companies[0] == company
            assert hq.companies[0].name == "Banzai"
            assert hq.companies[0].founded == 1988

            hq2 = await HQ.objects.select_all().get(name="Main")
            assert hq2.companies[0] == company
            assert hq2.companies[0].name == "Banzai"
            assert hq2.companies[0].founded == 1988


@pytest.mark.asyncio
async def test_load_all_many_to_many():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            nick1 = await NickName.objects.create(name="BazingaO", is_lame=False)
            nick2 = await NickName.objects.create(name="Bazinga20", is_lame=True)
            hq = await HQ.objects.create(name="Main")
            await hq.nicks.add(nick1)
            await hq.nicks.add(nick2)

            hq = await HQ.objects.get(name="Main")
            await hq.load_all()

            assert hq.nicks[0] == nick1
            assert hq.nicks[0].name == "BazingaO"

            assert hq.nicks[1] == nick2
            assert hq.nicks[1].name == "Bazinga20"

            hq2 = await HQ.objects.select_all().get(name="Main")
            assert hq2.nicks[0] == nick1
            assert hq2.nicks[0].name == "BazingaO"
            assert hq2.nicks[1] == nick2
            assert hq2.nicks[1].name == "Bazinga20"


@pytest.mark.asyncio
async def test_load_all_with_order():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            nick1 = await NickName.objects.create(name="Barry", is_lame=False)
            nick2 = await NickName.objects.create(name="Joe", is_lame=True)
            hq = await HQ.objects.create(name="Main")
            await hq.nicks.add(nick1)
            await hq.nicks.add(nick2)

            hq = await HQ.objects.get(name="Main")
            await hq.load_all(order_by="-nicks__name")

            assert hq.nicks[0] == nick2
            assert hq.nicks[0].name == "Joe"

            assert hq.nicks[1] == nick1
            assert hq.nicks[1].name == "Barry"

            await hq.load_all()
            assert hq.nicks[0] == nick1
            assert hq.nicks[1] == nick2

            hq2 = (
                await HQ.objects.select_all().order_by("-nicks__name").get(name="Main")
            )
            assert hq2.nicks[0] == nick2
            assert hq2.nicks[1] == nick1

            hq3 = await HQ.objects.select_all().get(name="Main")
            assert hq3.nicks[0] == nick1
            assert hq3.nicks[1] == nick2


@pytest.mark.asyncio
async def test_loading_reversed_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            hq = await HQ.objects.create(name="Main")
            await Company.objects.create(name="Banzai", founded=1988, hq=hq)

            company = await Company.objects.get(name="Banzai")
            await company.load_all()

            assert company.hq == hq

            company2 = await Company.objects.select_all().get(name="Banzai")
            assert company2.hq == hq


@pytest.mark.asyncio
async def test_loading_nested():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            language = await Language.objects.create(name="English")
            level = await CringeLevel.objects.create(name="High", language=language)
            level2 = await CringeLevel.objects.create(name="Low", language=language)
            nick1 = await NickName.objects.create(
                name="BazingaO", is_lame=False, level=level
            )
            nick2 = await NickName.objects.create(
                name="Bazinga20", is_lame=True, level=level2
            )
            hq = await HQ.objects.create(name="Main")
            await hq.nicks.add(nick1)
            await hq.nicks.add(nick2)

            hq = await HQ.objects.get(name="Main")
            await hq.load_all(follow=True, follow_reverse=1)

            assert hq.nicks[0] == nick1
            assert hq.nicks[0].name == "BazingaO"
            assert hq.nicks[0].level.name == "High"
            assert hq.nicks[0].level.language.name == "English"

            assert hq.nicks[1] == nick2
            assert hq.nicks[1].name == "Bazinga20"
            assert hq.nicks[1].level.name == "Low"
            assert hq.nicks[1].level.language.name == "English"

            hq2 = await HQ.objects.select_all(follow=True, follow_reverse=1).get(
                name="Main"
            )
            assert hq2.nicks[0] == nick1
            assert hq2.nicks[0].name == "BazingaO"
            assert hq2.nicks[0].level.name == "High"
            assert hq2.nicks[0].level.language.name == "English"

            assert hq2.nicks[1] == nick2
            assert hq2.nicks[1].name == "Bazinga20"
            assert hq2.nicks[1].level.name == "Low"
            assert hq2.nicks[1].level.language.name == "English"

            hq5 = await HQ.objects.select_all().get(name="Main")
            assert len(hq5.nicks) == 2
            await hq5.nicks.select_all(follow=True).all()
            assert hq5.nicks[0] == nick1
            assert hq5.nicks[0].name == "BazingaO"
            assert hq5.nicks[0].level.name == "High"
            assert hq5.nicks[0].level.language.name == "English"
            assert hq5.nicks[1] == nick2
            assert hq5.nicks[1].name == "Bazinga20"
            assert hq5.nicks[1].level.name == "Low"
            assert hq5.nicks[1].level.language.name == "English"

            await hq.load_all(
                follow=True, follow_reverse=1, exclude="nicks__level__language"
            )
            assert len(hq.nicks) == 2
            assert hq.nicks[0].level.language is None
            assert hq.nicks[1].level.language is None

            hq3 = (
                await HQ.objects.select_all(follow=True, follow_reverse=1)
                .exclude_fields("nicks__level__language")
                .get(name="Main")
            )
            assert len(hq3.nicks) == 2
            assert hq3.nicks[0].level.language is None
            assert hq3.nicks[1].level.language is None

            await hq.load_all(
                follow=True, follow_reverse=1, exclude="nicks__level__language__level"
            )
            assert len(hq.nicks) == 2
            assert hq.nicks[0].level.language is not None
            assert hq.nicks[0].level.language.level is None
            assert hq.nicks[1].level.language is not None
            assert hq.nicks[1].level.language.level is None

            await hq.load_all(follow=True, follow_reverse=1, exclude="nicks__level")
            assert len(hq.nicks) == 2
            assert hq.nicks[0].level is None
            assert hq.nicks[1].level is None

            await hq.load_all(follow=True, follow_reverse=1, exclude="nicks")
            assert len(hq.nicks) == 0
