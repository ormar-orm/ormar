from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config(force_rollback=True)


class Department(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="departments")

    id: int = ormar.Integer(primary_key=True, autoincrement=False)
    name: str = ormar.String(max_length=100)


class SchoolClass(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="schoolclasses")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    department: Optional[Department] = ormar.ForeignKey(Department, nullable=False)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="students")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    schoolclass: Optional[SchoolClass] = ormar.ForeignKey(SchoolClass)
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)


class Teacher(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="teachers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    schoolclass: Optional[SchoolClass] = ormar.ForeignKey(SchoolClass)
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)


create_test_database = init_tests(base_ormar_config)


async def create_data():
    department = await Department.objects.create(id=1, name="Math Department")
    department2 = await Department.objects.create(id=2, name="Law Department")
    class1 = await SchoolClass.objects.create(name="Math")
    class2 = await SchoolClass.objects.create(name="Logic")
    category = await Category.objects.create(name="Foreign", department=department)
    category2 = await Category.objects.create(name="Domestic", department=department2)
    await Student.objects.create(name="Jane", category=category, schoolclass=class1)
    await Student.objects.create(name="Judy", category=category2, schoolclass=class1)
    await Student.objects.create(name="Jack", category=category2, schoolclass=class2)
    await Teacher.objects.create(name="Joe", category=category2, schoolclass=class1)


@pytest.mark.asyncio
async def test_model_multiple_instances_of_same_table_in_schema():
    async with base_ormar_config.database:
        await create_data()
        classes = await SchoolClass.objects.select_related(
            ["teachers__category__department", "students__category__department"]
        ).all()
        assert classes[0].name == "Math"
        assert classes[0].students[0].name == "Jane"
        assert len(classes[0].model_dump().get("students")) == 2
        assert classes[0].teachers[0].category.department.name == "Law Department"
        assert classes[0].students[0].category.department.name == "Math Department"


@pytest.mark.asyncio
async def test_load_all_multiple_instances_of_same_table_in_schema():
    async with base_ormar_config.database:
        await create_data()
        math_class = await SchoolClass.objects.get(name="Math")
        assert math_class.name == "Math"

        await math_class.load_all(follow=True, follow_reverse=1)
        assert math_class.students[0].name == "Jane"
        assert len(math_class.model_dump().get("students")) == 2
        assert math_class.teachers[0].category.department.name == "Law Department"
        assert math_class.students[0].category.department.name == "Math Department"


@pytest.mark.asyncio
async def test_filter_groups_with_instances_of_same_table_in_schema():
    async with base_ormar_config.database:
        await create_data()
        math_class = (
            await SchoolClass.objects.select_related(
                ["teachers__category__department", "students__category__department"]
            )
            .filter(
                ormar.or_(
                    students__name="Jane",
                    teachers__category__name="Domestic",
                    students__category__name="Foreign",
                )
            )
            .get(name="Math")
        )
        assert math_class.name == "Math"
        assert math_class.students[0].name == "Jane"
        assert len(math_class.model_dump().get("students")) == 2
        assert math_class.teachers[0].category.department.name == "Law Department"
        assert math_class.students[0].category.department.name == "Math Department"

        classes = (
            await SchoolClass.objects.select_related(
                ["students__category__department", "teachers__category__department"]
            )
            .filter(
                ormar.and_(
                    ormar.or_(
                        students__name="Jane", students__category__name="Foreign"
                    ),
                    teachers__category__department__name="Law Department",
                )
            )
            .all()
        )
        assert len(classes) == 1
        assert classes[0].teachers[0].category.department.name == "Law Department"
        assert classes[0].students[0].category.department.name == "Math Department"
