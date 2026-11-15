from typing import Any, Optional, Union, cast

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import ormar
from ormar.queryset.utils import translate_list_to_dict
from tests.lifespan import init_tests, lifespan
from tests.settings import create_config

base_ormar_config = create_config()
app = FastAPI(lifespan=lifespan(base_ormar_config))
headers = {"content-type": "application/json"}


class Department(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    department_name: str = ormar.String(max_length=100)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    course_name: str = ormar.String(max_length=100)
    completed: bool = ormar.Boolean()
    department: Optional[Department] = ormar.ForeignKey(Department)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(Course)


to_exclude = {
    "id": ...,
    "courses": {
        "__all__": {"id": ..., "students": {"__all__": {"id", "studentcourse"}}}
    },
}

exclude_all = {"id": ..., "courses": {"__all__"}}

to_exclude_ormar = {
    "id": ...,
    "courses": {"id": ..., "students": {"id", "studentcourse"}},
}


create_test_database = init_tests(base_ormar_config)


def auto_exclude_id_field(to_exclude: Any) -> Union[dict, set]:
    if isinstance(to_exclude, dict):
        for key in to_exclude.keys():
            to_exclude[key] = auto_exclude_id_field(to_exclude[key])
        to_exclude["id"] = Ellipsis
        return to_exclude
    else:
        return {"id"}


def generate_exclude_for_ids(model: type[ormar.Model]) -> dict:
    to_exclude_base = translate_list_to_dict(model._iterate_related_models())
    return cast(dict, auto_exclude_id_field(to_exclude=to_exclude_base))


to_exclude_auto = generate_exclude_for_ids(model=Department)


@app.post("/departments/", response_model=Department)
async def create_department(department: Department):
    await department.save_related(follow=True, save_all=True)
    return department


@app.get("/departments/{department_name}")
async def get_department(department_name: str):
    department = await Department.objects.select_all(follow=True, follow_reverse=2).get(
        department_name=department_name
    )
    return department.model_dump(exclude=to_exclude)


@app.get("/departments/{department_name}/second")
async def get_department_exclude(department_name: str):
    department = await Department.objects.select_all(follow=True, follow_reverse=2).get(
        department_name=department_name
    )
    return department.model_dump(exclude=to_exclude_ormar)


@app.get("/departments/{department_name}/exclude")
async def get_department_exclude_all(department_name: str):
    department = await Department.objects.select_all(follow=True, follow_reverse=2).get(
        department_name=department_name
    )
    return department.model_dump(exclude=exclude_all)


@pytest.mark.asyncio
async def test_saving_related_in_fastapi():
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://testserver")
    async with client as client, LifespanManager(app):
        payload = {
            "department_name": "Ormar",
            "courses": [
                {
                    "course_name": "basic1",
                    "completed": True,
                    "students": [{"name": "Jack"}, {"name": "Abi"}],
                },
                {
                    "course_name": "basic2",
                    "completed": True,
                    "students": [{"name": "Kate"}, {"name": "Miranda"}],
                },
            ],
        }
        response = await client.post("/departments/", json=payload, headers=headers)
        department = Department(**response.json())

        assert department.id is not None
        assert len(department.courses) == 2
        assert department.department_name == "Ormar"
        assert department.courses[0].course_name == "basic1"
        assert department.courses[0].completed
        assert department.courses[1].course_name == "basic2"
        assert department.courses[1].completed

        response = await client.get("/departments/Ormar")
        response2 = await client.get("/departments/Ormar/second")
        assert response.json() == response2.json() == payload

        response3 = await client.get("/departments/Ormar/exclude")
        assert response3.json() == {"department_name": "Ormar"}
