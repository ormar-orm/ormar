from typing import Optional

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import ormar
from tests.lifespan import init_tests, lifespan
from tests.settings import create_config

base_ormar_config = create_config()
app = FastAPI(lifespan=lifespan(base_ormar_config))


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)
    tags = ormar.ManyToMany(Tag)


create_test_database = init_tests(base_ormar_config)

ItemRequest = Item.get_pydantic(exclude={"id", "tags"}, flatten_fields="category")
ItemResponse = Item.get_pydantic(flatten_fields={"category", "tags"})
CategoryResponse = Category.get_pydantic(flatten_fields="items")


@app.post("/items/", response_model=ItemResponse)
async def create_item(item: ItemRequest):  # type: ignore
    return await Item(**item.model_dump()).save()  # type: ignore


@app.get("/items/{item_id}/", response_model=ItemResponse)
async def get_item(item_id: int):
    return await Item.objects.select_related("tags").get(id=item_id)


@app.get("/categories/{category_id}/", response_model=CategoryResponse)
async def get_category(category_id: int):
    return await Category.objects.select_related("items").get(id=category_id)


@pytest.mark.asyncio
async def test_flatten_fields_request_and_response():
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://testserver")
    async with client as client, LifespanManager(app):
        category = await Category(name="cat").save()
        tag = await Tag(name="tag").save()

        response = await client.post(
            "/items/", json={"name": "item", "category": category.id}
        )
        assert response.status_code == 200
        created = response.json()
        assert created == {
            "id": created["id"],
            "name": "item",
            "category": category.id,
            "tags": [],
        }

        item = await Item.objects.get(id=created["id"])
        await item.tags.add(tag)

        response = await client.get(f"/items/{item.id}/")
        assert response.status_code == 200
        assert response.json() == {
            "id": item.id,
            "name": "item",
            "category": category.id,
            "tags": [tag.id],
        }

        response = await client.get(f"/categories/{category.id}/")
        assert response.status_code == 200
        assert response.json() == {"id": category.id, "name": "cat", "items": [item.id]}
