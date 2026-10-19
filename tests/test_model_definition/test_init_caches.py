from typing import Optional

import ormar
from tests.settings import create_config

base_ormar_config = create_config()


class Plain(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="init_cache_plain")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="init_cache_owners")

    id: int = ormar.Integer(primary_key=True)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="init_cache_pets")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


class Counter:
    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __call__(self, kwargs):
        self.calls += 1
        return self.original(kwargs)


def test_relation_less_model_skips_nested_json_serialization(monkeypatch):
    counter = Counter(Plain.serialize_nested_models_json_fields)
    monkeypatch.setattr(Plain, "serialize_nested_models_json_fields", counter)
    Plain(id=1, name="x")
    assert counter.calls == 0


def test_model_with_relations_still_serializes_and_registers(monkeypatch):
    counter = Counter(Pet.serialize_nested_models_json_fields)
    monkeypatch.setattr(Pet, "serialize_nested_models_json_fields", counter)
    owner = Owner(id=1)
    pet = Pet(id=2, owner=owner)
    assert counter.calls == 1
    assert pet.owner is owner
    assert owner.pets == [pet]


def test_internal_construct_serializes_nested_models_for_relations(monkeypatch):
    counter = Counter(Pet.serialize_nested_models_json_fields)
    monkeypatch.setattr(Pet, "serialize_nested_models_json_fields", counter)
    pet = Pet._internal_construct(id=3, owner=Owner(id=4))
    assert counter.calls == 1
    assert pet.owner.id == 4
