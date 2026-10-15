import random

import pytest

from benchmarks.conftest import WideRecord

pytestmark = pytest.mark.asyncio


async def save_wide_records(num_models: int) -> list[WideRecord]:
    """
    Saves wide models one by one, relying on defaults for most of the columns.

    :param num_models: number of models to save
    :type num_models: int
    :return: saved models
    :rtype: list[WideRecord]
    """
    records = [
        WideRecord(level=random.choice([10, 20, 30]), text=f"record {i}")
        for i in range(0, num_models)
    ]
    for record in records:
        await record.save()
    return records


async def create_wide_records(num_models: int) -> list[WideRecord]:
    """
    Creates wide models one by one through the queryset.

    :param num_models: number of models to create
    :type num_models: int
    :return: created models
    :rtype: list[WideRecord]
    """
    return [
        await WideRecord.objects.create(
            level=random.choice([10, 20, 30]), text=f"record {i}"
        )
        for i in range(0, num_models)
    ]


async def get_all_wide_records() -> list[WideRecord]:
    """
    Loads all wide models from the database.

    :return: loaded models
    :rtype: list[WideRecord]
    """
    return await WideRecord.objects.all()


async def get_one_wide_record(record_id: int) -> WideRecord:
    """
    Loads a single wide model by primary key.

    :param record_id: primary key of the model
    :type record_id: int
    :return: loaded model
    :rtype: WideRecord
    """
    return await WideRecord.objects.get(id=record_id)


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_saving_wide_models_individually(aio_benchmark, num_models: int):
    records = aio_benchmark(save_wide_records)(num_models)
    for record in records:
        assert record.id is not None


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_creating_wide_models_individually(aio_benchmark, num_models: int):
    records = aio_benchmark(create_wide_records)(num_models)
    for record in records:
        assert record.id is not None


@pytest.mark.parametrize("num_models", [250, 500, 1000])
async def test_get_all_wide_models(
    aio_benchmark, num_models: int, wide_records_in_db: list[WideRecord]
):
    records = aio_benchmark(get_all_wide_records)()
    assert len(records) == num_models
    assert records[0].col_json1 == wide_records_in_db[0].col_json1


@pytest.mark.parametrize("num_models", [250, 500, 1000])
async def test_get_one_wide_model(
    aio_benchmark, num_models: int, wide_records_in_db: list[WideRecord]
):
    record = aio_benchmark(get_one_wide_record)(wide_records_in_db[0].id)
    assert record.id == wide_records_in_db[0].id
