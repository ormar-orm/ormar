"""Tests for the emit_parent_signals opt-in on proxy ormar models."""

from typing import Callable

import pytest

import ormar
from ormar import post_delete, post_save, post_update, pre_delete, pre_save, pre_update
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="signal_persons")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class QuietProxy(Person):
    ormar_config = base_ormar_config.copy(proxy=True)


class LoudProxy(Person):
    ormar_config = base_ormar_config.copy(proxy=True, emit_parent_signals=True)


class MidProxy(LoudProxy):
    ormar_config = base_ormar_config.copy(proxy=True, emit_parent_signals=True)


class Solo(ormar.Model):
    """Non-proxy concrete model with the opt-in flag set — should be a no-op."""

    ormar_config = base_ormar_config.copy(
        tablename="signal_solo", emit_parent_signals=True
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


loud_base_config = base_ormar_config.copy(emit_parent_signals=True)


class SilentProxy(Person):
    """Proxy that explicitly opts out even though its source config opts in."""

    ormar_config = loud_base_config.copy(proxy=True, emit_parent_signals=False)


create_test_database = init_tests(base_ormar_config)


def collect(target: list) -> Callable:
    async def receiver(sender, instance, **kwargs):
        target.append((sender, type(instance), kwargs))

    return receiver


def disconnect_all(*signal_attrs) -> None:
    for signal_attr in signal_attrs:
        for receiver_id in list(signal_attr._receivers):
            del signal_attr._receivers[receiver_id]


@pytest.mark.asyncio
async def test_default_proxy_does_not_emit_parent_signals() -> None:
    parent_calls: list = []
    proxy_calls: list = []

    pre_save(Person)(collect(parent_calls))
    pre_save(QuietProxy)(collect(proxy_calls))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await QuietProxy.objects.create(name="quiet")

        assert proxy_calls and proxy_calls[0][0] is QuietProxy
        assert parent_calls == []
    finally:
        disconnect_all(Person.ormar_config.signals.pre_save)
        disconnect_all(QuietProxy.ormar_config.signals.pre_save)


@pytest.mark.asyncio
async def test_opt_in_emits_parent_pre_save_with_parent_sender() -> None:
    parent_calls: list = []
    proxy_calls: list = []

    pre_save(Person)(collect(parent_calls))
    pre_save(LoudProxy)(collect(proxy_calls))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await LoudProxy.objects.create(name="loud")

        assert len(proxy_calls) == 1
        assert proxy_calls[0][0] is LoudProxy
        assert len(parent_calls) == 1
        assert parent_calls[0][0] is Person
        assert parent_calls[0][1] is LoudProxy
    finally:
        disconnect_all(Person.ormar_config.signals.pre_save)
        disconnect_all(LoudProxy.ormar_config.signals.pre_save)


@pytest.mark.asyncio
async def test_opt_in_forwards_full_lifecycle() -> None:
    captured: dict[str, list] = {
        "pre_save": [],
        "post_save": [],
        "pre_update": [],
        "post_update": [],
        "pre_delete": [],
        "post_delete": [],
    }

    pre_save(Person)(collect(captured["pre_save"]))
    post_save(Person)(collect(captured["post_save"]))
    pre_update(Person)(collect(captured["pre_update"]))
    post_update(Person)(collect(captured["post_update"]))
    pre_delete(Person)(collect(captured["pre_delete"]))
    post_delete(Person)(collect(captured["post_delete"]))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                instance = await LoudProxy.objects.create(name="full")
                await instance.update(name="renamed")
                await instance.delete()

        for name, calls in captured.items():
            assert len(calls) == 1, f"{name} not received exactly once"
            assert calls[0][0] is Person, f"{name} sender mismatch"
    finally:
        cfg = Person.ormar_config.signals
        disconnect_all(
            cfg.pre_save,
            cfg.post_save,
            cfg.pre_update,
            cfg.post_update,
            cfg.pre_delete,
            cfg.post_delete,
        )


@pytest.mark.asyncio
async def test_two_level_proxy_chain_forwards_to_every_ancestor() -> None:
    person_calls: list = []
    loud_calls: list = []
    mid_calls: list = []

    pre_save(Person)(collect(person_calls))
    pre_save(LoudProxy)(collect(loud_calls))
    pre_save(MidProxy)(collect(mid_calls))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await MidProxy.objects.create(name="chain")

        assert mid_calls and mid_calls[0][0] is MidProxy
        assert loud_calls and loud_calls[0][0] is LoudProxy
        assert person_calls and person_calls[0][0] is Person
    finally:
        disconnect_all(
            Person.ormar_config.signals.pre_save,
            LoudProxy.ormar_config.signals.pre_save,
            MidProxy.ormar_config.signals.pre_save,
        )


@pytest.mark.asyncio
async def test_emit_parent_signals_on_non_proxy_is_noop() -> None:
    calls: list = []
    pre_save(Solo)(collect(calls))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await Solo.objects.create(name="one")

        assert len(calls) == 1
        assert calls[0][0] is Solo
    finally:
        disconnect_all(Solo.ormar_config.signals.pre_save)


def test_copy_explicit_false_overrides_inherited_true() -> None:
    """Pin the ``is not None`` pattern: explicit False wins over True parent."""
    parent = base_ormar_config.copy(emit_parent_signals=True)
    inherited = parent.copy()
    overridden = parent.copy(emit_parent_signals=False)

    assert parent.emit_parent_signals is True
    assert inherited.emit_parent_signals is True
    assert overridden.emit_parent_signals is False


@pytest.mark.asyncio
async def test_proxy_with_explicit_false_does_not_forward_even_with_loud_base() -> None:
    """Runtime regression: SilentProxy was built from a True-flagged config but
    overrides to False — saving it must not forward to the parent class."""
    parent_calls: list = []
    proxy_calls: list = []

    pre_save(Person)(collect(parent_calls))
    pre_save(SilentProxy)(collect(proxy_calls))

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                await SilentProxy.objects.create(name="silent")

        assert SilentProxy.ormar_config.emit_parent_signals is False
        assert len(proxy_calls) == 1
        assert proxy_calls[0][0] is SilentProxy
        assert parent_calls == []
    finally:
        disconnect_all(
            Person.ormar_config.signals.pre_save,
            SilentProxy.ormar_config.signals.pre_save,
        )
