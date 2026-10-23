import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Callable, Union

from ormar.exceptions import SignalDefinitionError

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


def callable_accepts_kwargs(func: Callable) -> bool:
    """
    Checks if function accepts **kwargs.

    :param func: function which signature needs to be checked
    :type func: function
    :return: result of the check
    :rtype: bool
    """
    return any(
        p
        for p in inspect.signature(func).parameters.values()
        if p.kind == p.VAR_KEYWORD
    )


def make_id(target: Any) -> Union[int, tuple[int, int]]:
    """
    Creates id of a function or method to be used as key to store signal

    :param target: target which id we want
    :type target: Any
    :return: id of the target
    :rtype: int
    """
    if hasattr(target, "__func__"):
        return id(target.__self__), id(target.__func__)
    return id(target)


class Signal:
    """
    Signal that notifies all receiver functions.
    In ormar used by models to send pre_save, post_save etc. signals.
    """

    def __init__(self) -> None:
        self._receivers: dict[Union[int, tuple[int, int]], Callable] = {}

    def connect(self, receiver: Callable) -> None:
        """
        Connects given receiver function to the signal.

        :raises SignalDefinitionError: if receiver is not callable
        or not accept **kwargs
        :param receiver: receiver function
        :type receiver: Callable
        """
        if not callable(receiver):
            raise SignalDefinitionError("Signal receivers must be callable.")
        if not callable_accepts_kwargs(receiver):
            raise SignalDefinitionError(
                "Signal receivers must accept **kwargs argument."
            )
        new_receiver_key = make_id(receiver)
        if new_receiver_key not in self._receivers:
            self._receivers[new_receiver_key] = receiver

    def disconnect(self, receiver: Callable) -> bool:
        """
        Removes the receiver function from the signal.

        :param receiver: receiver function
        :type receiver: Callable
        :return: flag if receiver was removed
        :rtype: bool
        """
        new_receiver_key = make_id(receiver)
        receiver_func: Union[Callable, None] = self._receivers.pop(
            new_receiver_key, None
        )
        return True if receiver_func is not None else False

    async def send(self, sender: type["Model"], **kwargs: Any) -> None:
        """
        Notifies all receiver functions with given kwargs
        :param sender: model that sends the signal
        :type sender: type["Model"]
        :param kwargs: arguments passed to receivers
        :type kwargs: Any
        """
        if not self._receivers:
            return
        receivers = [
            receiver_func(sender=sender, **kwargs)
            for receiver_func in self._receivers.values()
        ]
        await asyncio.gather(*receivers)


class SignalEmitter(dict):
    """
    Emitter that registers the signals in internal dictionary.
    If signal with given name does not exist it's auto added on access.
    """

    def __getattr__(self, item: str) -> Signal:
        return self.setdefault(item, Signal())

    def __setattr__(self, key: str, value: Signal) -> None:
        if not isinstance(value, Signal):
            raise SignalDefinitionError(f"{value} is not valid signal")
        self[key] = value
