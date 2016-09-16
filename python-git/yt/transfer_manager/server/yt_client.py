from yt.wrapper.client import Yt

from yt.packages.six import iteritems, add_metaclass

import types
from copy import deepcopy

def run_and_notify(func, self, *args, **kwargs):
    sync = kwargs.get("sync", True)
    if "sync" in kwargs:
        del kwargs["sync"]

    operation = func(self, *args, sync=False, **kwargs)
    message_queue = self.__dict__.get("message_queue")
    if message_queue:
        message_queue.put({
            "type": "operation_started",
            "operation": {
                "type": "yt",
                "id": operation.id,
                "cluster_name": operation.client._name}})
    if sync:
        operation.wait()

def start_transaction_and_notify(func, self, *args, **kwargs):
    tx = func(self, *args, **kwargs)
    message_queue = self.__dict__.get("message_queue")
    if message_queue:
        message_queue.put({
            "type": "transaction_started",
            "transaction": {
                "id": tx.transaction_id,
                "cluster_name": self._name}})

    return tx

class AddNotificationForRunMethods(type):
    def __new__(meta, name, bases, dict):
        cls = type.__new__(meta, name, bases, dict)
        for key, obj in iteritems(cls.wrapped_base.__dict__):
            if key.startswith("run_") and isinstance(obj, types.FunctionType):
                setattr(cls, key, (lambda func=obj: lambda *args, **kwargs: run_and_notify(func, *args, **kwargs))())
            if key.startswith("Transaction") and isinstance(obj, types.FunctionType):
                setattr(cls, key, (lambda func=obj: lambda *args, **kwargs: start_transaction_and_notify(func, *args, **kwargs))())
        return cls


@add_metaclass(AddNotificationForRunMethods)
class YtClientWithNotifications(Yt):
    wrapped_base = Yt

    def __init__(self, *args, **kwargs):
        super(YtClientWithNotifications, self).__init__(*args, **kwargs)
        self.config["operation_tracker"]["stderr_logging_level"] = "DEBUG"

def clone_client(yt_client):
    return type(yt_client)(config=deepcopy(yt_client.config))
