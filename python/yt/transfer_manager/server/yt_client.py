from yt.wrapper.client import Yt

import types

def run_and_notify(func, self, *args, **kwargs):
    sync = kwargs.get("sync", True)
    if "sync" in kwargs:
        del kwargs["sync"]

    operation = func(self, *args, sync=False, **kwargs)
    message_queue = self.__dict__.get("message_queue")
    if message_queue:
        message_queue.put({"type": "operation_started",
                           "operation": {
                               "id": operation.id,
                               "cluster_name": operation.client._name
                            }})
    if sync:
        operation.wait()

class AddNotificationForRunMethods(type):
    def __new__(meta, name, bases, dict):
        cls = type.__new__(meta, name, bases, dict)
        for key, obj in cls.wrapped_base.__dict__.iteritems():
            if key.startswith("run_") and isinstance(obj, types.FunctionType):
                setattr(cls, key, (lambda func=obj: lambda *args, **kwargs: run_and_notify(func, *args, **kwargs))())
        return cls


class YtClientWithNotifications(Yt):
    __metaclass__ = AddNotificationForRunMethods

    wrapped_base = Yt

    def __init__(self, *args, **kwargs):
        super(YtClientWithNotifications, self).__init__(*args, **kwargs)
