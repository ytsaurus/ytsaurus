import yt.yson
from yt.yson import * # noqa
from yt.yson import _loads_from_native_str, _dumps_to_native_str # noqa


def load(*args, **kwargs):
    return yt.yson.load(*args, **kwargs)


def loads(*args, **kwargs):
    return yt.yson.loads(*args, **kwargs)


def dump(*args, **kwargs):
    return yt.yson.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    return yt.yson.dumps(*args, **kwargs)
