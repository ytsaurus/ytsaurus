_sentinel = object()


def deepget(obj, keys, default=None):
    for key in keys:
        try:
            obj = obj.get(key, _sentinel)
        except AttributeError:
            return default
    return obj if obj is not _sentinel else default
