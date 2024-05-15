from .common import get_value

from collections.abc import Mapping, MutableMapping


class VerifiedDict(MutableMapping):
    def __init__(self, template_dict, keys_to_ignore=None, transform_func=None):
        self._enable_check = False
        self._make_subdicts_verified = True
        self._keys_to_ignore = get_value(keys_to_ignore, [])
        self._transform_func = transform_func

        self.store = dict()
        self.update(template_dict)

        self._enable_check = True
        self._make_subdicts_verified = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        if self._make_subdicts_verified and isinstance(value, dict) and key not in self._keys_to_ignore:
            value = VerifiedDict(value, transform_func=self._transform_func)
        if self._enable_check and key not in self.store:
            raise RuntimeError("Set failed: %r key is missing" % key)
        if self._transform_func is None:
            self.store[key] = value
        else:
            self.store[key] = self._transform_func(value, self.store.get(key))

    def __delitem__(self, key):
        del self.store[key]

    def __contains__(self, key):
        return key in self.store

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __repr__(self):
        cls = self.__class__.__name__
        return "{0}({1})".format(cls, str(self.store))

    def update_template_dict(self, template_dict):
        self._enable_check = False
        self.update(template_dict)
        self._enable_check = True
        return self


class FrozenDict(Mapping):
    def __init__(self, *args, **kwargs):
        self._store = dict(*args, **kwargs)
        self._hash = None

    def __len__(self):
        return len(self._store)

    def __iter__(self):
        return iter(self._store)

    def __getitem__(self, key):
        return self._store.__getitem__(key)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(tuple(sorted(self._store.items())))
        return self._hash

    def __repr__(self):
        cls = self.__class__.__name__
        items = ", ".join(map(repr, self._store.items()))
        return "{0}({1})".format(cls, items)

    def pop(self, key, default=None):
        if key in self._store:
            raise NotImplementedError("Failed to remove key from frozen dictionary")
        return default

    def as_dict(self):
        return self._store.copy()
