from .helpers import is_schema_module_available

import sys
import types
import typing


# types.GenericAlias can be subclassed only since 3.9.2.
# https://docs.python.org/3/library/types.html#types.GenericAlias
if sys.version_info >= (3, 9, 2):
    GenericAlias = types.GenericAlias
elif is_schema_module_available(skiff=False):
    # Hack to pass argument `_root=True` to the `__init_subclass__` in compatible with python2 syntax manner
    class _RootTrue:
        def __init_subclass__(cls, **kwargs):
            kwargs["_root"] = True
            super().__init_subclass__(cls, **kwargs)

    class GenericAlias(_RootTrue, typing._GenericAlias):
        pass
else:
    GenericAlias = object


class _VariantGenericAlias(GenericAlias):
    def copy_with(self, params):
        return Variant[params]

    def __eq__(self, other):
        if not isinstance(other, _VariantGenericAlias):
            return NotImplementedError
        return self.__args__ == other.__args__

    def __hash__(self):
        return hash(self.__args__)

    def __instancecheck__(self, obj):
        return self.__subclasscheck__(type(obj))

    def __subclasscheck__(self, cls):
        for arg in self.__args__:
            if issubclass(cls, arg):
                return True

    def __reduce__(self):
        func, (origin, args) = super().__reduce__()
        return func, (Variant, args)


class Variant:
    """
    Variant type; Variant[T1, T2, ..., TN] matches any type of T1, T2, ..., TN.

    Variant is just like typing.Union, but leaves type list unchanged
    (i.e. not flatten and do not removes duplicates)
    """
    def __class_getitem__(cls, parameters):
        if parameters == ():
            raise TypeError("Cannot make a Variant of no types.")

        if not isinstance(parameters, tuple):
            parameters = (parameters,)

        return _VariantGenericAlias(cls, parameters)

    @classmethod
    def _name(cls):
        return "{module}.{classname}".format(module=cls.__module__, classname=cls.__qualname__)

    def __new__(cls, *args, **kwargs):
        raise TypeError("Type {} cannot be instantiated".format(Variant._name()))

    def __init_subclass__(cls, *args, **kwargs):
        raise TypeError("{} cannot be subclassed".format(Variant._name()))
