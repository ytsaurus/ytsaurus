from yt.packages.six import PY3

import types
import typing


GenericAlias = types.GenericAlias if PY3 else object


class _VariantGenericAlias(GenericAlias):
    def copy_with(self, params):
        return Variant[params]

    def __eq__(self, other):
        if not isinstance(other, _VariantGenericAlias):
            return NotImplemented
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


class _SpecialForm:
    __slots__ = ('_name', '__doc__', '_getitem')

    def __init__(self, getitem):
        self._getitem = getitem
        self._name = getitem.__name__
        self.__doc__ = getitem.__doc__

    def __getattr__(self, item):
        if item in {'__name__', '__qualname__'}:
            return self._name

        raise AttributeError(item)

    def __mro_entries__(self, bases):
        raise TypeError("Cannot subclass {}".format(repr(self)))

    def __repr__(self):
        return __name__ + '.' + self._name

    def __reduce__(self):
        return self._name

    def __call__(self, *args, **kwds):
        raise TypeError("Cannot instantiate {}".format(repr(self)))

    def __or__(self, other):
        return typing.Union[self, other]

    def __ror__(self, other):
        return typing.Union[other, self]

    def __instancecheck__(self, obj):
        raise TypeError("{} cannot be used with isinstance()".format(self))

    def __subclasscheck__(self, cls):
        raise TypeError("{} cannot be used with issubclass()".format(self))

    def __getitem__(self, parameters):
        return self._getitem(self, parameters)


@_SpecialForm
def Variant(self, parameters):
    """
    Variant type; Variant[T1, T2, ..., TN] matches any type of T1, T2, ..., TN.

    Variant is just like typing.Union, but leaves type list unchanged
    (i.e. not flatten and do not removes duplicates)
    """

    if parameters == ():
        raise TypeError("Cannot make a Variant of no types.")

    if not isinstance(parameters, tuple):
        parameters = (parameters,)

    return _VariantGenericAlias(self, parameters)
