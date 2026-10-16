from ..numpy.intcolumn import (
    NumpyInt8Column, NumpyInt16Column, NumpyInt32Column, NumpyInt64Column,
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
)
from .base import ArrowColumnMixin


class ArrowInt8Column(ArrowColumnMixin, NumpyInt8Column):
    pass


class ArrowInt16Column(ArrowColumnMixin, NumpyInt16Column):
    pass


class ArrowInt32Column(ArrowColumnMixin, NumpyInt32Column):
    pass


class ArrowInt64Column(ArrowColumnMixin, NumpyInt64Column):
    pass


class ArrowUInt8Column(ArrowColumnMixin, NumpyUInt8Column):
    pass


class ArrowUInt16Column(ArrowColumnMixin, NumpyUInt16Column):
    pass


class ArrowUInt32Column(ArrowColumnMixin, NumpyUInt32Column):
    pass


class ArrowUInt64Column(ArrowColumnMixin, NumpyUInt64Column):
    pass
