from ..numpy.floatcolumn import NumpyFloat32Column, NumpyFloat64Column
from .base import ArrowColumnMixin


class ArrowFloat32Column(ArrowColumnMixin, NumpyFloat32Column):
    pass


class ArrowFloat64Column(ArrowColumnMixin, NumpyFloat64Column):
    pass
