from ..numpy.boolcolumn import NumpyBoolColumn
from .base import ArrowColumnMixin


class ArrowBoolColumn(ArrowColumnMixin, NumpyBoolColumn):
    pass
