from ..numpy.datecolumn import NumpyDateColumn
from .base import ArrowColumnMixin


class ArrowDateColumn(ArrowColumnMixin, NumpyDateColumn):
    pass
