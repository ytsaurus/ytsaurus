from ..numpy.datetimecolumn import (
    NumpyDateTime64Column, NumpyDateTimeColumn, create_numpy_datetime_column
)
from .base import ArrowColumnMixin


class ArrowDateTimeColumn(ArrowColumnMixin, NumpyDateTimeColumn):
    pass


class ArrowDateTime64Column(ArrowColumnMixin, NumpyDateTime64Column):
    pass


def create_arrow_datetime_column(spec, column_options):
    return create_numpy_datetime_column(
        spec, column_options,
        column_classes=(ArrowDateTimeColumn, ArrowDateTime64Column)
    )
