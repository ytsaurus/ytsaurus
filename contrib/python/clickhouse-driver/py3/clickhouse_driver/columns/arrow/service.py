from ... import errors
from ..nullablecolumn import create_nullable_column
from ..numpy.lowcardinalitycolumn import create_numpy_low_cardinality_column
from ..numpy.tuplecolumn import create_tuple_column
from ..service import aliases
from .boolcolumn import ArrowBoolColumn
from .datecolumn import ArrowDateColumn
from .datetimecolumn import create_arrow_datetime_column
from .floatcolumn import ArrowFloat32Column, ArrowFloat64Column
from .intcolumn import (
    ArrowInt8Column, ArrowInt16Column, ArrowInt32Column, ArrowInt64Column,
    ArrowUInt8Column, ArrowUInt16Column, ArrowUInt32Column, ArrowUInt64Column
)
from .stringcolumn import create_arrow_string_column

# Fixed-width types differ from their NumPy counterparts only in
# nullable reads: ArrowColumnMixin keeps the values and the nulls map
# intact instead of converting to an object ndarray.
column_by_type = {c.ch_type: c for c in [
    ArrowDateColumn,
    ArrowFloat32Column, ArrowFloat64Column,
    ArrowInt8Column, ArrowInt16Column, ArrowInt32Column, ArrowInt64Column,
    ArrowUInt8Column, ArrowUInt16Column, ArrowUInt32Column, ArrowUInt64Column,
    ArrowBoolColumn
]}


def get_arrow_column_by_spec(spec, column_options):
    def create_column_with_options(x):
        return get_arrow_column_by_spec(x, column_options)

    if spec == 'String' or spec.startswith('FixedString'):
        return create_arrow_string_column(spec, column_options)

    elif spec.startswith('DateTime'):
        return create_arrow_datetime_column(spec, column_options)

    elif spec.startswith('Tuple'):
        return create_tuple_column(
            spec, create_column_with_options, column_options
        )

    elif spec.startswith('Nullable'):
        return create_nullable_column(spec, create_column_with_options)

    elif spec.startswith('LowCardinality'):
        column = create_numpy_low_cardinality_column(
            spec, create_column_with_options, column_options
        )
        # Dictionary values are consumed by pd.Categorical: real items
        # are needed, not Arrow buffers.
        column.nested_column.arrow_buffers_allowed = False
        return column
    else:
        for alias, primitive in aliases:
            if spec.startswith(alias):
                return create_column_with_options(
                    primitive + spec[len(alias):]
                )

        if spec in column_by_type:
            cls = column_by_type[spec]
            return cls(**column_options)

        raise errors.UnknownTypeError('Unknown type {}'.format(spec))
