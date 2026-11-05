import numpy as np

from ..base import CommonSerialization
from ..numpy.stringcolumn import (
    NumpyByteFixedString, NumpyByteStringColumn, NumpyFixedString,
    NumpyStringColumn
)
from .base import ArrowColumnMixin


class ArrowStringBuffers(object):
    """
    String column read into Arrow-style buffers: concatenated bytes
    plus int64 offsets, no per-string Python objects. Assembled into
    a pyarrow array by ``clickhouse_driver.arrow.convert``.
    """
    __slots__ = ('offsets', 'data', 'nulls_map')

    def __init__(self, offsets, data, nulls_map=None):
        self.offsets = offsets
        self.data = data
        self.nulls_map = nulls_map

    def __len__(self):
        return len(self.offsets) // 8 - 1


class ArrowStringMixin(ArrowColumnMixin):
    # Reset for columns whose consumers need real items, e.g.
    # LowCardinality dictionaries.
    arrow_buffers_allowed = True

    def _buffers_encoding_ok(self):
        return self.encoding.lower() in ('utf-8', 'utf8')

    def _use_arrow_buffers(self, buf):
        return (
            self.arrow_buffers_allowed and
            self._buffers_encoding_ok() and
            type(self.serialization) is CommonSerialization and
            hasattr(buf, 'read_strings_arrow')
        )

    def _read_data(self, n_items, buf, nulls_map=None):
        if self._use_arrow_buffers(buf):
            offsets, data = buf.read_strings_arrow(n_items)
            return ArrowStringBuffers(offsets, data, nulls_map=nulls_map)

        return super(ArrowStringMixin, self)._read_data(
            n_items, buf, nulls_map=nulls_map
        )

    def _wrap_items(self, items):
        # Wrapping strings into an ndarray re-encodes them (unicode
        # dtype). Arrow consumes the raw tuple directly; the ndarray
        # is only needed for the nullable masked path.
        if self.nullable:
            return np.array(items, dtype=self.dtype)
        return items


class ArrowStringColumn(ArrowStringMixin, NumpyStringColumn):
    def read_items(self, n_items, buf):
        return self._wrap_items(
            buf.read_strings(n_items, encoding=self.encoding)
        )


class ArrowByteStringColumn(ArrowStringMixin, NumpyByteStringColumn):
    def _buffers_encoding_ok(self):
        # No decoding happens for byte strings: buffers can be used
        # regardless of encoding.
        return True

    def read_items(self, n_items, buf):
        return self._wrap_items(buf.read_strings(n_items))


class ArrowFixedString(ArrowStringMixin, NumpyFixedString):
    def _use_arrow_buffers(self, buf):
        # Fixed strings are trimmed of zero bytes on read: buffers
        # would keep the padding.
        return False

    def read_items(self, n_items, buf):
        return self._wrap_items(buf.read_fixed_strings(
            n_items, self.length, encoding=self.encoding
        ))


class ArrowByteFixedString(ArrowStringMixin, NumpyByteFixedString):
    def _use_arrow_buffers(self, buf):
        return False

    def read_items(self, n_items, buf):
        return self._wrap_items(
            buf.read_fixed_strings(n_items, self.length)
        )


def create_arrow_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']
    encoding = client_settings.get(
        'strings_encoding', NumpyStringColumn.default_encoding
    )

    if spec == 'String':
        cls = ArrowByteStringColumn if strings_as_bytes \
            else ArrowStringColumn
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = ArrowByteFixedString if strings_as_bytes \
            else ArrowFixedString
        return cls(length, encoding=encoding, **column_options)
