import numpy as np


class ArrowColumnMixin(object):
    """
    Read-path override for Arrow result queries
    (``query_arrow``/``query_arrow_stream``). Values are kept in
    wire-friendly form and assembled into pyarrow arrays by
    ``clickhouse_driver.arrow.convert``.
    """

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            # Arrow stores nullable columns the same way the wire
            # sends them: raw values plus a validity mask. No need to
            # convert to an object ndarray with None like the NumPy
            # family does.
            return np.ma.MaskedArray(items, mask=nulls_map)

        return items
