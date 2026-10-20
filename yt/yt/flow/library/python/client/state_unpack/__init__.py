from collections.abc import Mapping

import yt.yson as yson

from yt.yt.flow.library.python.client.state_unpack.bindings import unpack_state_bytes


def _column(row, name):
    return row.get(name, row.get(name.encode()))


def _is_null(value, name):
    if value is None:
        return True
    if isinstance(value, yson.YsonEntity):
        value_type = value.attributes.get("type", value.attributes.get(b"type", "null"))
        if value_type not in ("null", b"null"):
            raise ValueError(f"Unexpected entity type in column {name!r}")
        return True
    return False


def _packed_column(row, name):
    value = _column(row, name)
    return None if _is_null(value, name) else yson.get_bytes(value)


def unpack_state(row, encoding="utf-8"):
    """Return the logical state from an internal state row."""
    if not isinstance(row, Mapping):
        raise TypeError("State row must be a mapping")
    value = _column(row, "state")
    if not _is_null(value, "state"):
        return value
    compressed = _packed_column(row, "compressed")
    patch = _packed_column(row, "compressed_patch")
    format = _column(row, "format")
    if _is_null(format, "format"):
        format = None
    else:
        if not isinstance(format, Mapping):
            raise TypeError("State format must be a mapping or null")
        format = yson.dumps(format)
    unpacked = unpack_state_bytes(compressed, patch, format)
    return yson.loads(unpacked, encoding=encoding) if unpacked else yson.YsonEntity()
