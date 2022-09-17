import decimal
import struct

from yt.yson import get_bytes, YsonStringProxy

MAX_DECIMAL_PRECISION = 35


class _YtNaN(object):
    """
    Python compares NaN not as YT does
    """
    def __eq__(self, other):
        if isinstance(other, decimal.Decimal):
            return other.is_nan()
        else:
            return False

    def __str__(self):
        return "NaN"


YtNaN = _YtNaN()


def _get_decimal_byte_size(precision):
    if precision < 0 or precision > 38:
        raise ValueError("Bad precision: {}".format(precision))
    elif precision <= 9:
        return 4
    elif precision <= 18:
        return 8
    else:
        return 16


_DECIMAL_CONTEXT = decimal.Context(prec=2 * MAX_DECIMAL_PRECISION)


def encode_decimal(decimal_value, precision, scale):
    if isinstance(decimal_value, _YtNaN):
        decimal_value = decimal.Decimal("NaN")
    elif isinstance(decimal_value, str):
        decimal_value = decimal.Decimal(decimal_value)
    elif not isinstance(decimal_value, decimal.Decimal):
        raise TypeError("decimal_value must be Decimal, actual type: {}".format(decimal_value.__class__.__name__))

    if decimal_value == decimal.Decimal("Inf"):
        return {
            4: b"\xFF\xFF\xFF\xFE",
            8: b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE",
            16: b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE",
        }[_get_decimal_byte_size(precision)]
    elif decimal_value == decimal.Decimal("-Inf"):
        return {
            4: b"\x00\x00\x00\x02",
            8: b"\x00\x00\x00\x00\x00\x00\x00\x02",
            16: b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02",
        }[_get_decimal_byte_size(precision)]
    elif decimal_value.is_nan():
        return {
            4: b"\xFF\xFF\xFF\xFF",
            8: b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            16: b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        }[_get_decimal_byte_size(precision)]

    scaled = decimal_value.scaleb(scale, context=_DECIMAL_CONTEXT)
    intval = int(scaled)
    if scaled - intval != 0:
        raise RuntimeError("Cannot convert {} to Decimal<{},{}> without loosing precision".format(
            decimal_value,
            precision,
            scale,
        ))

    byte_size = _get_decimal_byte_size(precision)

    intval += 1 << (8 * byte_size - 1)
    if byte_size == 4:
        return struct.pack(">I", intval)
    elif byte_size == 8:
        return struct.pack(">Q", intval)
    elif byte_size == 16:
        return struct.pack(">QQ", intval >> 64, intval & 0xFFFFFFFFFFFFFFFF)
    else:
        raise RuntimeError("Unexpected precision: {}".format(precision))


def decode_decimal(yt_binary_value, precision, scale):
    def check_int_special_value(intval, binsize):
        shift = binsize * 8 - 1
        nan_value = (1 << shift) - 1
        value_map = {
            nan_value: decimal.Decimal("NaN"),
            nan_value - 1: decimal.Decimal("Inf"),
            -nan_value + 1: decimal.Decimal("-Inf"),
        }
        return value_map.get(intval, None)

    expected_size = _get_decimal_byte_size(precision)
    if isinstance(yt_binary_value, str):
        yt_binary_value = yt_binary_value.encode("ascii")
    if isinstance(yt_binary_value, YsonStringProxy):
        yt_binary_value = get_bytes(yt_binary_value)
    if len(yt_binary_value) != _get_decimal_byte_size(precision):
        raise ValueError(
            "Binary value of Decimal<{},{}> has invalid length; expected length: {} actual length: {}"
            .format(precision, scale, expected_size, len(yt_binary_value))
        )

    if len(yt_binary_value) == 4:
        intval, = struct.unpack(">I", yt_binary_value)
    elif len(yt_binary_value) == 8:
        intval, = struct.unpack(">Q", yt_binary_value)
    elif len(yt_binary_value) == 16:
        hi, lo = struct.unpack(">QQ", yt_binary_value)
        intval = (hi << 64) | lo
    else:
        raise AssertionError("Unexpected length: {}".format(len(yt_binary_value)))
    intval -= 1 << (len(yt_binary_value) * 8 - 1)

    special = check_int_special_value(intval, len(yt_binary_value))
    if special is not None:
        return special

    return decimal.Decimal(intval).scaleb(-scale, context=_DECIMAL_CONTEXT)
