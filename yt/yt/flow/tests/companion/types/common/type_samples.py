"""Shared typed sample rows and verification for the companion type-checking
tests. Both the Java and Python companion tests insert these rows, let the
companion mirror every field, and assert the output is byte-for-byte equal —
covering every distinct wire value type (string, int64, uint64, double, bool).

The values are chosen to stress the wire encoding: INT64_MIN/MAX, uint64 values
with the high bit set (held as a negative long on the Java side), fractional and
negative doubles (whose raw bits force the fixed-8-byte path), and non-ASCII
strings."""

import collections

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_HIGH_BIT = 2**63 + 12345  # high bit set: a valid uint64 stored as a negative long
UINT64_MAX = 2**64 - 1

# Each row carries a distinct ``key`` so input and output can be matched 1:1.
SAMPLE_ROWS = [
    {"key": "row_zero", "f_string": "", "f_int64": 0, "f_uint64": 0, "f_double": 0.0, "f_bool": False},
    {"key": "row_pos", "f_string": "ascii-value", "f_int64": 42, "f_uint64": 1, "f_double": 3.14159, "f_bool": True},
    {"key": "row_neg", "f_string": "negatives", "f_int64": -12345, "f_uint64": 7, "f_double": -2.5e10, "f_bool": False},
    {
        "key": "row_limits",
        "f_string": "limits",
        "f_int64": INT64_MIN,
        "f_uint64": UINT64_MAX,
        "f_double": 1.7976931348623157e308,
        "f_bool": True,
    },
    {
        "key": "row_highbit",
        "f_string": "unicode 世界 🌍",
        "f_int64": INT64_MAX,
        "f_uint64": UINT64_HIGH_BIT,
        "f_double": 2.2250738585072014e-308,
        "f_bool": True,
    },
]

PAYLOAD_FIELDS = ("f_string", "f_int64", "f_uint64", "f_double", "f_bool")

# Cypress queue schema shared by both companion tests' yt_sync bootstrap: the
# typed payload columns (matching ``PAYLOAD_FIELDS`` plus ``key``) followed by
# the system columns every ordered-dynamic queue table carries.
QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "f_string", "type": "string"},
    {"name": "f_int64", "type": "int64"},
    {"name": "f_uint64", "type": "uint64"},
    {"name": "f_double", "type": "double"},
    {"name": "f_bool", "type": "boolean"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def _normalize(value):
    """Decode string columns the yt client may hand back as bytes."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _check_field(key, column, got, want):
    """Compare one field strictly by value, tolerant of the yt client's scalar
    representation (bytes vs str, bool vs int) but strict on the value itself."""
    context = f"key={key!r} column={column!r}: got {got!r}, expected {want!r}"
    if column == "f_string":
        assert _normalize(got) == want, context
    elif column == "f_bool":
        assert isinstance(got, (bool, int)) and bool(got) == want, context
    elif column == "f_double":
        # Exact equality on purpose: the fixed-8-byte wire encoding round-trips
        # losslessly, so any drift means a serialization bug (e.g. the historical
        # varint-double mismatch with the C++ worker).
        assert isinstance(got, float) and got == want, context
    else:  # f_int64 / f_uint64
        assert not isinstance(got, bool) and int(got) == want, context


def assert_roundtrip(actual_rows):
    """Assert the companion mirrored every sample row field-for-field.

    ``actual_rows`` is the list of dicts read from the output queue (``key`` plus
    every payload field). The output must mirror the input exactly: each sample
    key appears once and only once, with no extra or duplicated rows -- so the
    test also catches replay / duplicated-consumption / idempotency bugs, not
    just dropped or corrupted fields.
    """
    key_counts = collections.Counter(_normalize(row["key"]) for row in actual_rows)

    duplicates = sorted(key for key, count in key_counts.items() if count > 1)
    assert not duplicates, f"companion emitted duplicate output for keys: {duplicates}"

    expected_keys = {row["key"] for row in SAMPLE_ROWS}
    actual_keys = set(key_counts)

    missing = expected_keys - actual_keys
    assert not missing, f"companion never emitted output for keys: {sorted(missing)}"

    unexpected = actual_keys - expected_keys
    assert not unexpected, f"companion emitted unexpected output for keys: {sorted(unexpected)}"

    assert len(actual_rows) == len(SAMPLE_ROWS), f"output row count {len(actual_rows)} != expected {len(SAMPLE_ROWS)}"

    by_key = {_normalize(row["key"]): row for row in actual_rows}
    for expected in SAMPLE_ROWS:
        actual = by_key[expected["key"]]
        for column in PAYLOAD_FIELDS:
            _check_field(expected["key"], column, actual[column], expected[column])
