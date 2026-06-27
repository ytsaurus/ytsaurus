from yt_commands import authors

from gdb_test_lib import get_core, analyze

import re


@authors("babenko")
def test_pp_unversioned_row():
    ctx, path = get_core()
    # The unversioned-row printer must expand every value with its type tag.
    out = analyze(ctx, path, "p GdbPrinterUnversionedRow")
    assert "TUnversionedOwningRow" in out
    assert re.search(r"i64\s+\{-42\}", out)
    assert re.search(r"string\s+\{hello\}", out)
    assert re.search(r"double\s+\{2\.5\}", out)
    assert re.search(r"boolean\s+\{true\}", out)
    assert "null" in out


@authors("babenko")
def test_pp_yt_print():
    ctx, path = get_core()
    # `yt-print` prints any value in full; here a row.
    out = analyze(ctx, path, "yt-print GdbPrinterUnversionedRow")
    assert "hello" in out and "-42" in out


@authors("babenko")
def test_pp_versioned_row():
    ctx, path = get_core()
    out = analyze(ctx, path, "p GdbPrinterVersionedRow")
    assert "TVersionedRow" in out
    assert "WriteTimestamps" in out
    assert "Keys" in out and "Values" in out
    assert "@32" in out  # value at write timestamp 0x20


@authors("babenko")
def test_pp_guid():
    ctx, path = get_core()
    # NYT::TGuid canonical text form: Parts32[3]..[0].
    out = analyze(ctx, path, "p GdbPrinterGuid")
    assert "44444444-33333333-22222222-11111111" in out


@authors("babenko")
def test_pp_error():
    ctx, path = get_core()
    # The error renders as a tree: code/message, origin, attributes, inner errors.
    out = analyze(ctx, path, "p GdbPrinterError")
    assert 'code=1' in out
    assert '"Disk quota exceeded"' in out
    assert 'thread="fixture"' in out                 # origin attributes
    assert re.search(r"tid=0x[0-9a-f]{8}\b", out)    # tid as 8 hex digits
    assert re.search(r"at \d{4}-\d\d-\d\dT", out)     # captured datetime (ISO)
    assert 'limit=100' in out                        # user attribute
    assert 'account="intermediate"' in out           # user attribute
    assert 'inner errors:' in out
    assert '"Underlying IO error"' in out            # recursed inner error


@authors("babenko")
def test_pp_yson():
    ctx, path = get_core()
    out = analyze(ctx, path, "p GdbPrinterYson")
    assert "TYsonString" in out
    assert "key=value" in out


@authors("babenko")
def test_pp_key_bound():
    ctx, path = get_core()
    out = analyze(ctx, path, "p GdbPrinterKeyBound")
    assert ">=" in out
    assert "1000" in out


@authors("babenko")
def test_pp_ordered_hash_map():
    ctx, path = get_core()
    # Insertion-ordered map walked via its intrusive list.
    out = analyze(ctx, path, "p GdbPrinterOrderedMap")
    assert "alpha" in out and "beta" in out and "gamma" in out
    assert "333" in out
