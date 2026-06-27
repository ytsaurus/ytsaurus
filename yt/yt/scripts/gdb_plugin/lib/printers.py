# YT-specific gdb pretty-printers.
#
# Only types that `ya tool gdb` (devtools/gdb) does NOT already cover live here:
# the table-client rows/values, NYT::TGuid, NYT::TError/TErrorOr and
# NYT::NYson::TYsonString. TString / TStringBuf / containers (TCompactVector,
# THashMap, ...) are already printed by the Arcadia bundle, so we never shadow
# them.
#
# Registered at import time, so the package auto-loader picks this up like any
# other module. Also exposes `yt-print <expr>` to print any YT value in full.

import datetime
import re

import gdb
import gdb.printing

import _announce


def _lookup_type(name):
    try:
        return gdb.lookup_type(name)
    except gdb.error:
        return None


# ---------------------------------------------------------------------------
# Table client: unversioned / versioned values and rows, key bounds.
# ---------------------------------------------------------------------------

# EValueType (library: yt/yt/client/table_client/row_base.h).
_VALUE_TYPES = {
    0x00: "min",
    0x01: "bottom",
    0x02: "null",
    0x03: "i64",
    0x04: "ui64",
    0x05: "double",
    0x06: "boolean",
    0x10: "string",
    0x11: "any",
    0x12: "composite",
    0xef: "max",
}


def _read_string_value(val):
    """The String payload is not zero-terminated -- honor Length."""
    length = int(val["Length"])
    data = val["Data"]["String"]
    if int(data) == 0:
        return ""
    raw = data.string(length=length, errors="backslashreplace")
    return raw


def _unpack_unversioned_value(val):
    """Returns (id, type_name, data_str) for a TUnversionedValue."""
    value_id = int(val["Id"])
    type_code = int(val["Type"].cast(gdb.lookup_type("unsigned char")))
    type_name = _VALUE_TYPES.get(type_code, "?0x%x" % type_code)
    data = ""
    if type_name == "i64":
        data = str(int(val["Data"]["Int64"]))
    elif type_name == "ui64":
        data = str(int(val["Data"]["Uint64"]))
    elif type_name == "double":
        data = str(float(val["Data"]["Double"]))
    elif type_name == "boolean":
        data = "true" if int(val["Data"]["Boolean"]) else "false"
    elif type_name in ("string", "any", "composite"):
        data = _read_string_value(val)
    return value_id, type_name, data


def _format_unversioned_value(val):
    value_id, type_name, data = _unpack_unversioned_value(val)
    if data != "":
        data = "{%s}" % data
    return "#%-2d  %-9s  %s" % (value_id, type_name, data)


def _format_versioned_value(val):
    ts = int(val["Timestamp"])
    value_id, type_name, data = _unpack_unversioned_value(val)
    if data != "":
        data = "{%s}" % data
    return "#%-2d  %-9s  @%s %s" % (value_id, type_name, ts, data)


def _values_after(header_ptr, count, value_type):
    """The value array immediately follows the header."""
    base = (header_ptr + 1).cast(value_type.pointer())
    return [base[i] for i in range(count)]


def _format_unversioned_row_from_header(header_ptr, name):
    value_type = _lookup_type("NYT::NTableClient::TUnversionedValue")
    if header_ptr is None or int(header_ptr) == 0 or value_type is None:
        return "%s <null>" % name
    count = int(header_ptr["Count"])
    lines = ["%s [%d] {" % (name, count)]
    for v in _values_after(header_ptr, count, value_type):
        lines.append("  " + _format_unversioned_value(v))
    lines.append("}")
    return "\n".join(lines)


def _format_versioned_row_from_header(header_ptr, name):
    uv = _lookup_type("NYT::NTableClient::TUnversionedValue")
    vv = _lookup_type("NYT::NTableClient::TVersionedValue")
    ts = _lookup_type("NYT::NTransactionClient::TTimestamp") or _lookup_type("unsigned long")
    if header_ptr is None or int(header_ptr) == 0 or None in (uv, vv, ts):
        return "%s <null>" % name
    value_count = int(header_ptr["ValueCount"])
    key_count = int(header_ptr["KeyCount"])
    wts_count = int(header_ptr["WriteTimestampCount"])
    dts_count = int(header_ptr["DeleteTimestampCount"])

    # Layout: header, then write ts[], delete ts[], key values[], values[].
    wts = (header_ptr + 1).cast(ts.pointer())
    dts = (wts + wts_count).cast(ts.pointer())
    keys = (dts + dts_count).cast(uv.pointer())
    values = (keys + key_count).cast(vv.pointer())

    lines = ["%s {" % name]
    lines.append("  WriteTimestamps [%s]" % ", ".join(str(int(wts[i])) for i in range(wts_count)))
    lines.append("  DeleteTimestamps [%s]" % ", ".join(str(int(dts[i])) for i in range(dts_count)))
    lines.append("  Keys [")
    for i in range(key_count):
        lines.append("    " + _format_unversioned_value(keys[i]))
    lines.append("  ]")
    lines.append("  Values [")
    for i in range(value_count):
        lines.append("    " + _format_versioned_value(values[i]))
    lines.append("  ]")
    lines.append("}")
    return "\n".join(lines)


def _unversioned_header_ptr(row_val):
    """Header pointer for either a TUnversionedRow (stored as Header_) or a
    TUnversionedOwningRow (whose blob starts at RowData_.Data_), or None."""
    header_type = _lookup_type("NYT::NTableClient::TUnversionedRowHeader")
    if header_type is None:
        return None
    for accessor in (
        lambda v: v["Header_"],                 # TUnversionedRow
        lambda v: v["RowData_"]["Data_"],       # TUnversionedOwningRow
    ):
        try:
            return accessor(row_val).cast(header_type.pointer())
        except gdb.error:
            continue
    return None


class TUnversionedValuePrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _format_unversioned_value(self.val)


class TVersionedValuePrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _format_versioned_value(self.val)


class TUnversionedRowPrinter:
    NAME = "TUnversionedRow"

    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _format_unversioned_row_from_header(_unversioned_header_ptr(self.val), self.NAME)


class TUnversionedOwningRowPrinter(TUnversionedRowPrinter):
    NAME = "TUnversionedOwningRow"


class TVersionedRowPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        header_type = _lookup_type("NYT::NTableClient::TVersionedRowHeader")
        if header_type is None:
            return "TVersionedRow"
        try:
            header = self.val["Header_"].cast(header_type.pointer())
        except gdb.error:
            return "TVersionedRow <null>"
        return _format_versioned_row_from_header(header, "TVersionedRow")


class TKeyBoundPrinter:
    """`>=`/`<`/... prefix followed by the (compact) prefix row.

    Handles both TKeyBound (TUnversionedRow prefix) and TOwningKeyBound
    (TUnversionedOwningRow prefix) via the shared header accessor."""

    def __init__(self, val):
        self.val = val

    def to_string(self):
        is_upper = int(self.val["IsUpper"])
        is_inclusive = int(self.val["IsInclusive"])
        rel = {
            (1, 1): "<=", (1, 0): "<",
            (0, 1): ">=", (0, 0): ">",
        }[(is_upper, is_inclusive)]
        header = _unversioned_header_ptr(self.val["Prefix"])
        body = ""
        if header is not None and int(header) != 0:
            count = int(header["Count"])
            vt = _lookup_type("NYT::NTableClient::TUnversionedValue")
            parts = [_format_unversioned_value(v).strip()
                     for v in _values_after(header, count, vt)]
            body = "[" + "; ".join(parts) + "]"
        return "%s %s" % (rel, body)


# ---------------------------------------------------------------------------
# NYT::TGuid (distinct from util's TGUID, which the Arcadia bundle prints).
# ---------------------------------------------------------------------------

class TGuidPrinter:
    def __init__(self, val):
        self.val = val

    def to_string(self):
        parts = self.val["Parts32"]
        # Canonical text form: Parts32[3] first, Parts32[0] last.
        return "-".join("%x" % int(parts[i]) for i in (3, 2, 1, 0))


# ---------------------------------------------------------------------------
# Generic container/value readers shared below (libc++ first, libstdc++ fallback).
# ---------------------------------------------------------------------------

def _vector_items(vec):
    """Elements of a std::vector as a list of gdb.Values."""
    try:
        begin, end = vec["__begin_"], vec["__end_"]           # libc++
    except gdb.error:
        impl = vec["_M_impl"]                                 # libstdc++
        begin, end = impl["_M_start"], impl["_M_finish"]
    n = int(end - begin)
    return [begin[i] for i in range(n)]


def _array_chars(arr, length):
    """Text of a std::array<char, N> honoring an explicit length."""
    for key in ("__elems_", "_M_elems"):
        try:
            return arr[key].string(length=length, errors="backslashreplace")
        except gdb.error:
            continue
    try:
        return arr.string(length=length, errors="backslashreplace")
    except gdb.error:
        return ""


def _string_view_text(sv):
    """Text of a std::basic_string_view (or a TStringBuf deriving it)."""
    base = sv
    try:
        for f in sv.type.strip_typedefs().fields():
            if f.is_base_class and "string_view" in str(f.type):
                base = sv.cast(f.type)
                break
    except (gdb.error, TypeError):
        pass
    for d, s in (("__data_", "__size_"), ("_M_str", "_M_len")):
        try:
            ptr, n = base[d], int(base[s])
        except gdb.error:
            continue
        if int(ptr) == 0 or n == 0:
            return ""
        try:
            return ptr.string(length=n, errors="backslashreplace")
        except gdb.error:
            return ""
    return ""


def _unquote(text):
    """Strip one layer of gdb's std::string quoting/escaping for inline display."""
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return text


# ---------------------------------------------------------------------------
# NYT::TOrderedHashMap -- insertion-ordered map kept as an intrusive list of
# std::pair<const K, V> items. Not covered by the Arcadia bundle; also reused by
# the TError attribute rendering below.
# ---------------------------------------------------------------------------

def _ordered_hash_map_items(map_val):
    """Yield (key, value) gdb.Values for a TOrderedHashMap, in insertion order."""
    end = map_val["List_"]["End_"]                    # the sentinel list node
    item_type = end.type.strip_typedefs().template_argument(0)
    pair_base, list_off = None, 0
    for f in item_type.fields():
        if not f.is_base_class:
            continue
        if pair_base is None and "pair" in str(f.type):
            pair_base = f.type
        elif "ListItem" in str(f.type):
            list_off = f.bitpos // 8
    if pair_base is None:
        return
    sentinel = int(end.address)
    node = end["Next_"]
    guard = 0
    while int(node) != sentinel and guard < 1_000_000:
        item = gdb.Value(int(node) - list_off).cast(item_type.pointer()).dereference()
        pair = item.cast(pair_base)
        yield pair["first"], pair["second"]
        node = node["Next_"]
        guard += 1


class TOrderedHashMapPrinter:
    def __init__(self, val):
        self.val = val

    def children(self):
        for i, (k, v) in enumerate(_ordered_hash_map_items(self.val)):
            yield ("[%d]" % i, k)
            yield ("[%d]" % i, v)

    def display_hint(self):
        return "map"

    def to_string(self):
        try:
            n = sum(1 for _ in _ordered_hash_map_items(self.val))
        except gdb.error:
            n = "?"
        return "TOrderedHashMap[%s]" % n


# ---------------------------------------------------------------------------
# NYT::TError / TErrorOr<void>.
#
# TError holds a unique_ptr<TImpl>, but TImpl is defined out-of-line (error.cpp)
# and the binary's debug info carries it as an *incomplete* type -- so its fields
# can't be named directly. Each field's offset within TImpl is recovered by
# disassembling its const accessor (a one-liner computing `this + offset`), and
# the field is read through the accessor's *return type*, which IS complete.
# Read-only and core-safe; the offset patterns cover both -O0 and optimized code.
# We render code, message, origin (host/pid/tid/thread), user attributes, and
# recurse into inner errors.
# ---------------------------------------------------------------------------

_TIMPL = "NYT::TErrorOr<void>::TImpl"
_GET_CODE = "%s::GetCode() const" % _TIMPL
_GET_MESSAGE = "%s::GetMessage() const" % _TIMPL
_ORIGIN_ATTRS = "%s::OriginAttributes() const" % _TIMPL
_ATTRIBUTES = "%s::Attributes() const" % _TIMPL
_INNER_ERRORS = "%s::InnerErrors() const" % _TIMPL

_MAX_ERROR_DEPTH = 5

_accessor_cache = {}


def _strip_ref_cv(ty):
    if ty.code == gdb.TYPE_CODE_REF:
        ty = ty.target()
    return ty.unqualified().strip_typedefs()


def _accessor_offset(method):
    """Read the `this + offset` displacement a TImpl accessor compiles to."""
    try:
        asm = gdb.execute("disassemble '%s'" % method, to_string=True)
    except gdb.error:
        return None
    # The displacement shows up as an add/lea/mov off `this`; a bare dereference
    # with no displacement means offset 0 (e.g. GetCode reads the first member).
    for pat in (r"add\s+\$0x([0-9a-f]+),%rax",
                r"lea\s+0x([0-9a-f]+)\(%rdi",
                r"mov\s+0x([0-9a-f]+)\(%rdi"):
        m = re.search(pat, asm)
        if m:
            return int(m.group(1), 16)
    if re.search(r"\(%r(?:di|ax)\)", asm):
        return 0
    return None


def _resolve_accessor(method):
    """(field_offset, field_type) for a TImpl const accessor, or (None, None).
    The offset comes from its disassembly, the type from its return type."""
    if method in _accessor_cache:
        return _accessor_cache[method]
    offset = _accessor_offset(method)
    try:
        ftype = _strip_ref_cv(gdb.parse_and_eval("'%s'" % method).type.target())
    except (gdb.error, AttributeError):
        ftype = None
    _accessor_cache[method] = (offset, ftype)
    return _accessor_cache[method]


def _field_at(addr, ftype):
    return gdb.Value(addr).cast(ftype.pointer()).dereference()


def _impl_addr(error_val):
    """Address of the TImpl behind a TError's unique_ptr, or 0 (the OK error)."""
    uptr = error_val["Impl_"]
    for accessor in (lambda u: u["__ptr_"],                        # libc++
                     lambda u: u["_M_t"]["_M_t"]["_M_head_impl"]):  # libstdc++
        try:
            return int(accessor(uptr))
        except gdb.error:
            continue
    # Empty-deleter unique_ptr: its first word is the stored pointer.
    try:
        void_pp = gdb.lookup_type("void").pointer().pointer()
        return int(uptr.address.cast(void_pp).dereference())
    except gdb.error:
        return 0


def _err_code(impl):
    off, ftype = _resolve_accessor(_GET_CODE)
    if off is None:
        return "?"
    try:
        if ftype is not None and ftype.code == gdb.TYPE_CODE_STRUCT:
            return int(_field_at(impl + off, ftype)["Value_"])  # TErrorCode wraps an int
        raw = bytes(gdb.selected_inferior().read_memory(impl + off, 4))
        return int.from_bytes(raw, "little", signed=True)
    except (gdb.error, KeyError):
        return "?"


def _err_message(impl):
    off, ftype = _resolve_accessor(_GET_MESSAGE)
    if off is None or ftype is None:
        return ""
    try:
        return _unquote(str(_field_at(impl + off, ftype)))
    except gdb.error:
        return ""


def _format_instant(microseconds):
    """A TInstant's microseconds-since-epoch as an ISO-8601 UTC string."""
    sec, usec = divmod(int(microseconds), 1_000_000)
    dt = datetime.datetime.fromtimestamp(sec, datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".%06dZ" % usec


def _err_origin(impl):
    off, ftype = _resolve_accessor(_ORIGIN_ATTRS)
    if off is None or ftype is None:
        return None
    try:
        o = _field_at(impl + off, ftype)
        pid = int(o["Pid"])
        if pid == 0:
            return None  # origin attributes not captured (e.g. an OK-derived error)
        parts = []
        host = _string_view_text(o["Host"])
        if host:
            parts.append("host=%s" % host)
        parts.append("pid=%d" % pid)
        parts.append("tid=0x%08x" % int(o["Tid"]))
        tn = o["ThreadName"]
        tlen = int(tn["Length"])
        if tlen > 0:
            parts.append('thread="%s"' % _array_chars(tn["Buffer"], tlen))
        try:
            usec = int(o["Datetime"]["Value_"])
            if usec:
                parts.append("at %s" % _format_instant(usec))
        except (gdb.error, ValueError, OverflowError):
            pass
        return " ".join(parts)
    except gdb.error:
        return None


def _err_attributes(impl):
    off, ftype = _resolve_accessor(_ATTRIBUTES)
    if off is None or ftype is None:
        return []
    try:
        attrs = _field_at(impl + off, ftype)
        return [(_unquote(str(k)), _unquote(str(v)))
                for k, v in _ordered_hash_map_items(attrs["Map_"])]
    except gdb.error:
        return []


def _err_inner(impl):
    off, ftype = _resolve_accessor(_INNER_ERRORS)
    if off is None or ftype is None:
        return []
    try:
        return _vector_items(_field_at(impl + off, ftype))
    except gdb.error:
        return []


def _format_error(error_val, indent="", depth=0):
    impl = _impl_addr(error_val)
    if not impl:
        return indent + "TError(OK)"
    head = "%sTError code=%s" % (indent, _err_code(impl))
    msg = _err_message(impl)
    if msg:
        head += ' "%s"' % msg
    lines = [head]
    inner_indent = indent + "    "
    origin = _err_origin(impl)
    if origin:
        lines.append("%sorigin: %s" % (inner_indent, origin))
    attrs = _err_attributes(impl)
    if attrs:
        lines.append("%sattributes: {%s}" % (
            inner_indent, "; ".join("%s=%s" % (k, v) for k, v in attrs)))
    inner = _err_inner(impl)
    if inner and depth < _MAX_ERROR_DEPTH:
        lines.append("%sinner errors:" % inner_indent)
        for child in inner:
            lines.append(_format_error(child, inner_indent + "    ", depth + 1))
    elif inner:
        lines.append("%s(+%d inner error(s), depth limit reached)" % (inner_indent, len(inner)))
    return "\n".join(lines)


class TErrorPrinter:
    """A null Impl_ is the OK error; otherwise a multi-line tree of code, message,
    origin (host/pid/tid/thread), user attributes, and nested inner errors.
    TImpl is an incomplete type, so its fields are reached via the disassembled-
    offset trick (see above)."""

    def __init__(self, val):
        self.val = val

    def to_string(self):
        return _format_error(self.val)


# ---------------------------------------------------------------------------
# NYT::NYson::TYsonString / TYsonStringBuf.
# ---------------------------------------------------------------------------

_YSON_TYPES = {0: "Node", 1: "ListFragment", 2: "MapFragment"}


class TYsonStringPrinter:
    """The text is a (Begin_, Size_) view; Type_ tags node vs list/map fragment."""

    def __init__(self, val):
        self.val = val

    def to_string(self):
        begin = self.val["Begin_"]
        if int(begin) == 0:
            return "TYsonString(null)"
        size = int(self.val["Size_"])
        try:
            ty = int(self.val["Type_"].cast(gdb.lookup_type("int")))
        except gdb.error:
            ty = 0
        kind = _YSON_TYPES.get(ty, str(ty))
        text = begin.string(length=size, errors="backslashreplace")
        return "TYsonString[%s] %s" % (kind, text)


# ---------------------------------------------------------------------------
# Registration.
# ---------------------------------------------------------------------------

def _build_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter("yt")
    pp.add_printer("TUnversionedValue", r"^NYT::NTableClient::TUnversionedValue$", TUnversionedValuePrinter)
    pp.add_printer("TVersionedValue", r"^NYT::NTableClient::TVersionedValue$", TVersionedValuePrinter)
    pp.add_printer("TUnversionedRow", r"^NYT::NTableClient::TUnversionedRow$", TUnversionedRowPrinter)
    pp.add_printer("TUnversionedOwningRow", r"^NYT::NTableClient::TUnversionedOwningRow$", TUnversionedOwningRowPrinter)
    pp.add_printer("TVersionedRow", r"^NYT::NTableClient::TVersionedRow$", TVersionedRowPrinter)
    pp.add_printer("TKeyBound", r"^NYT::NTableClient::T(Owning)?KeyBound$", TKeyBoundPrinter)
    pp.add_printer("TGuid", r"^NYT::TGuid$", TGuidPrinter)
    pp.add_printer("TError", r"^NYT::TError(Or<void>)?$", TErrorPrinter)
    pp.add_printer("TYsonString", r"^NYT::NYson::TYsonString(Buf)?$", TYsonStringPrinter)
    pp.add_printer("TOrderedHashMap", r"^NYT::TOrderedHashMap<", TOrderedHashMapPrinter)
    return pp


class YtPrint(gdb.Command):
    """yt-print <expr>: print any YT value (row, error, yson, map, ...) in full --
    pretty-printing on, output limits lifted. Like `p` but never elided."""

    def __init__(self):
        super().__init__("yt-print", gdb.COMMAND_DATA)

    def invoke(self, arg, from_tty):
        if not arg.strip():
            raise gdb.GdbError("expected an expression")
        val = gdb.parse_and_eval(arg)
        print(val.format_string(
            raw=False, pretty_structs=True, pretty_arrays=True, max_elements=0))


def register():
    gdb.printing.register_pretty_printer(None, _build_printer(), replace=True)
    YtPrint()
    _announce.command("yt-print")
    _announce.note("pretty-printers: table-client rows/values, TGuid, TError, "
                   "TYsonString, TOrderedHashMap")


register()
