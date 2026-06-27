# Type identification from vtables, and container resolution.
#
# Given a vptr field value, recover the dynamic type name; given an arbitrary
# pointer slot, walk back to the enclosing object's primary start and type.

import re

from sections import sections, in_ranges
from memory import read_ptr, read_s64, info_symbol, iter_ptrs_back

# "vtable for NYT::TRefCountedWrapper<...> + N in section ..."
_VTABLE_RE = re.compile(r"(construction )?vtable for (.+?)(?: \+ \d+)?(?: in section .*)?$")

_vtable_cache = {}


def vtable_symbol(vptr_value):
    """A vptr field value -> (raw_symbol, demangled_typename, is_construction)."""
    cached = _vtable_cache.get(vptr_value)
    if cached is not None:
        return cached
    res = _vtable_symbol_uncached(vptr_value)
    _vtable_cache[vptr_value] = res
    return res


def _vtable_symbol_uncached(vptr_value):
    sym = info_symbol(vptr_value)
    if not sym:
        return sym, None, False
    m = _VTABLE_RE.search(sym)
    if not m:
        return sym, None, False
    is_construction = bool(m.group(1))
    typename = m.group(2)
    # "construction vtable for Base-in-Derived": take the Derived part.
    if is_construction and "-in-" in typename:
        typename = typename.split("-in-", 1)[1]
    return sym, typename, is_construction


def object_typename(addr):
    """Identify an object's dynamic type from the vptr at offset 0."""
    vptr = read_ptr(addr)
    if vptr is None or not in_ranges(vptr, sections().code_relro_ranges()):
        return None, None
    sym, typename, _ = vtable_symbol(vptr)
    return typename, sym


# TRefCountedWrapper<INNER> / TRefCountedWrapperWithCookie<INNER, ...>
_WRAPPER_RE = re.compile(r"NYT::TRefCountedWrapper(WithCookie)?<(.+)>$")


def wrapper_inner_type(typename):
    """If typename is a TRefCountedWrapper<INNER>, return INNER, else None."""
    if typename is None:
        return None
    m = _WRAPPER_RE.match(typename.strip())
    if not m:
        return None
    inner = m.group(2)
    # For WithCookie<INNER, COOKIE> the inner is the first template arg.
    if m.group(1):
        inner = first_template_arg(inner)
    return inner.strip()


def first_template_arg(args):
    """The first top-level (depth-0 comma) template argument."""
    depth = 0
    for i, ch in enumerate(args):
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        elif ch == "," and depth == 0:
            return args[:i]
    return args


def split_template_args(args):
    """Split top-level comma-separated template args."""
    res = []
    depth = 0
    start = 0
    for i, ch in enumerate(args):
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        elif ch == "," and depth == 0:
            res.append(args[start:i].strip())
            start = i + 1
    res.append(args[start:].strip())
    return res


def find_enclosing_object(slot_addr, max_scan=0x4000):
    """Walk backward from slot_addr to the nearest preceding vptr (a value in the
    code/relro range), apply offset-to-top to find the primary object start.
    Returns (primary_addr, typename, vptr_slot_addr) or (None, None, None)."""
    relro = sections().code_relro_ranges()
    for a, val in iter_ptrs_back(slot_addr, max_scan):
        if not in_ranges(val, relro):
            continue
        _sym, typename, _is_construction = vtable_symbol(val)
        if typename is None:
            continue
        # offset-to-top is the signed i64 at (vtable_target - 16).
        off_to_top = read_s64(val - 16)
        if off_to_top is None:
            off_to_top = 0
        primary = a + off_to_top
        # Re-identify from the primary vptr (offset-to-top of 0 there).
        ptypename, _psym = object_typename(primary)
        if ptypename is not None:
            return primary, ptypename, a
        return primary, typename, a  # fall back to the secondary-vtable typename
    return None, None, None
