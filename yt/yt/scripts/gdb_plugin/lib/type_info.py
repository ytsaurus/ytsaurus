# Type identification from vtables, and container resolution.
#
# Given a vptr field value, recover the dynamic type name; given an arbitrary
# pointer slot, walk back to the enclosing object's primary start and type.

import re

import tcmalloc
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


# A BIND closure invoke thunk: "auto NYT::NDetail::TBindState<...>::Run<...>(...)".
_CLOSURE_KEY = "NYT::NDetail::TBindState<"
# A closure's Run thunk sits near the start of its allocation; cap the in-block
# scan so a large vtable-less buffer isn't walked word by word.
_CLOSURE_SCAN_CAP = 0x400


def closure_type_from_symbol(sym):
    """If #sym is a TBindState<...>::Run invoke thunk, return the
    'NYT::NDetail::TBindState<...>' type (balanced angle brackets), else None."""
    if not sym:
        return None
    i = sym.find(_CLOSURE_KEY)
    if i < 0:
        return None
    depth = 0
    for p in range(i + len(_CLOSURE_KEY) - 1, len(sym)):
        c = sym[p]
        if c == "<":
            depth += 1
        elif c == ">":
            depth -= 1
            if depth == 0:
                # Must be the invoke thunk (…>::Run…), not an incidental mention.
                return sym[i:p + 1] if sym[p + 1:].startswith("::Run") else None
    return None


def _closure_in_range(lo, hi):
    """The TBindState type of a Run thunk found among the words in [lo, hi), or
    None. Used to name a vtable-less closure given its exact allocation bounds."""
    relro = sections().code_relro_ranges()
    a = lo & ~0x7
    hi = min(hi, lo + _CLOSURE_SCAN_CAP)
    while a < hi:
        val = read_ptr(a)
        if val is not None and in_ranges(val, relro):
            ctype = closure_type_from_symbol(info_symbol(val))
            if ctype is not None:
                return ctype
        a += 8
    return None


def find_enclosing_object(slot_addr, max_scan=0x4000):
    """Resolve the heap object/allocation a pointer slot lives in. Returns
    (primary_addr, typename, marker_slot, kind):
      - kind 'object': a vtable-backed object (primary is its real start);
      - kind 'closure': a vtable-less TBindState closure, named from a Run thunk;
      - (None, None, None, None) when nothing nameable is found.

    With tcmalloc we snap the slot to its exact allocation start (read from the
    page map) and read the vtable there -- precise, and it lets the retention
    trace recurse on a real base. Without tcmalloc we fall back to a backward
    scan for the nearest preceding vptr."""
    blk = tcmalloc.allocation_of(slot_addr)
    if blk is not None:
        base, size = blk
        ptypename, _psym = object_typename(base)
        if ptypename is not None:
            return base, ptypename, base, "object"
        # A real allocation with no vtable at its start -- a closure or POD. Look
        # for a Run thunk inside the *exact* allocation (no guessing); else it is
        # an anonymous block we can bound but not name.
        ctype = _closure_in_range(base, base + size)
        if ctype is not None:
            return base, ctype, base, "closure"
        return None, None, None, None
    return _find_enclosing_by_walk(slot_addr, max_scan)


def _find_enclosing_by_walk(slot_addr, max_scan=0x4000):
    """Best-effort fallback when tcmalloc isn't available: walk backward to the
    nearest preceding vptr and take its object as the container. Approximate -- it
    can over-reach into an unrelated earlier object -- which is exactly why the
    tcmalloc page-map path is preferred whenever it's available."""
    relro = sections().code_relro_ranges()
    for a, val in iter_ptrs_back(slot_addr, max_scan):
        if not in_ranges(val, relro):
            continue
        _sym, typename, _is_construction = vtable_symbol(val)
        if typename is None:
            continue
        # offset-to-top is the signed i64 at (vtable_target - 16).
        off_to_top = read_s64(val - 16) or 0
        primary = a + off_to_top
        # Re-identify from the primary vptr (offset-to-top of 0 there).
        ptypename, _psym = object_typename(primary)
        name = ptypename or typename  # fall back to the secondary-vtable name
        return primary, name, a, "object"
    return None, None, None, None
