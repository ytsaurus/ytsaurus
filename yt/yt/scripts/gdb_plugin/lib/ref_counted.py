# Ref-count resolution: StrongCount / WeakCount / liveness of a ref-counted
# object, including the tricky virtually-inherited TRefCountedBase part.

import gdb

from memory import read_ptr, read_s64, gdb_type, QuietStderr
from sections import sections, in_ranges
from type_info import object_typename, wrapper_inner_type
from signature import scan_signature


class RefCount:
    __slots__ = ("obj", "typename", "inner", "base_addr", "strong", "weak", "error", "signature")

    # A plausible StrongCount is small and positive. A huge value almost always
    # means we mis-identified a non-ref-counted blob as an object.
    SANE_MAX = 1 << 24

    def __init__(self, obj):
        self.obj = obj
        self.typename = None
        self.inner = None
        self.base_addr = None
        self.strong = None
        self.weak = None
        self.error = None
        # Signature verdict: 'alive' | 'dead' | 'none' (no signature in this build).
        self.signature = "none"

    @property
    def ok(self):
        return self.strong is not None

    @property
    def alive(self):
        # The self-validating signature is authoritative when present: it proves
        # the bytes are a live counter, independent of any StrongCount heuristic.
        if self.signature == "alive":
            return True
        if self.signature == "dead":
            return False
        return (
            self.strong is not None
            and 0 < self.strong < RefCount.SANE_MAX
            and (self.weak is None or 0 <= self.weak < (1 << 40))
        )


_refcount_cache = {}
# vptr value -> signed offset from object start to its TRefCountedBase subobject.
_base_off_cache = {}


def resolve_refcount(addr):
    cached = _refcount_cache.get(addr)
    if cached is not None:
        return cached
    rc = _resolve_refcount_uncached(addr)
    _refcount_cache[addr] = rc
    return rc


def _resolve_refcount_uncached(addr):
    """Resolve StrongCount/WeakCount for a ref-counted object at addr.

    YT's ref counter lives in the virtually-inherited TRefCountedBase subobject,
    not at offset 0. We must cast THROUGH the wrapper type so gdb applies the
    virtual-base offset; a naive (TRefCountedBase*)intptr cast does NOT adjust it.
    """
    rc = RefCount(addr)
    vptr = read_ptr(addr)
    # Normalize an interior pointer to the most-derived object start. A pointer to
    # a secondary- or virtual-base subobject (e.g. a TRefCountedPtr to a diamond
    # whose TRefCounted is a shared virtual base) has its own vptr carrying a
    # non-zero offset-to-top; the vbase-offset cast below is only valid from the
    # real object head.
    if vptr is not None and in_ranges(vptr, sections().code_relro_ranges()):
        off_to_top = read_s64(vptr - 16)
        if off_to_top:
            addr = (addr + off_to_top) & ((1 << 64) - 1)
            rc.obj = addr
            vptr = read_ptr(addr)
    typename, _sym = object_typename(addr)
    rc.typename = typename
    if typename is None:
        # No usable vptr. This is the New<T> "final type" layout: for a final,
        # non-polymorphic T, New lays a TRefCounter immediately *before* the
        # object (no vtable to name the type). The self-validating signature
        # still pins the counter, so we can report counts and liveness.
        status, sig_addr, strong, weak = scan_signature(addr - 0x28, addr + 0x8)
        if status is not None:
            rc.typename = "(final/non-polymorphic; no vtable)"
            rc.signature = status
            rc.base_addr = sig_addr
            rc.strong = strong if status == "alive" else 0
            rc.weak = weak if status == "alive" else 0
            return rc
        rc.error = "no vtable / not a YT ref-counted object"
        return rc

    inner = wrapper_inner_type(typename)
    rc.inner = inner

    # The offset (object_start -> TRefCountedBase subobject) is constant for a
    # given concrete type, uniquely identified by its primary vptr value. Cache
    # it so we pay the slow templated cast only once per type, then resolve every
    # other instance with pure pointer arithmetic.
    base_off = _base_off_cache.get(vptr) if vptr is not None else None
    if base_off is not None:
        base_addr = (addr + base_off) & ((1 << 64) - 1)
    else:
        base_addr = None
        # Strategy 1: cast through TRefCountedWrapper<INNER>* -> TRefCountedBase*.
        if inner is not None:
            for wrapper in (
                "NYT::TRefCountedWrapper<%s>" % inner,
                "NYT::TRefCountedWrapperWithCookie<%s>" % inner,
            ):
                base_addr = _try_cast_to_base(addr, wrapper)
                if base_addr is not None:
                    break
        # Strategy 2: cast directly through the dynamic type (covers non-wrapper
        # ref-counted types, e.g. classes deriving TRefCounted directly).
        if base_addr is None and typename is not None:
            base_addr = _try_cast_to_base(addr, typename)
        if base_addr is not None and vptr is not None:
            _base_off_cache[vptr] = base_addr - addr  # signed offset

    if base_addr is None:
        # Casts failed (ambiguous/virtual base). The self-validating signature
        # can still locate the counter from raw bytes: scan around the object
        # start (covers the counter-before-object layout at addr-sizeof(counter)
        # and the derived layout just past the vptr).
        status, sig_addr, strong, weak = scan_signature(addr - 0x20, addr + 0x40)
        if status is None:
            rc.error = "could not resolve TRefCountedBase subobject (ambiguous/virtual-base botch)"
            return rc
        rc.signature = status
        rc.base_addr = sig_addr
        rc.strong = strong if status == "alive" else 0
        rc.weak = weak if status == "alive" else 0
        return rc

    rc.base_addr = base_addr
    # Prefer the self-validating signature: it pinpoints the counter head (robust
    # to the +8 shift the signature itself adds ahead of StrongCount) and
    # distinguishes a live object from freed-but-unreclaimed memory.
    status, sig_addr, strong, weak = scan_signature(base_addr - 0x8, base_addr + 0x20)
    if status == "alive":
        rc.signature = "alive"
        rc.base_addr = sig_addr
        rc.strong = strong
        rc.weak = weak
        return rc
    if status == "dead":
        rc.signature = "dead"
        rc.base_addr = sig_addr
        rc.strong = 0
        rc.weak = 0
        return rc

    # No matching signature. Canonical layout is base+0 = vptr, base+8 =
    # StrongCount, base+16 = WeakCount. But a build with a different/older
    # signature scheme (e.g. the earlier typeinfo-salted one) inserts a word
    # before StrongCount, shifting the counts by +8. Pick whichever offset yields
    # a sane (StrongCount, WeakCount) pair.
    rc.signature = "none"
    for off in (8, 16):
        strong = read_s64(base_addr + off)
        weak = read_s64(base_addr + off + 8)
        if (strong is not None and 0 < strong < RefCount.SANE_MAX and
                weak is not None and 0 <= weak < (1 << 40)):
            rc.strong = strong
            rc.weak = weak
            return rc
    rc.strong = read_s64(base_addr + 8)
    rc.weak = read_s64(base_addr + 16)
    return rc


def _try_cast_to_base(addr, type_expr):
    """Cast int addr -> (TYPE*) -> (TRefCountedBase*), returning the adjusted
    address, or None on any gdb error (ambiguous base, unknown type, ...)."""
    expr = "(void*)(NYT::TRefCountedBase*)(%s*)0x%x" % (type_expr, addr)
    try:
        with QuietStderr():
            v = gdb.parse_and_eval(expr)
        return int(v.cast(gdb_type("unsigned long")))
    except gdb.error:
        return None
