# Ref-counted signature (see library/cpp/yt/memory/ref_counted.h).
#
# When built with YT_ENABLE_REF_COUNTED_SIGNATURE every TRefCounter begins with
# a self-validating signature word:
#
#     signature   <- counter+0 : RefCountedAliveSignatureMagic ^ &signature,
#                                 overwritten with RefCountedDeadSignatureMagic at death
#     StrongCount <- counter+8
#     WeakCount   <- counter+16
#
# The salt is the signature word's own address, so the marker cannot be forged
# by a verbatim copy elsewhere. It lets us tell a live object from freed-but-
# unreclaimed memory and pinpoint the counter without trusting StrongCount
# ranges or fragile virtual-base casts.

import struct

from sections import sections
from memory import read_block, read_ptr, read_s64
from type_info import vtable_symbol, object_typename
from sections import in_ranges

M64 = (1 << 64) - 1
REFCOUNTED_ALIVE_MAGIC = 0xA11E0B1EC70A11E0
REFCOUNTED_DEAD_MAGIC = 0xDEADBEEFDEADBEEF


def alive_signature(addr):
    """The alive magic a valid signature word living at #addr must hold: the
    magic XOR-ed with the word's own address (matches
    ComputeRefCountedAliveSignature in ref_counted.h)."""
    return (REFCOUNTED_ALIVE_MAGIC ^ addr) & M64


def scan_signature(lo, hi):
    """Scan aligned words in [lo, hi) for a ref-counted signature.

    Returns (status, sig_addr, strong, weak):
      'alive' -> a word validating against its own address; counts follow it
      'dead'  -> the dead poison
      None    -> nothing found (non-signature build, or not a counter here)

    Scanning low->high lands on the counter head, since the signature precedes
    the StrongCount/WeakCount words.
    """
    lo &= ~0x7
    if hi <= lo:
        return (None, None, None, None)
    blk = read_block(lo, hi - lo)
    if blk is None:
        return (None, None, None, None)
    n = len(blk) // 8
    if n == 0:
        return (None, None, None, None)
    vals = struct.unpack_from("<%dQ" % n, blk, 0)
    for i in range(n):
        w = vals[i]
        if w == REFCOUNTED_DEAD_MAGIC:
            return ("dead", lo + i * 8, None, None)
        if w == alive_signature(lo + i * 8):
            s = lo + i * 8
            strong = vals[i + 1] if i + 1 < n else read_s64(s + 8)
            weak = vals[i + 2] if i + 2 < n else read_s64(s + 16)
            return ("alive", s, strong, weak)
    return (None, None, None, None)


def _object_from_counter(sig_addr):
    """Given a live signature (counter head), find the enclosing object's primary
    start and type. Two layouts: derived (a TRefCountedBase vptr precedes the
    counter at sig-8) and counter-before-object (the New<T> final-type layout:
    object follows the counter, at sig + RefCounterSpace, 24 or padded up)."""
    relro = sections().code_relro_ranges()
    # Derived layout: the counter is preceded at sig-8 by its TRefCountedBase
    # vptr. Under multiple/virtual inheritance that is a SECONDARY base vtable, so
    # apply its offset-to-top to reach the most-derived object start and resolve
    # the real type there -- otherwise such objects (e.g. a hydra manager whose
    # TRefCounter is a virtual base) resolve to a base vtable and get filtered out.
    vptr = read_ptr(sig_addr - 8)
    if vptr is not None and in_ranges(vptr, relro):
        off_to_top = read_s64(vptr - 16) or 0
        primary = (sig_addr - 8 + off_to_top) & M64
        ptn, _psym = object_typename(primary)
        if ptn is not None:
            return primary, ptn
        _sym, tn, _ctor = vtable_symbol(vptr)
        if tn is not None:
            return sig_addr - 8, tn
    # Counter-before-object (New<T> final-type) layout.
    for cand in (sig_addr + 24, sig_addr + 32):
        v = read_ptr(cand)
        if v is not None and in_ranges(v, relro):
            _sym, tn, _ctor = vtable_symbol(v)
            if tn is not None:
                return cand, tn
    return None, None


def find_live_objects_by_type(substr, limit=64):
    """Signature-based heap sweep: every live (alive-signature) ref-counted
    object whose resolved type name contains #substr -- locate a leaked object by
    name with no symbol gymnastics. Returns [(object_addr, typename, strong)]."""
    out = []
    seen = set()
    CHUNK = 16 * 1024 * 1024
    for lo, hi in sections().heap_zones():
        a = lo
        while a < hi:
            n = min(CHUNK, hi - a)
            blk = read_block(a, n)
            if blk:
                m = len(blk) // 8
                vals = struct.unpack_from("<%dQ" % m, blk, 0)
                for i in range(m):
                    s = a + i * 8
                    if vals[i] != alive_signature(s):
                        continue
                    obj, tn = _object_from_counter(s)
                    if obj is None or tn is None or substr not in tn or obj in seen:
                        continue
                    seen.add(obj)
                    strong = vals[i + 1] if i + 1 < m else read_s64(s + 8)
                    out.append((obj, tn, strong))
                    if len(out) >= limit:
                        return out
            a += n
    return out
