# Low-level coredump memory access.
#
# Bulk reads, the one-time heap snapshot that makes repeated pointer searches
# fast, and the pointer-to-address search itself. Read-only.

import bisect
import os
import struct

import gdb

from sections import sections

_type_cache = {}


def gdb_type(name):
    """Cached gdb.lookup_type. Lazy so the module can be sourced before a binary
    is loaded (e.g. via `gdb -ix`)."""
    t = _type_cache.get(name)
    if t is None:
        t = gdb.lookup_type(name)
        _type_cache[name] = t
    return t


# Cached snapshot of the writable heap: list of (base, bytes), read once and
# reused for every lookup. This is what makes the walker practical on multi-GB
# cores: gdb's `find /g` rescans the whole heap per call (~minutes on a 1.8 GB
# core), whereas here we pay one bulk read and answer each lookup with a C-level
# bytes.find (memmem) over the cached blocks (sub-second).
_heap_blocks = None
_heap_bases = None  # ascending base addresses, parallel to _heap_blocks (for bisect)


def read_block(addr, nbytes):
    """Bulk-read nbytes at addr -> bytes, or None if inaccessible.

    Served from the cached heap snapshot when the range is fully inside it (no
    gdb round-trip); otherwise falls back to gdb."""
    blocks = _heap_blocks
    if blocks:
        i = bisect.bisect_right(_heap_bases, addr) - 1
        if 0 <= i < len(blocks):
            base, blk = blocks[i]
            off = addr - base
            if off >= 0 and off + nbytes <= len(blk):
                return blk[off:off + nbytes]
    try:
        return bytes(gdb.selected_inferior().read_memory(addr, nbytes))
    except (gdb.error, OverflowError, ValueError):
        return None


def read_ptr(addr):
    """Read an 8-byte pointer-sized value at addr; None if inaccessible."""
    b = read_block(addr, 8)
    if b is None or len(b) < 8:
        return None
    return struct.unpack_from("<Q", b)[0]


def read_s64(addr):
    b = read_block(addr, 8)
    if b is None or len(b) < 8:
        return None
    return struct.unpack_from("<q", b)[0]


def iter_ptrs_back(end_addr, span):
    """Yield (addr, value) for 8-byte words in [end_addr-span, end_addr],
    high to low, using one bulk read. Inaccessible -> stops."""
    start = (end_addr - span) & ~0x7
    n = (end_addr - start) // 8 + 1
    blk = read_block(start, n * 8)
    if blk is None:
        for s in (span // 2, span // 4, 0x400):  # back off to a shorter window
            start2 = (end_addr - s) & ~0x7
            n2 = (end_addr - start2) // 8 + 1
            blk = read_block(start2, n2 * 8)
            if blk is not None:
                start, n = start2, n2
                break
        if blk is None:
            return
    vals = struct.unpack_from("<%dQ" % n, blk, 0)
    for i in range(n - 1, -1, -1):
        yield start + i * 8, vals[i]


class QuietStderr:
    """Redirect fd 2 to /dev/null for the duration. Suppresses gdb's C-level
    warnings (e.g. "found construction vtable instead") that `to_string=True`
    does not capture. The tool's own output (fd 1) is unaffected."""

    def __enter__(self):
        self._saved = os.dup(2)
        self._null = os.open(os.devnull, os.O_WRONLY)
        os.dup2(self._null, 2)
        return self

    def __exit__(self, *exc):
        os.dup2(self._saved, 2)
        os.close(self._null)
        os.close(self._saved)
        return False


def info_symbol(addr):
    """`info symbol addr` -> symbol string, or '' if none."""
    try:
        with QuietStderr():
            out = gdb.execute("info symbol 0x%x" % addr, to_string=True).strip()
    except gdb.error:
        return ""
    if not out or out.startswith("No symbol matches"):
        return ""
    return out


def heap_blocks():
    """The cached heap snapshot, built once on first use."""
    global _heap_blocks, _heap_bases
    if _heap_blocks is not None:
        return _heap_blocks
    blocks = []
    total = 0
    for lo, hi in sections().heap_zones():
        a = lo
        while a < hi:
            n = min(256 << 20, hi - a)
            blk = read_block(a, n)
            if blk is None:  # back off on unreadable spans
                n2 = n
                while n2 >= (1 << 16) and blk is None:
                    n2 //= 2
                    blk = read_block(a, n2)
                if blk is None:
                    a += (1 << 16)
                    continue
            blocks.append((a, blk))
            total += len(blk)
            a += len(blk)
    print("Heap snapshot: %.1f MB in %d block(s) (one-time)" % (total / 1e6, len(blocks)))
    blocks.sort(key=lambda b: b[0])
    _heap_blocks = blocks
    _heap_bases = [b[0] for b in blocks]
    return blocks


_find_cache = {}


# x86-64 pointers use the low 48 bits; the top 16 are free and are where YT's
# tagged/packed pointers stash a tag (e.g. TAtomicIntrusivePtr packs a local
# refcount there -- "grabs refs in 64K batches"). So we match a holder by the
# low 48 bits of the target only: that catches a plain TIntrusivePtr (tag 0) and
# a TAtomicIntrusivePtr / any TTaggedPtr holder alike. (Heap addresses have a
# zero top 16, so the low 6 bytes ARE the address.)
_PACKED_PTR_ADDRESS_BYTES = 6


def find_pointers_to(target):
    """All 8-aligned addresses in the heap whose 8-byte value points to #target,
    ignoring any tag packed into the top 16 bits.

    Uses the cached heap snapshot + bytes.find, so repeated lookups (as in
    retention tracing) don't each rescan the whole core."""
    if target in _find_cache:
        return _find_cache[target]
    # The low 48 bits (6 little-endian bytes) of the target address. At an
    # 8-aligned slot these are the word's low bytes; the trailing 2 bytes (the
    # tag) may be anything.
    needle = struct.pack("<Q", target & ((1 << 64) - 1))[:_PACKED_PTR_ADDRESS_BYTES]
    hits = []
    for base, blk in heap_blocks():
        pos = blk.find(needle)
        while pos != -1:
            if (base + pos) & 7 == 0:  # real pointer slots are 8-aligned
                hits.append(base + pos)
            pos = blk.find(needle, pos + 1)
    _find_cache[target] = hits
    return hits


_M48 = (1 << 48) - 1
_range_find_cache = {}


def find_pointers_into(target, size):
    """All 8-aligned heap slots whose value (low 48 bits) points anywhere into
    [target, target+size). Returns [(slot, sub_offset)] where sub_offset is the
    pointed-to byte offset within the object (0 == the object base).

    This catches a holder of a *secondary base subobject* (multiple inheritance):
    a TIntrusivePtr<ISomeInterface> stores a pointer to the interface subobject at
    a non-zero offset, which a plain base-address search misses.

    One heap pass, keyed on the 4 address bytes that are stable across the object
    (bits 16..47): since a single object spans well under 64 KiB, every sub-object
    address shares those bytes (modulo a rare 64 KiB-boundary carry, handled by
    searching both endpoints' keys). Candidates are then verified against the full
    range, so tagged pointers (tag in the top 16 bits) match too."""
    if size <= 8:
        return [(s, 0) for s in find_pointers_to(target)]
    ckey = (target, size)
    if ckey in _range_find_cache:
        return _range_find_cache[ckey]
    lo = target & ((1 << 64) - 1)
    hi = lo + size
    # The stable middle bytes (address bits 16..47) at word offset 2; usually one
    # value, two if [lo, hi) crosses a 64 KiB boundary.
    keys = {struct.pack("<Q", a)[2:_PACKED_PTR_ADDRESS_BYTES] for a in (lo, hi - 8)}
    out = []
    seen = set()
    for base, blk in heap_blocks():
        blen = len(blk)
        for key in keys:
            pos = blk.find(key)
            while pos != -1:
                slot = base + pos - 2  # the word whose bytes[2:6] == key
                soff = slot - base
                if slot & 7 == 0 and soff >= 0 and soff + 8 <= blen and slot not in seen:
                    v = struct.unpack_from("<Q", blk, soff)[0] & _M48
                    if lo <= v < hi:
                        seen.add(slot)
                        out.append((slot, v - lo))
                pos = blk.find(key, pos + 1)
    out.sort()
    _range_find_cache[ckey] = out
    return out
