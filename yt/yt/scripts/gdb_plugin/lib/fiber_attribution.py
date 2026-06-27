# Off-heap (parked-fiber-stack) holder attribution.
#
# A retention ROOT with "no live strong holder" almost always means the object
# is pinned from a place the heap scan can't classify -- most often a *parked
# fiber's stack* (an intrusive ptr living as a local on a suspended coroutine,
# e.g. an in-flight RPC's IServiceContextPtr). The stack slot IS found by the
# heap sweep (fiber stacks are writable LOAD segments), but it has no enclosing
# vtable, so it reads as a raw stack hit.
#
# This module enumerates registered-but-waiting fibers via TFiberRegistry, reads
# each fiber's saved registers from TContMachineContext, maps a stack slot to
# the owning fiber by the writable segment that contains its saved rsp, and
# formats its backtrace (via the fiber primitives in fiber.py).

import re
import struct

import gdb

from sections import sections
from memory import find_pointers_to, read_block, read_ptr, info_symbol
from sections import in_ranges

import fiber


def _registry_object_addr():
    """Address of the in-place TFiberRegistry singleton (a LeakySingleton whose
    Storage holds the object itself). Read from the minimal symbol table, since
    it is a non-debug symbol parse_and_eval can't name."""
    try:
        out = gdb.execute(
            "info variables LeakySingleton<NYT::NConcurrency::TFiberRegistry>",
            to_string=True)
    except gdb.error:
        return None
    for line in out.splitlines():
        if "Storage" in line and "guard" not in line:
            m = re.search(r"(0x[0-9a-f]+)", line)
            if m:
                return int(m.group(1), 16)
    return None


def _enumerate_fibers():
    """All registered fibers (gdb.Values), core-friendly: walk the registry's
    intrusive list directly from memory rather than calling TFiberRegistry::Get()
    (an inferior call, impossible on a coredump)."""
    saddr = _registry_object_addr()
    if saddr is None:
        return []
    try:
        fibers = gdb.parse_and_eval(
            "((NYT::NConcurrency::TFiberRegistry*)0x%x)->Fibers_" % saddr)
    except gdb.error:
        return []
    addrs = []
    try:
        if fiber.is_util_intrusive_list(fibers):
            fiber.parse_util_list(addrs, fibers)
        elif fiber.is_intrusive_list(fibers):
            fiber.parse_intrusive_list(addrs, fibers)
        else:
            fiber.parse_vector(addrs, fibers)
    except gdb.error:
        return []
    out = []
    for a in addrs:
        fib = fiber.get_fiber_from_address(a)
        if fib is not None:
            out.append(fib)
    return out


_fiber_stacks_cache = None


_thread_stacks_cache = None


def thread_stacks():
    """[(thread, rsp, segLo, segHi)] for live threads whose rsp lies in a known
    writable segment. A thread's live stack is [rsp, segHi). Covers plain threads
    and *active* fibers (an active fiber runs on a thread, so its live context is
    the thread's, not its stale saved TContMachineContext)."""
    global _thread_stacks_cache
    if _thread_stacks_cache is not None:
        return _thread_stacks_cache
    zones = sections().heap_zones()
    out = []
    try:
        inferior = gdb.selected_inferior()
    except Exception:
        _thread_stacks_cache = []
        return _thread_stacks_cache
    saved = None
    try:
        saved = gdb.selected_thread()
    except Exception:
        pass
    try:
        for t in inferior.threads():
            try:
                t.switch()
                rsp = int(gdb.parse_and_eval("(unsigned long)$rsp"))
            except Exception:
                continue
            for lo, hi in zones:
                if lo <= rsp < hi:
                    out.append((t, rsp, lo, hi))
                    break
    finally:
        if saved is not None:
            try:
                saved.switch()
            except Exception:
                pass
    _thread_stacks_cache = out
    return out


def _active_segments():
    """Stack segments occupied by a live thread -- i.e. running (incl. active
    fibers). Parked-fiber attribution skips these (their saved context is stale)."""
    return {(lo, hi) for _t, _rsp, lo, hi in thread_stacks()}


def find_threads_referencing(target):
    """Live threads whose stack holds a pointer to #target. Returns
    [(thread, slot)] (deduped by thread, lowest slot kept)."""
    stacks = thread_stacks()
    if not stacks:
        return []
    best = {}
    for slot in find_pointers_to(target):
        for t, rsp, lo, hi in stacks:
            if rsp <= slot < hi:
                if t.num not in best or slot < best[t.num][1]:
                    best[t.num] = (t, slot)
                break
    return [v for _num, v in sorted(best.items())]


def fiber_stacks():
    """[(segLo, segHi, rsp, index, fiber)] for *parked* fibers, or []. Fibers
    whose stack belongs to a live thread (active fibers) are excluded -- their
    saved TContMachineContext is stale; the thread pass handles them."""
    global _fiber_stacks_cache
    if _fiber_stacks_cache is not None:
        return _fiber_stacks_cache
    try:
        fibers = _enumerate_fibers()
    except Exception as e:
        print("Could not enumerate fibers: %s" % e)
        _fiber_stacks_cache = []
        return _fiber_stacks_cache
    zones = sections().heap_zones()
    active = _active_segments()
    out = []
    for i, fib in enumerate(fibers):
        try:
            rsp = int(fiber.retrieve_fiber_context_regs(fib)[fiber.MJB_RSP])
        except Exception:
            continue
        for lo, hi in zones:
            if lo <= rsp < hi:
                if (lo, hi) not in active:
                    out.append((lo, hi, rsp, i, fib))
                break
    _fiber_stacks_cache = out
    return out


def find_fibers_referencing(target):
    """Parked fibers whose live stack region holds a pointer to #target.
    Returns [(index, fiber, slot)] (deduped by fiber, lowest slot kept)."""
    stacks = fiber_stacks()
    if not stacks:
        return []
    best = {}
    for slot in find_pointers_to(target):
        for lo, hi, rsp, i, fib in stacks:
            # Live stack data is [rsp, segEnd) -- the slot must be at or above the
            # saved stack pointer to be a real local, not stale red-zone garbage.
            if lo <= slot < hi and slot >= rsp:
                if i not in best or slot < best[i][1]:
                    best[i] = (fib, slot)
                break
    return [(i, fib, slot) for i, (fib, slot) in sorted(best.items())]


def format_thread_backtrace(thread, limit=48):
    """gdb's native backtrace for a live thread (it unwinds core threads
    directly). Returns the lines."""
    saved = None
    try:
        saved = gdb.selected_thread()
    except Exception:
        pass
    try:
        thread.switch()
        bt = gdb.execute("backtrace %d" % limit, to_string=True)
    except Exception:
        bt = ""
    finally:
        if saved is not None:
            try:
                saved.switch()
            except Exception:
                pass
    return bt.splitlines()


_MASK64 = (1 << 64) - 1


def _seed_regs_for(fib):
    """The fiber's saved registers (rbp, rsp, rip, ...) from its
    TContMachineContext, as a name->value dict."""
    buf = fiber.retrieve_fiber_context_regs(fib)
    return {name: int(buf[idx]) & _MASK64 for name, idx in fiber._FIBER_SEED_REGS}


def format_fiber_backtrace(fib, limit=48):
    """Return #fib's backtrace as a list of lines, from the fiber's saved
    registers (TContMachineContext). Read-only. Two strategies:
      1. rbp frame-pointer walk -- exact; used whenever frame pointers are kept
         (no-omit-fp production and debug builds).
      2. approximate stack scan -- for frame-pointer-omitting builds, where a
         precise unwind isn't recoverable from a coredump; carries some stale
         frames but surfaces the real chain."""
    seed = _seed_regs_for(fib)
    lines = _fp_walk(seed.get("rip"), seed.get("rbp"), limit)
    if lines is not None:
        return lines
    return _stack_scan(seed.get("rip"), seed.get("rsp"), limit)


def _fp_walk(rip, rbp, limit):
    """Walk the rbp frame-pointer chain from (rip, rbp), symbolizing each frame.
    Returns the formatted lines, or None if this isn't a valid frame-pointer
    chain (a frame-pointer-omitting build) so the caller can fall back."""
    if rip is None or not rbp:
        return None
    text = sections().named(".text")
    out = []
    seen = set()
    for i in range(limit):
        sym = (info_symbol(rip) or "?").split(" in section ")[0]
        out.append("#%-2d 0x%012x  %s" % (i, rip, sym))
        if not rbp or rbp in seen:
            break
        seen.add(rbp)
        ret = read_ptr(rbp + 8)
        nxt = read_ptr(rbp)
        # A real frame-pointer chain: [rbp+8] is a post-call return address in
        # .text and [rbp] (the saved rbp) moves up the stack. If either fails the
        # build omits frame pointers -- bail so the caller falls back to a scan.
        if ret is None or not in_ranges(ret, text) or not _follows_call(ret):
            break
        if nxt is None or nxt <= rbp:
            break
        rip, rbp = ret, nxt
    if len(out) < 2:  # didn't advance past the seed -> not a frame-pointer chain
        return None
    return out


def _stack_scan(rip, rsp, limit):
    """Frame-pointer-omitting fallback: from the saved (rip, rsp), emit the words
    on the fiber stack that look like return addresses (in .text, right after a
    call). Approximate -- carries stale frames from earlier calls -- but surfaces
    the real chain. A precise unwind isn't recoverable from a coredump without
    frame pointers or CFI through the util/ switch."""
    if rip is None or rsp is None:
        return ["(saved fiber context unavailable)"]
    text = sections().named(".text")
    out = ["[stack scan -- approximate; build omits frame pointers]",
           "#0  0x%012x  %s" % (rip, (info_symbol(rip) or "?").split(" in section ")[0])]
    blk = read_block(rsp, 0x10000) or b""
    n = len(blk) // 8
    vals = struct.unpack_from("<%dQ" % n, blk, 0) if n else ()
    shown = 0
    last = None
    for v in vals:
        if not in_ranges(v, text) or not _follows_call(v):
            continue
        s = (info_symbol(v) or "").split(" in section ")[0]
        if not s or "vtable" in s or s == last:
            continue
        out.append("    0x%012x  %s" % (v, s))
        last = s
        shown += 1
        if shown >= limit:
            break
    return out


def _follows_call(addr):
    """Heuristic: is #addr immediately preceded by a call instruction? Cuts stale
    code pointers from the stack scan. Recognizes E8 rel32 and FF /2 (call r/m)."""
    pre = read_block(addr - 7, 7)
    if not pre or len(pre) < 7:
        return False
    if pre[2] == 0xE8:  # E8 rel32 -> 5-byte call ending at addr
        return True
    for i in range(len(pre) - 1):  # FF /2 (reg field == 2) -> call r/m
        if pre[i] == 0xFF and ((pre[i + 1] >> 3) & 7) == 2:
            return True
    return False
