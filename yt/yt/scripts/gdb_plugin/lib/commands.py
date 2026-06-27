# gdb command bindings and output formatting.
#
#     yt-rc-obj     <addr>     resolve type + StrongCount/WeakCount + liveness
#     yt-rc-backref <addr>     find and classify every live pointer to an object;
#                              also attributes off-heap (parked-fiber) holders
#     yt-rc-cycle   <addr>     follow strong holders; report a retention CYCLE or ROOT
#     yt-rc-find    <type>     signature-based heap sweep for live objects by type

import gdb

from memory import gdb_type
from sections import sections
from ref_counted import resolve_refcount
from holders import analyze_holders, trace_retention
from signature import find_live_objects_by_type
from fiber_attribution import (
    find_fibers_referencing, format_fiber_backtrace,
    find_threads_referencing, format_thread_backtrace,
)


def _parse_addr(arg):
    arg = arg.strip()
    if not arg:
        raise gdb.GdbError("expected an address argument")
    return int(gdb.parse_and_eval(arg).cast(gdb_type("unsigned long")))


def _fmt_rc(rc):
    if rc.error and not rc.ok:
        return "type=%s ERROR: %s" % (rc.typename, rc.error)
    sig = "" if rc.signature == "none" else " sig=%s" % rc.signature
    return "type=%s strong=%s weak=%s base=0x%x%s" % (
        rc.typename, rc.strong, rc.weak, rc.base_addr or 0, sig)


def _short(typename, width=80):
    if typename and len(typename) > width:
        return typename[:width - 1] + "…"
    return typename


def _print_off_heap_holders(addr):
    """Attribute holders that live on a live stack rather than the heap: running
    threads (incl. active fibers) and parked fibers. Prints each with its
    backtrace. Returns True if anything was attributed."""
    threads = find_threads_referencing(addr)
    fibers = find_fibers_referencing(addr)
    if not threads and not fibers:
        return False
    print("")
    print("off-heap holders: %d live stack(s) pin this object:" % (len(threads) + len(fibers)))
    for thread, slot in threads:
        ident = "#%s" % thread.num
        try:
            lwp = thread.ptid[1]  # (pid, lwp, tid)
            if lwp:
                ident += " LWP %s" % lwp
        except Exception:
            pass
        if thread.name:
            ident += " (%s)" % thread.name
        print("  >>> running thread %s (stack slot 0x%x):" % (ident, slot))
        for line in format_thread_backtrace(thread):
            print("    " + line)
    for fidx, fib, slot in fibers:
        print("  >>> parked fiber #%d (stack slot 0x%x):" % (fidx, slot))
        for line in format_fiber_backtrace(fib):
            print("    " + line)
    return True


class YtRefcount(gdb.Command):
    """yt-rc-obj <addr>: resolve type and StrongCount/WeakCount."""

    def __init__(self):
        super().__init__("yt-rc-obj", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        addr = _parse_addr(arg)
        rc = resolve_refcount(addr)
        print("object   0x%x" % addr)
        print("type     %s" % rc.typename)
        if rc.inner:
            print("inner    %s" % rc.inner)
        if rc.ok:
            print("counter  0x%x" % rc.base_addr)
            print("strong   %d" % rc.strong)
            print("weak     %d" % rc.weak)
            if rc.signature in ("alive", "dead"):
                print("signature %s" % rc.signature)
            print("alive    %s" % ("yes" if rc.alive else "NO (freed?)"))
        else:
            print("ERROR    %s" % rc.error)


class YtHolders(gdb.Command):
    """yt-rc-backref <addr>: find live pointers to obj and classify them."""

    def __init__(self):
        super().__init__("yt-rc-backref", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        addr = _parse_addr(arg)
        target_rc, holders = analyze_holders(addr)
        print("target   0x%x  %s" % (addr, _fmt_rc(target_rc)))
        zones = sections().heap_zones()
        zbytes = sum(hi - lo for lo, hi in zones)
        print("zones    %d writable regions, %.1f MB scanned" % (len(zones), zbytes / 1e6))
        print("hits     %d pointer slots" % len(holders))
        print("")
        strong = sum(1 for h in holders if h.kind == "strong")
        print("%-18s %-7s %-18s %s" % ("slot", "kind", "container", "type / note"))
        for h in holders:
            cont = "0x%x" % h.container if h.container else "-"
            ct = _short(h.container_type, 70) if h.container_type else h.note
            print("0x%-16x %-7s %-18s %s" % (h.slot, h.kind, cont, ct))
        print("")
        print("strong holders found: %d  (target StrongCount=%s)" % (strong, target_rc.strong))
        if target_rc.strong is not None and strong != target_rc.strong:
            print("NOTE: strong-holder count != StrongCount "
                  "(some holders unclassified, on stack, or self-refs)")
        # Off-heap holders: a pointer living on a live stack (running thread,
        # active or parked fiber). Asked directly rather than keyed off holder
        # classification -- a stack slot may be misattributed to the enclosing
        # execution-stack object (e.g. a pooled stack) rather than read as raw.
        _print_off_heap_holders(addr)


class YtTrace(gdb.Command):
    """yt-rc-cycle <addr>: follow strong holders; report a retention CYCLE or root."""

    def __init__(self):
        super().__init__("yt-rc-cycle", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        addr = _parse_addr(arg)
        print("tracing retention of 0x%x" % addr)
        print("  %s" % _fmt_rc(resolve_refcount(addr)))
        print("")
        for i, r in enumerate(trace_retention(addr)):
            print("--- result %d: %s ---" % (i + 1, r["kind"].upper()))
            if r["kind"] == "cycle":
                self._print_path(r["path"])
                print("  >>> CYCLE: 0x%x holds back into the start object" % r["edge"][0])
            elif r["kind"] == "root":
                self._print_path(r.get("path", []))
                obj = r.get("obj", 0)
                print("  ROOT obj 0x%x : %s" % (obj, r.get("reason")))
                for h in r.get("candidates", []) or []:
                    print("    candidate holder: 0x%x %s [%s] %s" % (
                        h.container or 0, _short(h.container_type, 60), h.kind, h.note))
                # No heap holder accounts for it -> attribute to a live stack
                # (running thread / active or parked fiber).
                if obj:
                    _print_off_heap_holders(obj)
            else:
                self._print_path(r.get("path", []))
                print("  %s" % r.get("kind"))
            print("")

    def _print_path(self, path):
        for container, obj, h in path:
            crc = h.container_refcount
            cinfo = " (strong=%s)" % crc.strong if crc is not None and crc.ok else ""
            print("  0x%x  %s%s" % (container, _short(h.container_type, 60), cinfo))
            print("        --%s-->  0x%x" % (h.kind, obj))


class YtFind(gdb.Command):
    """yt-rc-find <type-substr>: signature-based sweep for live objects by type."""

    def __init__(self):
        super().__init__("yt-rc-find", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        substr = arg.strip()
        if not substr:
            raise gdb.GdbError("expected a type-name substring")
        print("scanning heap for live ref-counted objects matching %r ..." % substr)
        hits = find_live_objects_by_type(substr)
        if not hits:
            print("none found")
            return
        print("%-18s %-8s %s" % ("object", "strong", "type"))
        for obj, tn, strong in hits:
            print("0x%-16x %-8s %s" % (obj, strong, _short(tn, 80)))
        print("\n%d live instance(s). Trace one with: yt-rc-cycle <object>" % len(hits))


def register():
    YtRefcount()
    YtHolders()
    YtTrace()
    YtFind()
    print("Commands: yt-rc-obj, yt-rc-backref, yt-rc-cycle, yt-rc-find")


register()
