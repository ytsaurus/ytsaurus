# Interactive fiber commands.
#
#     yt-fiber-list           concise roster of parked fibers (one line each)
#     yt-fiber-bt <n>         full backtrace of parked fiber n (read-only)
#     yt-fiber-select [<n>]   switch gdb into fiber n's register context
#                             (no argument restores the original context)
#
# All build on the core-friendly enumeration in fiber_attribution.py (the
# TFiberRegistry is read straight from memory, no inferior calls), so the list
# and backtrace work on a coredump. yt-fiber-select needs a live inferior (gdb
# forbids modifying registers on a core) and degrades with a clear message.

import re

import gdb

import fiber
from fiber_attribution import fiber_stacks, format_fiber_backtrace

# Scheduler / context-switch plumbing that wraps every parked fiber; skipped when
# picking the "interesting" leaf frame for the concise roster.
_PLUMBING = (
    "MachineContext", "SwitchTo", "SwitchFromFiber", "YieldFiber",
    "WaitUntilSet", "WaitFor", "RunInFiberContext", "FiberTrampoline",
    "TBindState", "TCallback", "TInvoker", "TRunnableAdapter",
)


def _parked_fiber(index):
    for _lo, _hi, _rsp, idx, fib in fiber_stacks():
        if idx == index:
            return fib
    return None


def _frame_symbol(line):
    """The symbol part of a backtrace line, or None for a non-frame line."""
    m = re.search(r"0x[0-9a-f]+\s+(.+)$", line)
    return m.group(1).strip() if m else None


def _interesting_frame(lines):
    """The first frame outside the scheduler plumbing -- i.e. the user code the
    fiber is actually parked in. Falls back to the deepest available frame."""
    fallback = None
    for line in lines:
        sym = _frame_symbol(line)
        if sym is None:
            continue
        if fallback is None:
            fallback = sym
        if not any(p in sym for p in _PLUMBING):
            return sym
    return fallback or "?"


class YtFiberList(gdb.Command):
    """yt-fiber-list: concise roster of parked fibers (index, rsp, leaf frame)."""

    def __init__(self):
        super().__init__("yt-fiber-list", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        stacks = fiber_stacks()
        if not stacks:
            print("no parked fibers found")
            return
        print("%d parked fiber(s); backtrace one with: yt-fiber-bt <n>" % len(stacks))
        for _lo, _hi, rsp, idx, fib in stacks:
            frame = _interesting_frame(format_fiber_backtrace(fib))
            print("  #%-4d rsp=0x%012x  %s" % (idx, rsp, frame))


class YtFiberBacktrace(gdb.Command):
    """yt-fiber-bt <n>: full backtrace of parked fiber n (read-only, core-safe)."""

    def __init__(self):
        super().__init__("yt-fiber-bt", gdb.COMMAND_STACK)

    def invoke(self, arg, from_tty):
        arg = arg.strip()
        if not arg:
            raise gdb.GdbError("expected a fiber index (see yt-fiber-list)")
        try:
            index = int(arg)
        except ValueError:
            raise gdb.GdbError("expected a fiber index")
        fib = _parked_fiber(index)
        if fib is None:
            raise gdb.GdbError("no parked fiber #%d (run yt-fiber-list)" % index)
        print("fiber #%d:" % index)
        for line in format_fiber_backtrace(fib):
            print("  " + line)


class _FiberRegisterSwitcher:
    """Save the live registers, set them to a fiber's saved TContMachineContext,
    and restore on demand. The switch lets gdb's native unwinder walk the fiber."""

    _REG_NAMES = [name for name, _idx in fiber._FIBER_SEED_REGS]

    def __init__(self, fib):
        self._saved = {
            name: int(gdb.parse_and_eval("(unsigned long)$%s" % name))
            for name in self._REG_NAMES
        }
        self._buf = fiber.retrieve_fiber_context_regs(fib)

    def _apply(self, values):
        # Pin the innermost frame first so setting registers can't corrupt a
        # selected outer frame.
        gdb.execute("select-frame 0")
        for name, value in values.items():
            gdb.execute("set $%s = %d" % (name, value))

    def switch(self):
        self._apply({name: int(self._buf[idx]) for name, idx in fiber._FIBER_SEED_REGS})

    def restore(self):
        self._apply(self._saved)


class YtFiberSelect(gdb.Command):
    """yt-fiber-select [<n>]: switch gdb into parked fiber n's context; no argument
    restores. While selected, `bt` / `info locals` operate on the fiber. Needs a
    live inferior -- gdb cannot modify registers on a coredump."""

    def __init__(self):
        super().__init__("yt-fiber-select", gdb.COMMAND_STACK)
        self._active = None

    def invoke(self, arg, from_tty):
        arg = arg.strip()
        if not arg:
            if self._active is None:
                print("no fiber context selected")
                return
            self._active.restore()
            self._active = None
            print("restored original context")
            return
        if self._active is not None:
            print("already in a fiber context; run 'yt-fiber-select' with no argument to restore first")
            return
        try:
            index = int(arg)
        except ValueError:
            raise gdb.GdbError("expected a fiber index")
        fib = _parked_fiber(index)
        if fib is None:
            raise gdb.GdbError("no parked fiber #%d (run yt-fiber-list)" % index)
        switcher = _FiberRegisterSwitcher(fib)
        try:
            switcher.switch()
        except gdb.error as e:
            # Setting registers needs a live inferior; gdb forbids it on a core.
            raise gdb.GdbError(
                "cannot switch register context on a coredump (%s); use "
                "yt-fiber-bt %d for a read-only backtrace instead" % (e, index))
        self._active = switcher
        print("switched to fiber #%d; use bt / info locals, then 'yt-fiber-select' to restore" % index)


def register():
    YtFiberList()
    YtFiberBacktrace()
    YtFiberSelect()
    print("Commands: yt-fiber-list, yt-fiber-bt, yt-fiber-select")


register()
