from __future__ import print_function
import re
import gdb


def to_int(value):
    try:
        return int(value)
    except (gdb.error, ValueError):
        return int(str(value), 16)


def to_ptr(value):
    return "0x%x" % long(value.cast(gdb.lookup_type("uintptr_t")))


def get_pretty_name(pc):
    # Here we rely on GDB address formatting. Please, check:
    # (gdb) show print symbol
    # (gdb) show print symbol-filename
    s = str(pc.cast(gdb.lookup_type("void").pointer()))
    m = re.match(r"^0x[0-9a-f]+ <(.+)\+\d+(?: at [^>]+)>$", s)

    if m:
        name = m.group(1)
    else:
        name = "???"

    if name.startswith("NYT::NDetail::TInvoker<"):
        return "(bound function invoker)"
    elif name.startswith("NYT::NDetail::TInvokerHelper<"):
        return "(bound function invoker helper)"
    elif name.startswith("NYT::NDetail::TRunnableAdapter<"):
        return "(bound function runnable adapter)"

    return "`" + name + "`"


def get_pretty_location(pc):
    sal = gdb.find_pc_line(to_int(to_ptr(pc)))
    return sal.pc, "%s:%s" % (sal.symtab.filename, sal.line)


def get_fiber_stack_pointer(expr):
    voidty = gdb.lookup_type("void")
    fiberty = gdb.lookup_type("NYT::NConcurrency::TFiber")

    fiberptr = gdb.parse_and_eval(expr)
    fiberptr = fiberptr.cast(fiberty.pointer())

    if to_int(fiberptr) == 0:
        raise RuntimeError("Null fiber pointer")

    fiber = fiberptr.dereference()
    state = fiber["State_"]["Value"]

    if str(state) == "NYT::NConcurrency::EFiberState::Running":
        raise RuntimeError("Fiber is running")

    return fiber["Context_"]["SP"].cast(voidty.pointer().pointer())


class FiberBtCmd(gdb.Command):
    """Print fiber backtrace by traversing frame pointers.

    This function should work if frame pointers are present in the inferior.

    Usage: (gdb) fbt <fiberptr>
    """

    def __init__(self):
        gdb.Command.__init__(self, "fbt", gdb.COMMAND_STACK, gdb.COMPLETE_NONE)

    def invoke(self, arg, _from_tty):
        vpp = gdb.lookup_type("void").pointer().pointer()

        fp = get_fiber_stack_pointer(arg) + 5  # See context-switching implementation.
        no = 1

        while to_int(fp) != 0:
            sp = (fp + 0).dereference().cast(vpp)
            pc = (fp + 1).dereference()

            if to_int(sp) != 0:
                name = get_pretty_name(pc)
                addr, location = get_pretty_location(pc)
                print("#%-3d sp=%-14s  at  pc=0x%x %s in %s" % (
                    no, sp, addr, name, location
                ))
                fp = sp.dereference().cast(vpp)
                no = no + 1
            else:
                fp = 0
                no = 0


class FiberCmd(gdb.Command):
    """Execute gdb command in the context of fiber <fiberptr>.

    Switch PC and SP to the ones in the fiber's structure,
    execute an arbitrary gdb command, and restore PC and SP.

    Usage: (gdb) fib <fiberptr> <gdbcmd>

    Note that it is ill-defined to modify state in the context of a goroutine.
    Restrict yourself to inspecting values.
    """

    def __init__(self):
        gdb.Command.__init__(self, "fib", gdb.COMMAND_STACK, gdb.COMPLETE_NONE)

    def invoke(self, arg, _from_tty):
        arg, cmd = arg.split(None, 1)

        fp = get_fiber_stack_pointer(arg) + 5  # See context-switching implementation.
        sp = (fp + 0).dereference()
        pc = (fp + 1).dereference()

        saved_frame = gdb.selected_frame()
        gdb.parse_and_eval("$saved_pc = $pc")
        gdb.parse_and_eval("$saved_sp = $sp")
        try:
            gdb.parse_and_eval("$sp = %s" % to_ptr(sp))
            gdb.parse_and_eval("$pc = %s" % to_ptr(pc))
            gdb.execute(cmd)
        finally:
            gdb.parse_and_eval("$pc = $saved_pc")
            gdb.parse_and_eval("$sp = $saved_sp")
            saved_frame.select()

FiberBtCmd()
FiberCmd()
