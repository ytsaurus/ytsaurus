from __future__ import print_function
import gdb


def from_vpp(v):
    if isinstance(v, gdb.Value):
        v = long(v.cast(gdb.lookup_type("uintptr_t")))
    assert isinstance(v, long)
    return v


def to_vpp(v):
    if isinstance(v, int) or isinstance(v, long):
        v = gdb.parse_and_eval(str(v))
    assert isinstance(v, gdb.Value)
    return v.cast(gdb.lookup_type("void").pointer().pointer())


def get_name(pc):
    s = gdb.execute("info symbol %s" % from_vpp(pc), to_string=True)
    i = s.find(" in section ")
    if s.startswith("No symbol matches"):
        return None
    else:
        return s[:i]


def get_pretty_name(pc):
    name = get_name(pc)
    if name is None:
        return "???"
    elif name.startswith("NYT::NDetail::TInvoker<"):
        return "(bound function invoker)"
    elif name.startswith("NYT::NDetail::TInvokerHelper<"):
        return "(bound function invoker helper)"
    elif name.startswith("NYT::NDetail::TRunnableAdapter<"):
        return "(bound function runnable adapter)"
    else:
        return name


def get_pretty_location(pc):
    sal = gdb.find_pc_line(from_vpp(pc))
    return sal.pc, "%s:%s" % (sal.symtab.filename, sal.line)


class FiberExecutionContextBase(object):
    """
    Stack layout:

      >            <--  stack grows down |
      >    0x0000  ..............********|  0xffff
      >                          ^       ^
      >                      SP -|       |- BP

    Suspended stack contains (top to bottom):

      > [0] %r15  <-- SP
      > [1] %r14
      > [2] %r13
      > [3] %r12
      > [4] %rbx
      > [5] %rbp  <-- FP
      > [6] frame return address

    """

    def __init__(self, sp, bp, sz):
        uintptr_t = gdb.lookup_type("uintptr_t")
        self.sp = long(sp.cast(uintptr_t))
        self.bp = long(bp.cast(uintptr_t))
        self.sz = long(sz.cast(uintptr_t))

    @property
    def is_corrupted(self):
        return self.sp < self.bp or self.sp >= self.bp + self.sz

    @property
    def frame_pointer(self):
        return to_vpp(self.sp) + 5

    @property
    def frame_fragments(self):
        begin = to_vpp(self.bp if self.is_corrupted else self.sp)
        end = to_vpp(self.bp + self.sz)

        result = []
        while begin < end:
            ptr = to_vpp(begin.dereference())
            name = get_name(ptr)
            if name is not None:
                result.append((ptr, get_pretty_name(ptr)))
            begin += 1
        return result


class FiberExecutionContext16(FiberExecutionContextBase):
    def __init__(self, expr):
        value = gdb.parse_and_eval(expr)
        fiber = None
        if str(value.type) == "NYT::TIntrusivePtr<NYT::NConcurrency::TFiber>":
            fiber = value["T_"]
        if str(value.type) == "NYT::NConcurrency::TFiber *":
            fiber = value
        if fiber is None:
            raise gdb.GdbError("Passed value `%s` of type `%s` is not a fiber." %
                               (str(value), str(value.type)))
        impl = fiber["Impl_"]["_M_t"]["_M_head_impl"]
        sp = impl["Context_"]["SP_"]
        bp = impl["Stack_"]["_M_ptr"]["Stack"]
        sz = impl["Stack_"]["_M_ptr"]["Size"]
        super(FiberExecutionContext16, self).__init__(sp, bp, sz)
        self.is_running = (str(impl["State_"]["_M_i"].cast(gdb.lookup_type("int"))) == "4")


class FiberExecutionContext17(FiberExecutionContextBase):
    def __init__(self, expr):
        value = gdb.parse_and_eval(expr)
        fiber = None
        if str(value.type) == "NYT::TIntrusivePtr<NYT::NConcurrency::TFiber>":
            fiber = value["T_"]
        if str(value.type) == "NYT::NConcurrency::TFiber *":
            fiber = value
        if fiber is None:
            raise gdb.GdbError("Passed value `%s` of type `%s` is not a fiber." %
                               (str(value), str(value.type)))
        sp = fiber["Context_"]["SP_"]
        bp = fiber["Stack_"]["_M_ptr"]["Stack_"]
        sz = fiber["Stack_"]["_M_ptr"]["Size_"]
        super(FiberExecutionContext17, self).__init__(sp, bp, sz)
        self.is_running = (str(fiber["State_"]["Value"]) == "NYT::NConcurrency::EFiberState::Running")


def arg_to_fiber_context(arg):
    argv = gdb.string_to_argv(arg)
    if argv[0] not in ["16", "17"]:
        raise gdb.GdbError("Please, specify either `16` or `17` as a first argument.")
    if argv[0] == "16":
        ctx = FiberExecutionContext16(argv[1])
    if argv[0] == "17":
        ctx = FiberExecutionContext17(argv[1])
    if ctx.is_running:
        print("WARNING: Fiber is currently running, results may be incorrect.")
    if ctx.is_corrupted:
        print("WARNING: Fiber is corrupted: SP %08x is out of range [%08x ; %08x)." %
              (ctx.sp, ctx.bp, ctx.bp + ctx.sz))
    return ctx


class FiberBtCmd(gdb.Command):
    """
    Print fiber backtrace by traversing frame pointers.

    This function should work if fiber is suspended and frame pointers are present on the stack.

    Usage: (gdb) fbt <16|17> <fiber>
    """

    def __init__(self):
        gdb.Command.__init__(self, "fbt", gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)

    def invoke(self, arg, _from_tty):
        ctx = arg_to_fiber_context(arg)

        fp = ctx.frame_pointer
        no = 1

        print("Unwinding stack...")
        try:
            while from_vpp(fp) != 0L:
                sp = to_vpp((fp + 0).dereference())
                pc = to_vpp((fp + 1).dereference())

                if from_vpp(sp) != 0L:
                    name = get_pretty_name(pc)
                    addr, location = get_pretty_location(pc)
                    print("#%-3d sp=%-14s  at  pc=0x%x %s in %s" % (
                        no, sp, addr, name
                    ))
                    fp = to_vpp(sp.dereference())
                    no = no + 1
                else:
                    fp = to_vpp(0)
                    no = 0
        except gdb.MemoryError, e:
            print("Unwinding failed: %s" % e)


class FiberScanCmd(gdb.Command):
    """
    Scan fiber stack and extract function-like pointers.

    Usage: (gdb) fscan <fiber>
    """

    def __init__(self):
        gdb.Command.__init__(self, "fscan", gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)

    def invoke(self, arg, _from_tty):
        ctx = arg_to_fiber_context(arg)

        print("Searching for stack entries that look like return addresses...")
        for pc, name in ctx.frame_fragments:
            if len(name) > 80:
                name = name[:80] + "..."
            print("# %14s in %s" % ("0x%08x" % from_vpp(pc), name))


class YtFrameFilter():
    def __init__(self):
        self.name = "YT"
        self.priority = 100
        self.enabled = True

        gdb.frame_filters[self.name] = self

    def filter(self, frame_iter):
        return frame_iter


FiberBtCmd()
FiberScanCmd()
