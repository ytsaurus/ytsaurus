import gdb

# Defined in util/system/context_x86_64.asm
MJB_RBX = 0
MJB_RBP = 1
MJB_R12 = 2
MJB_R13 = 3
MJB_R14 = 4
MJB_R15 = 5
MJB_RSP = 6
MJB_PC = 7
MJB_SIZE = 8

REG_NAMES = [None for i in range(MJB_SIZE)]
REG_NAMES[MJB_RBX] = 'rbx'
REG_NAMES[MJB_RBP] = 'rbp'
REG_NAMES[MJB_R12] = 'r12'
REG_NAMES[MJB_R13] = 'r13'
REG_NAMES[MJB_R14] = 'r14'
REG_NAMES[MJB_R15] = 'r15'
REG_NAMES[MJB_RSP] = 'rsp'
REG_NAMES[MJB_PC] = 'rip'


def get_gdb_version():
    return list(map(int, gdb.execute('show version', to_string=True).split('\n')[0].split(' ')[-1].split('.')))


class FrameSelector():
    def __init__(self, frame):
        self.frame = frame
        self.return_frame = gdb.selected_frame()

    def __enter__(self):
        self.frame.select()
        return self

    def __exit__(self, type, value, traceback):
        if not type is None:
            gdb.write('{}\n'.format(type))
            gdb.write('{}\n'.format(value))
            gdb.write('{}\n'.format(traceback))
        self.return_frame.select()


def select_frame(arg):
    return FrameSelector(arg)


def retrieve_fiber_context_regs(fiber):
    t = gdb.lookup_type('TContMachineContext')
    result = fiber['Context_'].cast(t)['Buf_']
    return result


def get_reg(reg):
    return gdb.parse_and_eval('(uint64_t)${}'.format(reg))


def set_reg(reg, value):
    return gdb.execute('set ${} = {}'.format(reg, value))


class FiberContextSwitcher():
    def __init__(self, fiber):
        self.old_regs = [get_reg(REG_NAMES[i]) for i in range(MJB_SIZE)]
        self.fiber_regs = retrieve_fiber_context_regs(fiber)

    def switch(self):
        # Ensure that selected frame is stack top to prevent registers corruption
        gdb.execute('select-frame 0')
        # Switch to fiber context
        for i in range(MJB_SIZE):
            set_reg(REG_NAMES[i], self.fiber_regs[i])
        return self

    def switch_back(self):
        gdb.execute('select-frame 0')
        for i in range(MJB_SIZE):
            set_reg(REG_NAMES[i], self.old_regs[i])

    def __enter__(self):
        self.switch()
        return self

    def __exit__(self, type, value, traceback):
        if not type is None:
            gdb.write('{}\n'.format(traceback))
        self.switch_back()


def switch_to_fiber_context(fiber):
    return FiberContextSwitcher(fiber)


def search_stack_for_symbol(eval_func, depth=10):
    if depth == 0:
        return None
    try:
        return eval_func()
    except gdb.error:
        prev_frame = gdb.selected_frame().older()
        # May be syscall frame, where some symbols are unavailable
        if not prev_frame is None:
            with select_frame(prev_frame):
                return search_stack_for_symbol(eval_func, depth - 1)
    return None


def get_fiber_registry():
    def eval_func():
        return gdb.parse_and_eval('NYT::NConcurrency::GetFiberRegistry()->Fibers_')
    return search_stack_for_symbol(eval_func)


def get_running_fibers():
    inferior = gdb.selected_inferior()
    selected_thread = gdb.selected_thread()
    running_fibers = []
    for thread in inferior.threads():
        thread.switch()
        def eval_func():
            return str(gdb.parse_and_eval('NYT::NConcurrency::FiberContext->CurrentFiber.T_'))
        fiber = search_stack_for_symbol(eval_func)
        if not fiber is None:
            running_fibers.append(fiber)
    selected_thread.switch()
    return running_fibers


fibers_list = None

def format_string_multiline(value):
    if get_gdb_version() >= [11, 2]:
        return value.format_string(max_elements=0, pretty_structs=True, pretty_arrays=True)
    else:
        gdb.execute('set max-value-size unlimited')
        gdb.execute('set print pretty on')
        return str(value)


def retrieve_fibers_list():
    global fibers_list
    if not fibers_list is None:
        return fibers_list
    registry = get_fiber_registry()
    if registry is None:
        gdb.write('Couldn\'t find fiber registry\n')
        return None
    fibers = []
    running = get_running_fibers()
    n_filtered = 0
    for i in format_string_multiline(registry).split('\n'):
        if i.find('[') == -1:
            continue
        address = i.split(' ')[-1].replace(',', '')
        if not address in running:
            def eval_func():
                return gdb.parse_and_eval('{{NYT::NConcurrency::TFiber}} {}'.format(address))
            fiber = search_stack_for_symbol(eval_func)
            fibers.append(fiber)
        else:
            n_filtered += 1
    gdb.write('Filtered {} running fibers\n'.format(n_filtered))
    fibers_list = fibers
    return fibers


def get_compact_elements(compact_vector):
    if compact_vector is None:
        return None
    result = []
    inline_size = int(compact_vector['InlineMeta_']['SizePlusOne'])
    if inline_size > 0:
        inline_size -= 1
        for i in range(inline_size):
            result.append(compact_vector['InlineElements_'][i])
        return result

    storage = compact_vector['OnHeapMeta_']['Storage'].dereference()
    elements = storage['Elements']
    end = storage['End']
    heap_size = gdb.parse_and_eval('({} - {}) / {}'.format(
        end,
        elements,
        end.type.target().sizeof
    ))
    for i in range(heap_size):
        result.append(elements[i])
    return result


def find_trace_context():
    frame = gdb.selected_frame()
    thread = gdb.selected_thread()
    try:
        # For some reason it's faster to get full backtrace and check if frame present by hand,
        # than check output of select-frame
        frames = gdb.execute('where', to_string=True)
        if frames.find('NYT::NConcurrency::RunInFiberContext') == -1:
            return None
        gdb.execute('select-frame function NYT::NConcurrency::RunInFiberContext(NYT::TCallback<void ()>)')
        target_frame = gdb.selected_frame().newer()
        if not target_frame is None:
            target_frame.select()
            # gdb can create dummy thread here
            is_null = int(gdb.parse_and_eval('uninlined_state->Storage_.IsNull()'))
            if not is_null:
                thread.switch()
                target_frame.select()
                return gdb.parse_and_eval('{NYT::NTracing::TTraceContext} NYT::NTracing::RetrieveTraceContextFromPropStorage(&uninlined_state->Storage_)')
            else:
                gdb.write('Trace context is NULL\n')
        return None
    except gdb.error:
        pass
    finally:
        thread.switch()
        frame.select()
    return None


class PrintFibersCommand(gdb.Command):
    def __init__(self):
        super(PrintFibersCommand, self).__init__('print_yt_fibers', gdb.COMMAND_USER)

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 0:
            gdb.write('No arguments required\n')
        else:
            fibers = retrieve_fibers_list()
            if fibers is None:
                return
            gdb.write('Total {} fibers\n'.format(len(fibers)))
            for i, fiber in enumerate(fibers):
                with switch_to_fiber_context(fiber):
                    gdb.write('Fiber #{}\n'.format(i))
                    gdb.execute('where')
                    gdb.write('\n')


class PrintFibersWithTraceTagsCommand(gdb.Command):
    def __init__(self):
        super(PrintFibersWithTraceTagsCommand, self).__init__('print_yt_fibers_with_tags', gdb.COMMAND_USER)

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 0:
            gdb.write('No arguments required\n')
        else:
            fibers = retrieve_fibers_list()
            if fibers is None:
                return
            gdb.write('Total {} fibers\n'.format(len(fibers)))
            for i, fiber in enumerate(fibers):
                with switch_to_fiber_context(fiber):
                    gdb.write('{}\n'.format('Fiber #{}'.format(i)))
                    trace_context = find_trace_context()
                    if not trace_context is None:
                        try:
                            gdb.write('logging tag: {}\n'.format(trace_context['LoggingTag_']))
                        except:
                            pass
                        try:
                            gdb.write('tags: {}\n'.format(', '.join(
                                '{} = {}'.format(tag['first'], tag['second']) for tag in get_compact_elements(trace_context['Tags_'])
                            )))
                        except:
                            pass
                    else:
                        gdb.write('Trace context not found\n')
                    gdb.execute('where')
                    gdb.write('\n')


class SelectFiberCommand(gdb.Command):
    def __init__(self):
        super(SelectFiberCommand, self).__init__('select_yt_fiber', gdb.COMMAND_USER)
        self.fiber_switcher = None

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 1:
            gdb.write('Too many arguments\n')
        elif len(argv) == 0:
            if self.fiber_switcher is None:
                gdb.write('No fiber context selected\n')
                return
            self.fiber_switcher.switch_back()
            self.fiber_switcher = None
        else:
            if not self.fiber_switcher is None:
                gdb.write('You must switch back to original context first\n')
                return
            try:
                ind = int(argv[0])
                fibers = retrieve_fibers_list()
                if not (0 <= ind < len(fibers)):
                    gdb.write('ind must be in [0; {})\n'.format(len(fibers)))
                    return
                self.fiber_switcher = FiberContextSwitcher(fibers[ind])
                self.fiber_switcher.switch()
            except:
                gdb.write('Failed\n')
                raise


def register_fibers_printer():
    PrintFibersCommand()
    PrintFibersWithTraceTagsCommand()
    SelectFiberCommand()
