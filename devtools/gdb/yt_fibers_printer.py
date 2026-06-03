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
    context_type = gdb.lookup_type('TContMachineContext')
    result = fiber['MachineContext_'].cast(context_type)['Buf_']
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
        # Ensure that selected frame is stack top to prevent registers corruption.
        gdb.execute('select-frame 0')
        # Switch to fiber context.
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
        # Could be syscall frame, where some symbols are unavailable.
        if not prev_frame is None:
            with select_frame(prev_frame):
                return search_stack_for_symbol(eval_func, depth - 1)
    return None


def get_fiber_from_address(address):
    def eval_func():
        return gdb.parse_and_eval('{{NYT::NConcurrency::TFiber}} {}'.format(address))
    return search_stack_for_symbol(eval_func)


def is_intrusive_list(fibers):
    return str(fibers).find("Head_") != -1


def is_util_intrusive_list(fibers):
    return str(fibers).find("TIntrusiveList") != -1


def get_first_node(fibers):
    words = str(fibers).split(' ')

    for idx, word in enumerate(words):
        if word.find("Next") != -1:
            return words[idx + 2].replace(',', '')


def is_sentinel(node):
    return str(node).find("TFiberRegistry") != -1


# Given address is that of node.
# Since fiber has ITrampoline base its address
# is 8 bytes left of the node's one
def obtain_fiber_address(node_address):
    vptr_size = 8
    int_addr = int(node_address, base=16)
    int_addr -= vptr_size
    return hex(int_addr)

def get_prev_next_from_node(node_address):
    fiber = get_fiber_from_address(obtain_fiber_address(node_address))

    base_type = gdb.lookup_type('NYT::TIntrusiveNode<NYT::NConcurrency::TFiber, NYT::NConcurrency::NDetail::TFiberRegisterTag>')
    prev = str(fiber.cast(base_type)['Prev'])
    next = str(fiber.cast(base_type)['Next'])

    return (prev, next)


def parse_intrusive_list(addresses, fibers):
    node = get_first_node(fibers)

    while not is_sentinel(node):
        addresses.append(obtain_fiber_address(node))

        (_, next) = get_prev_next_from_node(node)
        node = next


def parse_vector(addresses, fibers):
    for line in format_string_multiline(fibers).split('\n'):
        if line.find('[') == -1:
            continue
        address = line.split(' ')[-1].replace(',', '')
        addresses.append(address)


def parse_util_list(addresses, fibers):
    down_cast = (str(fibers).find("(NYT::NConcurrency::NDetail::TFiberBase*)") != -1)
    for line in format_string_multiline(fibers).split('\n'):
        if line.find('[') == -1:
            continue
        address = line.split(' ')[-3].replace(']', '')
        if down_cast:
            address = obtain_fiber_address(address)
        addresses.append(address)

def get_registered_fiber_addresses():
    def eval_func():
        return gdb.parse_and_eval('NYT::NConcurrency::TFiberRegistry::Get()->Fibers_')
    fibers = search_stack_for_symbol(eval_func)
    if not fibers:
        gdb.write('Could not find fiber registry\n')
        return []
    addresses = []

    if is_util_intrusive_list(fibers):
        parse_util_list(addresses, fibers)
        return addresses

    if is_intrusive_list(fibers):
        parse_intrusive_list(addresses, fibers)
        return addresses

    parse_vector(addresses, fibers)
    return addresses


def get_running_fiber_addresses():
    inferior = gdb.selected_inferior()
    selected_thread = gdb.selected_thread()
    addresses = []
    for thread in inferior.threads():
        thread.switch()
        def eval_func():
            return str(gdb.parse_and_eval('NYT::NConcurrency::NDetail::FiberContext->CurrentFiber'))
        address = search_stack_for_symbol(eval_func)
        if not address is None:
            addresses.append(address)
    selected_thread.switch()
    return addresses


cached_waiting_fibers = None

def format_string_multiline(value):
    if get_gdb_version() >= [11, 2]:
        return value.format_string(max_elements=0, pretty_structs=True, pretty_arrays=True)
    else:
        gdb.execute('set max-value-size unlimited')
        gdb.execute('set print pretty on')
        return str(value)


def get_waiting_fibers():
    global cached_waiting_fibers
    if not cached_waiting_fibers is None:
        return cached_waiting_fibers
    registered_fiber_addresses = get_registered_fiber_addresses()
    running_fibers_addresses = set(get_running_fiber_addresses())
    fibers = []
    n_filtered = 0
    for address in registered_fiber_addresses:
        if address not in running_fibers_addresses:
            fiber = get_fiber_from_address(address)
            if fiber is None:
                gdb.write('Failed to obtain instance for fiber {}\n'.format(address))
            else:
                fibers.append(fiber)
        else:
            n_filtered += 1
    gdb.write('Filtered out {} running fiber(s)\n'.format(n_filtered))
    cached_waiting_fibers = fibers
    return fibers


def get_compact_vector_elements(compact_vector):
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


def _try_get_trace_context_via_callback_frame():
    # Newer binaries: TCallback::operator() spills |BindState_.Get()| to the
    # stack under the well-known typed name |fiberBindState|. The bind state's
    # vtable identifies whether it's a TBindState<true, ...> instantiation
    # (i.e. carries TPropagatingStorage). If so, Storage_ lives at offset
    # sizeof(TBindStateBase) within the derived object — it is the first
    # member of TPropagateMixin<true>, which is the second base class.
    #
    # Reading via the vtable + layout rather than via a |dynamic_type| /
    # |lookup_type| cast lets the printer work even when the bound callable
    # is an unnamed lambda (whose synthetic gdb name |$_1| can't be parsed
    # as a C++ type).
    base_ptr = gdb.parse_and_eval('fiberBindState')
    if int(base_ptr) == 0:
        return None, False
    vptr_val = gdb.parse_and_eval('*(unsigned long*){}'.format(int(base_ptr)))
    sym_line = gdb.execute('info symbol {:#x}'.format(int(vptr_val)), to_string=True)
    if 'TBindState<true' not in sym_line:
        # Propagate=false: bind state present but no propagating storage to
        # report. Mark as handled so the walk stops at this frame.
        return None, True
    tbsb_size = gdb.lookup_type('NYT::NDetail::TBindStateBase').sizeof
    storage_addr = int(base_ptr) + tbsb_size
    impl_ptr = gdb.parse_and_eval('*(unsigned long*){}'.format(storage_addr))
    if int(impl_ptr) == 0:
        return None, True
    return gdb.parse_and_eval(
        '{{NYT::NTracing::TTraceContext}} NYT::NTracing::TryGetTraceContextFromPropagatingStorage('
        '*(NYT::NConcurrency::TPropagatingStorage*){})'.format(storage_addr)), True


def _try_get_trace_context_via_bind_state_frame():
    # COMPAT(babenko): older binaries spilled |state| to the stack inside
    # TBindState::Run under the name |unoptimizedState| specifically so the
    # printer could find it. Current binaries spill in TCallback::operator()
    # as |fiberBindState| (see _try_get_trace_context_via_callback_frame).
    # Drop this fallback once no live binaries predating the rework remain.
    is_null = int(gdb.parse_and_eval('unoptimizedState->Storage_.IsNull()'))
    if is_null:
        return None, True
    return gdb.parse_and_eval(
        '{NYT::NTracing::TTraceContext} '
        'NYT::NTracing::TryGetTraceContextFromPropagatingStorage(unoptimizedState->Storage_)'), True


def find_trace_context():
    frame = gdb.selected_frame()
    prev = frame
    thread = gdb.selected_thread()
    trace_context = None
    try:
        # For some reason it's faster to get full backtrace and check if frame present by hand
        # than checking output of select-frame.
        frames = gdb.execute('where', to_string=True)
        if frames.find('NYT::NConcurrency::NDetail::RunInFiberContext') == -1:
            return None
        gdb.execute('select-frame function NYT::NConcurrency::NDetail::RunInFiberContext(NYT::NConcurrency::TFiber*, NYT::TCallback<void ()>)')
        # Walk newer frames trying two extraction strategies: the newer
        # |fiberBindState| path (works on TCallback::operator() frames against
        # current binaries) and the COMPAT(babenko) |unoptimizedState| path
        # (works on TBindState::Run frames against pre-rework binaries).
        frame = gdb.selected_frame().newer()
        i = 0
        while frame:
            frame.select()
            for extractor in (
                _try_get_trace_context_via_callback_frame,
                _try_get_trace_context_via_bind_state_frame,
            ):
                try:
                    tc, found = extractor()
                except gdb.error:
                    continue
                if not found:
                    continue
                if tc is not None:
                    thread.switch()
                    frame.select()
                    trace_context = tc
                else:
                    gdb.write(f'Trace context at frame[{i}] is null\n')
                break
            if trace_context is not None:
                break
            prev = frame
            frame = frame.newer()
            i += 1
    except gdb.error as e:
        gdb.write('GDB error: {}\n'.format(e))
        pass
    finally:
        thread.switch()
        prev.select()
    return trace_context


class PrintFibersCommand(gdb.Command):
    def __init__(self):
        super(PrintFibersCommand, self).__init__('print_yt_fibers', gdb.COMMAND_USER)

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 0:
            gdb.write('No arguments required\n')
            return

        fibers = get_waiting_fibers()
        gdb.write('Found {} waiting fiber(s)\n'.format(len(fibers)))
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
            return

        fibers = get_waiting_fibers()
        gdb.write('Total {} fibers\n'.format(len(fibers)))
        for i, fiber in enumerate(fibers):
            with switch_to_fiber_context(fiber):
                gdb.write('{}\n'.format('Fiber #{}'.format(i)))
                trace_context = find_trace_context()
                if not trace_context is None:
                    try:
                        gdb.write('Logging tag: {}\n'.format(trace_context['LoggingTag_']))
                    except:
                        pass
                    try:
                        gdb.write('Tags: {}\n'.format(', '.join(
                            '{} = {}'.format(tag['first'], tag['second']) for tag in get_compact_vector_elements(trace_context['Tags_'])
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
            return

        if len(argv) == 0:
            if self.fiber_switcher is None:
                gdb.write('No fiber context selected\n')
                return
            self.fiber_switcher.switch_back()
            self.fiber_switcher = None
            return

        if not self.fiber_switcher is None:
            gdb.write('You must switch back to original context first\n')
            return

        try:
            ind = int(argv[0])
            fibers = get_waiting_fibers()
            if not (0 <= ind < len(fibers)):
                gdb.write('Fiber index must be in [0; {})\n'.format(len(fibers)))
                return
            self.fiber_switcher = FiberContextSwitcher(fibers[ind])
            self.fiber_switcher.switch()
        except:
            gdb.write('Failed to select fiber\n')
            raise


def register_fibers_printer():
    PrintFibersCommand()
    PrintFibersWithTraceTagsCommand()
    SelectFiberCommand()
