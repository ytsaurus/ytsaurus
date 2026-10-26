# GDB commands for TCont debugging (library/cpp/coroutine/engine)
# Based on devtools/gdb/yt_fibers_printer.py

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


class Cont():
    READY = 'ready'
    WAITING = 'waiting'

    def __init__(self, value, state):
        self.value = value
        self.name = value['Name_'].string()
        self.state = state


def retrieve_cont_context_regs(cont):
    context_type = gdb.lookup_type('TContMachineContext')
    result = cont.value['Trampoline_']['Ctx_'].cast(context_type)['Buf_']
    return result


def get_reg(reg):
    return gdb.parse_and_eval('(uint64_t)${}'.format(reg))


def set_reg(reg, value):
    return gdb.execute('set ${} = {}'.format(reg, value))


class ContContextSwitcher():
    def __init__(self, cont):
        self.old_regs = [get_reg(REG_NAMES[i]) for i in range(MJB_SIZE)]
        self.cont_regs = retrieve_cont_context_regs(cont)

    def switch(self):
        # Ensure that selected frame is stack top to prevent registers corruption.
        gdb.execute('select-frame 0')
        # Switch to cont context.
        for i in range(MJB_SIZE):
            set_reg(REG_NAMES[i], self.cont_regs[i])
        return self

    def switch_back(self):
        gdb.execute('select-frame 0')
        for i in range(MJB_SIZE):
            set_reg(REG_NAMES[i], self.old_regs[i])

    def __enter__(self):
        self.switch()
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            gdb.write('{}\n'.format(traceback))
        self.switch_back()


def switch_to_cont_context(cont):
    return ContContextSwitcher(cont)


def find_executor():
    frame = gdb.newest_frame()
    while frame is not None:
        if frame.name() is None:
            frame = frame.older()
            continue
        if frame.name() == 'NCoro::TTrampoline::DoRun':
            frame.select()
            return gdb.parse_and_eval('this->Cont_->Executor_').referenced_value()
        if frame.name().startswith('ContHelperFunc'):
            frame.select()
            return gdb.parse_and_eval('cont->Executor_').referenced_value()
        if frame.name().startswith('ContHelperMemberFunc'):
            frame.select()
            return gdb.parse_and_eval('c->Executor_').referenced_value()
        if frame.name() == 'TContExecutor::RunScheduler':
            frame.select()
            return gdb.parse_and_eval('this').dereference()
        frame = frame.older()
    return None


def for_each_in_intr_list(intrlist, action):
    it = intrlist['End_']['Next_'].dereference()
    while it.address != intrlist['End_'].address:
        action(it)
        it = it['Next_'].dereference()


def get_ready_cont_values(executor):
    cont_values = []
    cont_type = gdb.lookup_type('TCont')
    def collector(list_item):
        cont_values.append(list_item.cast(cont_type))
    for_each_in_intr_list(executor['Ready_'], collector)
    for_each_in_intr_list(executor['ReadyNext_'], collector)
    return cont_values


def apply_to_subtree(node, action):
    action(node)
    if node['Left_'] != 0:
        apply_to_subtree(node['Left_'].dereference(), action)
    if node['Right_'] != 0:
        apply_to_subtree(node['Right_'].dereference(), action)


def for_each_in_rbtree(tree, action):
    if tree['Data_']['Parent_'] != 0:
        apply_to_subtree(tree['Data_']['Parent_'].dereference(), action)


def get_waiting_cont_values(executor):
    cont_values = []
    cont_poll_event_type = gdb.lookup_type('NCoro::TContPollEvent')
    def collector(tree_node):
        cont = tree_node.cast(cont_poll_event_type)['Cont_'].dereference()
        if not cont['Scheduled_']:
            cont_values.append(cont)
    for_each_in_rbtree(executor['WaitQueue_']['IoWait_'], collector)
    return cont_values


conts_cache = {}


def get_conts():
    global conts_cache
    cache_entry = conts_cache.get(gdb.selected_thread())
    if cache_entry is not None:
        return cache_entry
    executor = find_executor()
    if executor is None:
        gdb.write('Failed to find current TContExecutor\n')
        return []
    conts = [Cont(value, Cont.READY) for value in get_ready_cont_values(executor)] + [Cont(value, Cont.WAITING) for value in get_waiting_cont_values(executor)]
    conts_cache[gdb.selected_thread()] = conts
    return conts


def offline_backtrace(cont):
    regs = retrieve_cont_context_regs(cont)
    rbp = int(regs[MJB_RBP])
    rip = int(regs[MJB_PC])
    depth = 0
    while rbp != 0 and rip != 0:
        gdb.write('#{} '.format(depth))
        block = gdb.block_for_pc(int(rip))
        function = None
        innermost_function = None
        while block is not None:
            if block.function is not None:
                function = block.function
                if innermost_function is None:
                    innermost_function = block.function
            block = block.superblock
        function_name = '(unknown function)'
        if function is not None:
            function_name = function.name
        sal = gdb.current_progspace().find_pc_line(rip - 5)
        file_line = '(unknown)'
        if sal.symtab is not None:
            file_line = '{}:{}'.format(sal.symtab.fullname(), sal.line)
        gdb.write('0x{:016X} / 0x{:016X} {} at {}\n'.format(rip, rbp, function_name, file_line))
        rip = int(gdb.parse_and_eval('*(uint64_t*){}'.format(rbp + 8)))
        new_rbp = int(gdb.parse_and_eval('*(uint64_t*){}'.format(rbp)))
        if new_rbp == rbp:
            gdb.write('(same frame, maybe executable compiled with -fomit-frame-pointer)\n')
            break
        rbp = new_rbp
        depth += 1


class PrintContsCommand(gdb.Command):
    '''Usage: print-conts [with-backtrace|with-bt]

    Prints list of current thread's coroutines (TConts).

    with-bt|with-backtrace - also print a backtrace for each TCont (full backtrace for live process debugging, call chain for debugging a core file)
    '''
    def __init__(self):
        super(PrintContsCommand, self).__init__('print-conts', gdb.COMMAND_USER)

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 1:
            gdb.write('Too many arguments\n')
            return
        with_bt = False
        if len(argv) == 1:
            if argv[0] in ['with-backtrace', 'with-bt']:
                with_bt = True
            else:
                gdb.write('Unknown argument: {}'.format(argv[0]))
                return

        gdb.execute('set print frame-arguments all')
        gdb.execute('set print entry-values compact')
        conts = get_conts()
        if len(conts) > 0:
            gdb.write('Found {} cont(s)\n'.format(len(conts)))
            for i, cont in enumerate(conts):
                gdb.write('Cont #{}: "{}", state: {}\n'.format(i, cont.name, cont.state))
                if with_bt:
                    if gdb.inferiors()[0].was_attached:
                        try:
                            with switch_to_cont_context(cont):
                                gdb.execute('where')
                                gdb.write('\n')
                        except Exception as ex:
                            gdb.write('Failed to switch to cont context: {}\n'.format(ex))
                    else:
                        offline_backtrace(cont)
                        gdb.write('\n')


class SelectContCommand(gdb.Command):
    '''Usage: select-cont [ID]

    Switches to the stack of TCont identified by ID (current running TCont, if ID omitted). Use print-conts to get IDs of current thread's TConts.
    Live process debugging only.
    '''
    def __init__(self):
        super(SelectContCommand, self).__init__('select-cont', gdb.COMMAND_USER)
        self.cont_switcher = None

    def invoke(self, argument, fromtty):
        argv = gdb.string_to_argv(argument)
        if len(argv) > 1:
            gdb.write('Too many arguments\n')
            return

        if len(argv) == 0:
            if self.cont_switcher is None:
                gdb.write('No cont context selected\n')
                return
            self.cont_switcher.switch_back()
            self.cont_switcher = None
            return

        if not self.cont_switcher is None:
            gdb.write('You must switch back to original context first\n')
            return

        try:
            ind = int(argv[0])
            conts = get_conts()
            if not (0 <= ind < len(conts)):
                gdb.write('Cont index must be in [0; {})\n'.format(len(conts)))
                return
            self.cont_switcher = ContContextSwitcher(conts[ind])
            self.cont_switcher.switch()
        except:
            gdb.write('Failed to select cont\n')
            raise


def register_commands():
    PrintContsCommand()
    SelectContCommand()
