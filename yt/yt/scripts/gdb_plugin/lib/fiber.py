# Fiber primitives: parse the TFiberRegistry list and read a fiber's saved
# registers (TContMachineContext). Self-contained (gdb only); the higher-level
# off-heap attribution that uses these lives in fiber_attribution.py.

import gdb

# Saved-register layout in TContMachineContext::Buf_ (util/system/context_x86_64.asm).
MJB_RBX = 0
MJB_RBP = 1
MJB_R12 = 2
MJB_R13 = 3
MJB_R14 = 4
MJB_R15 = 5
MJB_RSP = 6
MJB_PC = 7

# name -> index in TContMachineContext::Buf_ (a fiber's saved registers).
_FIBER_SEED_REGS = [
    ("rbx", MJB_RBX), ("rbp", MJB_RBP),
    ("r12", MJB_R12), ("r13", MJB_R13), ("r14", MJB_R14), ("r15", MJB_R15),
    ("rsp", MJB_RSP), ("rip", MJB_PC),
]


def get_gdb_version():
    return list(map(int, gdb.execute("show version", to_string=True).split("\n")[0].split(" ")[-1].split(".")))


class FrameSelector:
    def __init__(self, frame):
        self.frame = frame
        self.return_frame = gdb.selected_frame()

    def __enter__(self):
        self.frame.select()
        return self

    def __exit__(self, *exc):
        self.return_frame.select()
        return False


def select_frame(frame):
    return FrameSelector(frame)


def retrieve_fiber_context_regs(fiber):
    """Saved register buffer (Buf_) of a fiber's TContMachineContext."""
    context_type = gdb.lookup_type("TContMachineContext")
    return fiber["MachineContext_"].cast(context_type)["Buf_"]


def search_stack_for_symbol(eval_func, depth=10):
    """Run eval_func, walking to older frames if a symbol isn't in scope."""
    if depth == 0:
        return None
    try:
        return eval_func()
    except gdb.error:
        prev_frame = gdb.selected_frame().older()
        if prev_frame is not None:  # syscall frames may lack symbols
            with select_frame(prev_frame):
                return search_stack_for_symbol(eval_func, depth - 1)
    return None


def get_fiber_from_address(address):
    def eval_func():
        return gdb.parse_and_eval("{NYT::NConcurrency::TFiber} %s" % address)
    return search_stack_for_symbol(eval_func)


def is_intrusive_list(fibers):
    return str(fibers).find("Head_") != -1


def is_util_intrusive_list(fibers):
    return str(fibers).find("TIntrusiveList") != -1


def format_string_multiline(value):
    if get_gdb_version() >= [11, 2]:
        return value.format_string(max_elements=0, pretty_structs=True, pretty_arrays=True)
    gdb.execute("set max-value-size unlimited")
    gdb.execute("set print pretty on")
    return str(value)


def get_first_node(fibers):
    words = str(fibers).split(" ")
    for idx, word in enumerate(words):
        if word.find("Next") != -1:
            return words[idx + 2].replace(",", "")


def is_sentinel(node):
    return str(node).find("TFiberRegistry") != -1


# A registry node is a TIntrusiveNode base; the TFiber sits one vptr before it.
def obtain_fiber_address(node_address):
    return hex(int(node_address, base=16) - 8)


def get_prev_next_from_node(node_address):
    fiber = get_fiber_from_address(obtain_fiber_address(node_address))
    base_type = gdb.lookup_type(
        "NYT::TIntrusiveNode<NYT::NConcurrency::TFiber, NYT::NConcurrency::NDetail::TFiberRegisterTag>")
    prev = str(fiber.cast(base_type)["Prev"])
    next = str(fiber.cast(base_type)["Next"])
    return (prev, next)


def parse_intrusive_list(addresses, fibers):
    node = get_first_node(fibers)
    while not is_sentinel(node):
        addresses.append(obtain_fiber_address(node))
        (_, node) = get_prev_next_from_node(node)


def parse_vector(addresses, fibers):
    for line in format_string_multiline(fibers).split("\n"):
        if line.find("[") == -1:
            continue
        addresses.append(line.split(" ")[-1].replace(",", ""))


def parse_util_list(addresses, fibers):
    down_cast = str(fibers).find("(NYT::NConcurrency::NDetail::TFiberBase*)") != -1
    for line in format_string_multiline(fibers).split("\n"):
        if line.find("[") == -1:
            continue
        address = line.split(" ")[-3].replace("]", "")
        if down_cast:
            address = obtain_fiber_address(address)
        addresses.append(address)
