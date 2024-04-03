import ctypes
import os

PR_SET_PDEATHSIG = 1
PR_GET_PDEATHSIG = 2

PR_SET_NAME = 15
PR_GET_NAME = 16
TASK_NAME_LEN = 15

libc = ctypes.CDLL("libc.so.6", use_errno=True)


def prctl(option: int, *args):
    args += (ctypes.c_ulong(0),) * (4 - len(args))
    ret = libc.prctl(option, *args)
    if ret < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return ret


def set_pdeathsig(sig: int):
    prctl(PR_SET_PDEATHSIG, ctypes.c_ulong(sig))


def get_pdeathsig() -> int:
    sig = ctypes.c_int()
    prctl(PR_GET_PDEATHSIG, ctypes.byref(sig))
    return sig.value


def set_name(name: str):
    buf = ctypes.create_string_buffer(name.encode())
    # Buffer contains null-termination byte and actual limit for the size buffer is TASK_NAME_LEN + 1 bytes.
    if len(buf) > TASK_NAME_LEN + 1:
        raise Exception(f"Length of encoded name '{name}' is too long: {len(buf)} > {TASK_NAME_LEN}")
    prctl(PR_SET_NAME, buf)


def get_name() -> str:
    buf = ctypes.create_string_buffer(TASK_NAME_LEN)
    prctl(PR_GET_NAME, buf)
    return buf.value.decode()
