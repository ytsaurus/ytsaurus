import config
from contextlib import contextmanager

@contextmanager
def KeyboardInterruptsCatcher(keyboard_interrupt_action):
    """
    If caught KeyboardInterrupt(s), do keyboard_interrupt_action.
    """
    try:
        yield
    except KeyboardInterrupt:
        if config.KEYBOARD_ABORT:
            while True:
                try:
                    keyboard_interrupt_action()
                except KeyboardInterrupt:
                    continue
                break
        raise