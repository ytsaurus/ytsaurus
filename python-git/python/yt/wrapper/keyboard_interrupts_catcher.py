import config
from contextlib import contextmanager

@contextmanager
def KeyboardInterruptsCatcher(keyboard_interrupt_action, limit=10):
    """
    If caught KeyboardInterrupt(s), do keyboard_interrupt_action.
    """
    try:
        yield
    except KeyboardInterrupt:
        if config.KEYBOARD_ABORT:
            counter = 0
            while True:
                try:
                    keyboard_interrupt_action()
                except KeyboardInterrupt:
                    counter += 1
                    if counter <= limit:
                        continue
                    else:
                        raise
                break
        raise
