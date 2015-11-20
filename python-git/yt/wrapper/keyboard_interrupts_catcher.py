from contextlib import contextmanager

@contextmanager
def KeyboardInterruptsCatcher(keyboard_interrupt_action, enable=True, limit=10):
    """
    If caught KeyboardInterrupt(s), do keyboard_interrupt_action.
    """
    if enable:
        try:
            yield
        except KeyboardInterrupt:
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
    else:
        yield
