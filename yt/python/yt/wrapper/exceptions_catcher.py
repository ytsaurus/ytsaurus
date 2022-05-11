from contextlib import contextmanager


@contextmanager
def ExceptionCatcher(exception_types, exception_action, enable=True, limit=10):
    """If KeyboardInterrupt(s) are caught, does keyboard_interrupt_action."""
    if enable:
        try:
            yield
        except exception_types:
            counter = 0
            while True:
                try:
                    exception_action()
                except exception_types:
                    counter += 1
                    if counter <= limit:
                        continue
                    else:
                        raise
                break
            raise
    else:
        yield


def KeyboardInterruptsCatcher(*args, **kwargs):
    return ExceptionCatcher(KeyboardInterrupt, *args, **kwargs)
