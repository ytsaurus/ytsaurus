import logging
import threading

logger = logging.getLogger(__name__)


_TL_GUARDS = threading.local()


class GuardException(Exception):
    pass


class GuardTypes(object):
    FETCH = 'fetch'
    RUN_YMAKE = 'run_ymake'


def _pop_guard(storage, name):
    old_val = getattr(storage, name)
    res = old_val[-1]
    new_val = old_val[:-1]

    if new_val:
        setattr(storage, name, new_val)
    else:
        delattr(storage, name)

    return res


def _push_guard(storage, name, value):
    old_val = getattr(storage, name, [])
    new_val = old_val + [value]
    setattr(storage, name, new_val)


def _get_guard(storage, name):
    getattr(storage, name)


def _update_guard(storage, name, updater):
    if not hasattr(storage, name):
        return
    old_val = getattr(storage, name, None)
    new_val = list(map(updater, old_val))
    logger.debug('Updating guard %s from %s -> %s', name, old_val, new_val)
    setattr(storage, name, new_val)


def update_guard(name, updater=lambda _: True):
    _update_guard(_TL_GUARDS, name, updater)


def guarded(name, value=None, checker=lambda x: x is None, switch_off=lambda: False):
    def guardian(func):
        def boo(*args, **kwargs):
            with Guard(name, value, checker, switch_off):
                return func(*args, **kwargs)
        return boo

    return guardian


class Guard(object):
    def __init__(self, name, value=None, checker=lambda x: x is None, switch_off=lambda: False):
        self.name = name
        self.checker = checker
        self.start_value = value
        self.storage = _TL_GUARDS
        self.switch_off = switch_off

    def __enter__(self):
        logger.debug('Enter guard %s with value %s', self.name, self.start_value)
        _push_guard(self.storage, self.name, self.start_value)

    def __exit__(self, exc_type, exc_val, exc_tb):
        val = _pop_guard(self.storage, self.name)
        logger.debug('Exit guard %s with value %s', self.name, val)
        if not self.checker(val) and exc_type is None:
            if not self.switch_off():
                raise GuardException('Guard for {} failed with value {}'.format(self.name, val))
            else:
                logger.debug('Guard for %s could fail with value %s but switched off', self.name, val)
