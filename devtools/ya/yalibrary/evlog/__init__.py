import datetime
import logging
import os
import six
import sys
import threading
import time

from exts import fs
from exts import yjson
from exts import os2


_LOG_FILE_NAME_FMT = '%H-%M-%S'
_LOG_DIR_NAME_FMT = '%Y-%m-%d'
_EVLOG_SUFFIX = '.evlog'


def _fix_non_utf8(data):
    from six.moves import collections_abc

    def _decode_non_ut8_item(data, errors='replace'):
        return data.decode('UTF-8', errors=errors) if isinstance(data, six.binary_type) else data

    res = {}
    for k, v in data.items():
        if isinstance(v, six.binary_type):
            v = _decode_non_ut8_item(v)
        elif isinstance(v, (dict, type(os.environ))):
            v = _fix_non_utf8(v)
        elif isinstance(v, collections_abc.Iterable):
            v = list(map(_decode_non_ut8_item, v))
        res[_decode_non_ut8_item(k)] = v
    return res


class EmptyEvlogListException(Exception):
    mute = True


class DummyEvlog(object):
    def write(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        pass

    def get_writer(self, *args, **kwargs):
        return self


class Evlog(object):
    def __init__(self, evlog_dir, chunk_name, filename, replacements=None):
        self._evlog_dir = evlog_dir
        self._chunk_name = chunk_name
        self._filename = filename
        fs.create_dirs(os.path.join(evlog_dir, chunk_name))
        self.path = os.path.join(evlog_dir, chunk_name, filename)
        self._fileobj = open(self.path, 'w')
        self._lock = threading.Lock()

        self._replacements = []
        if not replacements:
            for k, v in six.iteritems(os.environ):
                if k.endswith('TOKEN') and Evlog.__json_safe(v) and v:
                    self._replacements.append(v)
        else:
            for v in replacements:
                if Evlog.__json_safe(v) and v:
                    self._replacements.append(v)

        self._replacements = sorted(self._replacements)

    @staticmethod
    def __json_safe(s):
        if any(
            [
                i in s
                for i in (
                    '"',
                    "[",
                    "]",
                    "{",
                    "}",
                    "\\",
                    ",",
                    ":",
                )
            ]
        ):
            return False
        if s in (
            "true",
            "false",
            "null",
        ):
            return False
        return True

    def _remove_secrets(self, s):
        for r in self._replacements:
            s = s.replace(r, "[SECRET]")
        return s

    def write(self, namespace, event, **kwargs):
        value = {
            'timestamp': time.time(),
            'thread_name': threading.current_thread().name,
            'namespace': namespace,
            'event': event,
            'value': kwargs,
        }
        try:
            s = yjson.dumps(value) + '\n'
        except (UnicodeDecodeError, OverflowError):
            s = yjson.dumps(_fix_non_utf8(value)) + '\n'

        s = self._remove_secrets(s)

        with self._lock:
            try:
                self._fileobj.write(s)
            except IOError as e:
                import errno

                if e.errno == errno.ENOSPC:
                    sys.stderr.write("No space left on device, clean up some files. Try running 'ya gc cache'\n")
                    sys.stderr.write(s)
                else:
                    raise e

    def get_writer(self, namespace):
        def inner(event, **kwargs):
            self.write(namespace, event, **kwargs)

        return inner

    def close(self):
        self._fileobj.close()

    def __iter__(self):
        for root, dirs, files in os2.fastwalk(self._evlog_dir):
            for f in files:
                if f.endswith(_EVLOG_SUFFIX) and f != self._filename:
                    yield os.path.join(root, f)

    def get_latest(self):
        evlogs = sorted(self)
        if not evlogs:
            raise EmptyEvlogListException('Empty event logs list')
        return evlogs[-1]


def with_evlog(params, evlog_dir, days_to_save, now_time, run_uid, hide_token):
    def parse_log_dir(x):
        return datetime.datetime.strptime(x, _LOG_DIR_NAME_FMT)

    def older_than(x):
        return now_time - x > datetime.timedelta(days=days_to_save)

    fs.create_dirs(evlog_dir)
    for x in os.listdir(evlog_dir):
        try:
            if older_than(parse_log_dir(x)):
                fs.remove_tree_safe(os.path.join(evlog_dir, x))
        except Exception:
            logging.debug("While analysing %s:", x, exc_info=sys.exc_info())

    log_chunk = now_time.strftime(_LOG_DIR_NAME_FMT)
    filename = (
        getattr(params, 'evlog_file', None) or now_time.strftime(_LOG_FILE_NAME_FMT) + '.' + run_uid + _EVLOG_SUFFIX
    )
    logging.debug('Event log file is %s', filename)

    evlog = Evlog(evlog_dir, log_chunk, filename, replacements=hide_token)
    evlog.write('init', 'init', args=sys.argv, env=os.environ.copy())

    try:
        yield evlog
    finally:
        evlog.close()
