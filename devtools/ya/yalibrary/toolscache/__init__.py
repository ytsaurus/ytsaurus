import contextlib
import datetime
import logging
import os
import grpc
import six
import subprocess
import threading
import time

from devtools.distbuild.libs.gsid_classifier.python import gsid_classifier

import core.gsid
import core.report
import exts.process as extp

from core.config import tool_root, misc_root, build_root
from core.config import has_mapping
from exts.windows import on_win
from exts import fs
from exts import func
from exts import retry
from exts.timer import AccumulateTime

from yalibrary.evlog import _LOG_DIR_NAME_FMT, _LOG_FILE_NAME_FMT

logger = logging.getLogger(__name__)


# Uses app_ctx.params to query command-line options.


@func.lazy
def supported():
    return not (on_win() or has_mapping())


@func.lazy
def _logs_are_enabled():
    try:
        # Check if logs are enabled.
        import app_ctx
        app_ctx.file_log
        return True
    except (ImportError, AttributeError):
        return False


@func.lazy
def _log_name():
    if _logs_are_enabled():
        now = datetime.datetime.now()
        directory = os.path.join(misc_root(), 'logs', now.strftime(_LOG_DIR_NAME_FMT))
        try:
            fs.ensure_dir(directory)
            log_file = os.path.join(directory, '{}.{}.tclog'.format(now.strftime(_LOG_FILE_NAME_FMT), core.gsid.uid()))

            import app_ctx
            app_ctx.dump_debug['tc_log'] = log_file

            return log_file
        except EnvironmentError:
            return None
    else:
        return None


def _retry_grpc(e):
    if isinstance(e, _RestartTCException):
        return True

    if isinstance(e, subprocess.CalledProcessError):
        return True

    if not isinstance(e, grpc.RpcError) or not hasattr(e, 'code'):
        return False

    # UNKNOWN for grpc#15623: "Stream removed" in details
    # FAILED_PRECONDITION and UNIMPLEMENTED for race during server shutdown.
    return e.code() in [grpc.StatusCode.CANCELLED, grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNKNOWN,
                        grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNIMPLEMENTED]


@contextlib.contextmanager
def _with_no_tc_notification(cls, thr_ident):
    """ Ignore tc notifications for thread thr_ident """
    cls._in_progress_thread = thr_ident
    yield
    cls._in_progress_thread = None


class _RestartTCException(Exception):
    pass


# Essentially a singleton
class _Server(object):
    class _Report(object):
        _grpc_exceptions = 0
        _restarts_failed = 0
        _restarts_backward = 0
        _non_cached = 0

        @classmethod
        def get_stat_dict(cls):
            return {'grpc_exceptions': cls._grpc_exceptions,
                    'restarts_failed': cls._restarts_failed,
                    'restarts_backward': cls._restarts_backward,
                    'non_cached': cls._non_cached,
                    'tc': _SERVER._tc_enable,
                    'ac': _SERVER._ac_enable,
                    }

    _tc_lock = threading.Lock()
    _ac_lock = threading.Lock()
    _in_progress_thread = None
    _full_path = None
    _sb_path = None
    _sb_path_id = None
    _sb_id = None
    _ini_file = None
    _done = False

    _read_config = False
    _single_instance = False

    _tc_enable = False
    _tc_master = False
    _tc_full_address = None
    _tc_lock_file = None
    _tc_cache_size = None

    _ac_enable = False
    _ac_master = False
    _ac_full_address = None
    _ac_lock_file = None
    _ac_cache_size = None

    _dead_line = None

    _gl_config_options = {}
    _tc_config_options = {}
    _ac_config_options = {}

    _bld_dir = None

    _tool_version = 4

    @classmethod
    def _do_read_config(cls, opts):
        if cls._read_config:
            return

        with cls._tc_lock:
            if cls._read_config:
                return

            if not opts:
                try:
                    import app_ctx
                    params = app_ctx.params
                except (ImportError, AttributeError):
                    logger.debug("Tools cache app_ctx unavailable, use global setup")
                    # wait for app.execute to complete
                    # cls._read_config = True
                    cls._Report._non_cached += 1
                    return
            else:
                logger.debug("Tools cache uses passed opts")
                params = opts

            try:
                import app_ctx
                dump_debug = app_ctx.dump_debug
            except (ImportError, AttributeError):
                logger.debug("Dump debug unavailable")
                dump_debug = {}

            if not cls._is_user_build():
                cls._read_config = True
                logger.debug("Tools cache default parameters (disabled in batch mode)")
                return

            if not getattr(params, 'tools_cache', False):
                cls._read_config = True
                logger.debug("Disabled tools cache")
                return
            else:
                cls._tc_enable = True

            if params.tools_cache_master:
                cls._tc_master = True

            if params.build_cache:
                cls._ac_enable = True

            if params.build_cache_master:
                cls._ac_master = params.build_cache_master

            cls._gl_config_options = params.tools_cache_gl_conf
            cls._tc_config_options = params.tools_cache_conf
            cls._ac_config_options = params.build_cache_conf

            if params.tools_cache_ini:
                cls._ini_file = params.tools_cache_ini

            if 'wait_term_s' in cls._gl_config_options:
                cls._dead_line = int(cls._gl_config_options['wait_term_s'])

            if 'lock_file' in cls._tc_config_options:
                cls._tc_lock_file = cls._tc_config_options['lock_file']
            else:
                cls._tc_lock_file = os.path.join(tool_root(cls._tool_version), '.cache_lock')

            if cls._ini_file is None and 'cache_dir' not in cls._ac_config_options and hasattr(params, 'bld_dir'):
                cls._bld_dir = os.path.join(params.bld_dir, 'cache', '7')
            elif cls._ini_file is None and 'cache_dir' not in cls._ac_config_options and getattr(params, 'custom_build_directory', None):
                cls._bld_dir = os.path.join(params.custom_build_directory, 'cache', '7')
            try:
                dump_debug['build_cache_root'] = cls._bld_dir
                dump_debug['build_cache_db_file'] = os.path.join(cls._bld_dir, "acdb.sqlite")
            except Exception as e:
                logger.debug("(dump_debug) While store build cache info into: %s", e)

            cls._single_instance = 'service' in cls._gl_config_options and cls._gl_config_options['service'] == 'all'

            if cls._single_instance:
                cls._ac_lock_file = cls._tc_lock_file
            else:
                if 'lock_file' in cls._ac_config_options:
                    cls._ac_lock_file = cls._ac_config_options['lock_file']
                elif cls._bld_dir:
                    cls._ac_lock_file = os.path.join(cls._bld_dir, '.cache_lock')
                else:
                    cls._ac_lock_file = os.path.join(build_root(), 'cache', '7', '.cache_lock')

            cls._ac_cache_size = getattr(params, "cache_size", None)
            cls._tc_cache_size = getattr(params, "tools_cache_size", None)

            if cls._ac_cache_size is not None:
                cls._ac_cache_size = min(8223372036854775808, long(cls._ac_cache_size) if six.PY2 else int(cls._ac_cache_size))  # noqa

            if cls._tc_cache_size is not None:
                cls._tc_cache_size = min(8223372036854775808, long(cls._tc_cache_size) if six.PY2 else int(cls._tc_cache_size))  # noqa

            if params.tools_cache_bin:
                cls._full_path = params.tools_cache_bin
                cls._done = True

            # For testing only
            if 'sb_path' in cls._tc_config_options:
                cls._sb_path = cls._tc_config_options['sb_path']

            # For testing only
            if 'sb_path_id' in cls._tc_config_options:
                cls._sb_path_id = cls._tc_config_options['sb_path_id']

            # For testing only
            if 'sb_id' in cls._tc_config_options:
                cls._sb_id = cls._tc_config_options['sb_id']

            logger.debug("Tools cache parameters: tc enabled=%s, tc master=%s, ac enabled=%s, ac master=%s, tc_lock_file=%s, ac_lock_file=%s, binary=%s, ini=%s, tc_conf=%s, ac_conf=%s, gl_conf=%s",
                         cls._tc_enable, cls._tc_master, cls._ac_enable, cls._ac_master, cls._tc_lock_file, cls._ac_lock_file,
                         cls._full_path, cls._ini_file, cls._tc_config_options, cls._ac_config_options, cls._gl_config_options)

            cls._read_config = True

    @classmethod
    def _is_user_build(cls):
        return gsid_classifier.USER_BUILD == gsid_classifier.classify_gsid(core.gsid.flat_session_id())

    @classmethod
    def _run_tc_binary(cls, thr_ident):
        if cls._sb_id or cls._sb_path_id:
            with _with_no_tc_notification(cls, thr_ident):
                cls._fetch_ya_tc()

        args = ["--daemonize"] + \
            (["--ini-file", cls._ini_file] if cls._ini_file else ["--tools_cache-lock_file", cls._tc_lock_file]) + \
            ([] if _logs_are_enabled() else ["--no-logs"]) + \
            (["--tools_cache-sb_id", cls._sb_id] if cls._sb_id else []) + \
            (["--tools_cache-sb_path", cls._sb_path] if cls._sb_path else []) + \
            ["--tools_cache-master_mode", "true" if cls._tc_master else "false"] + \
            (["--tools_cache-disk_limit={}".format(cls._tc_cache_size)] if cls._tc_cache_size is not None else []) + \
            ["--tools_cache-{k}={v}".format(k=k, v=v if v else '') for k, v in six.iteritems(cls._tc_config_options)] + \
            ["--grpc-{k}={v}".format(k=k, v=v if v else '') for k, v in six.iteritems(cls._gl_config_options)] + \
            [("ALL-" if cls._single_instance else "TC-") + core.gsid.flat_session_id()]

        new_args = (["--tools_cache-sb_alt_id", cls._sb_path_id] if cls._sb_path_id else []) + \
            (["--grpc-service=all"] if cls._single_instance else ["--grpc-service=tools_cache"])

        if cls._single_instance:
            new_args += (["--ac-master_mode", "true" if cls._ac_master else "false"]) + \
                (["--ac-cache_dir", cls._bld_dir] if cls._bld_dir else []) + \
                ["--ac-{k}={v}".format(k=k, v=v if v else '') for k, v in six.iteritems(cls._ac_config_options)]

        logger.debug("run {0} with args {1}".format(cls._full_path, args + new_args))
        p = extp.popen([cls._full_path] + args + new_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        diag_output = "out={}, err={}".format(out, err)

        if p.returncode == 1:
            logger.debug("run {0} with args {1}, restart {2}".format(cls._full_path, args, diag_output))
            p = extp.popen([cls._full_path] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            cls._Report._restarts_backward += 1
            out, err = p.communicate()
            diag_output = "out={}, err={}".format(out, err)

        if p.returncode:
            logger.debug("Command failed with code=%s %s", p.returncode,  diag_output)
            raise subprocess.CalledProcessError(p.returncode, [cls._full_path] + args, output=diag_output)

    @classmethod
    def _run_ac_binary(cls):
        # Make sure tools cache is running
        out = cls.check_status()
        assert out.State == 2, str(out)

        args = ["--daemonize"] + \
            (["--ini-file", cls._ini_file] if cls._ini_file else ["--tools_cache-lock_file", cls._tc_lock_file, "--ac-lock_file", cls._ac_lock_file]) + \
            ([] if _logs_are_enabled() else ["--no-logs"]) + \
            (["--tools_cache-sb_id", cls._sb_id] if cls._sb_id else []) + \
            (["--tools_cache-sb_path", cls._sb_path] if cls._sb_path else []) + \
            (["--tools_cache-sb_alt_id", cls._sb_path_id] if cls._sb_path_id else []) + \
            (["--ac-master_mode", "true" if cls._ac_master else "false"]) + \
            (["--ac-cache_dir", cls._bld_dir] if cls._bld_dir else []) + \
            (["--ac-disk_limit={}".format(cls._ac_cache_size)] if cls._ac_cache_size is not None else []) + \
            ["--grpc-service=build_cache"] + \
            ["--ac-{k}={v}".format(k=k, v=v if v else '') for k, v in six.iteritems(cls._ac_config_options)] + \
            ["--grpc-{k}={v}".format(k=k, v=v if v else '') for k, v in six.iteritems(cls._gl_config_options)] + \
            ["AC-" + core.gsid.flat_session_id()]

        logger.debug("run {0} with args {1}".format(cls._full_path, args))
        p = extp.popen([cls._full_path] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        diag_output = "out={}, err={}".format(out, err)
        if p.returncode:
            logger.debug("Command failed with code=%s %s", p.returncode,  diag_output)
            raise subprocess.CalledProcessError(p.returncode, [cls._full_path] + args, output=diag_output)

    @classmethod
    @retry.retrying(max_times=3)
    def _fetch_ya_tc(cls):
        # other ya-tc may remove requested one
        import yalibrary.tools as tools
        return os.path.realpath(tools.tool("ya-tc", cache=False))

    @classmethod
    def _server_params(cls, thr_ident):
        if cls._done:
            return

        with cls._tc_lock:
            if cls._done:
                return

            logger.debug("Tools cache server resource download/parameters")

            import yalibrary.tools as tools
            with _with_no_tc_notification(cls, thr_ident):
                # Start from p.1.
                # Relying on yalibrary.fetcher
                cls._sb_path, cls._sb_path_id = os.path.split(tools.toolchain_root("ya-tc", toolchain_extra=None, for_platform=None))
                cls._sb_id = tools.resource_id("ya-tc", toolchain_extra=None, for_platform=None).replace('sbr:', '')
                cls._full_path = cls._fetch_ya_tc()

            logger.debug("Tools cache server resource download/parameters done: id=%s, path_id=%s, full_path=%s", cls._sb_id, cls._sb_path_id, cls._full_path)

            # Should be set in reverse order of checks to avoid races.
            cls._done = True

    @classmethod
    @retry.retrying(max_times=6, retry_sleep=lambda i, t: 1)
    def _start_with_lock(cls, thr_ident, read_address, diag, start_tc):
        """ Needs "cls._tc_lock if start_tc else cls._ac_lock" captured
            Permit at most 3 concurrent versions of server potentially replacing each other.
            Timeouts for single instance of server should be tuned elsewhere.
            Additional attempts for high load environment.
        """
        full_address = read_address()
        if full_address is not None:
            return full_address

        logger.debug("Restart service: {}".format(diag))
        if start_tc:
            cls._run_tc_binary(thr_ident)
        else:
            cls._run_ac_binary()
        full_address = read_address()
        if full_address is not None:
            return full_address

        cls._Report._restarts_failed += 1
        raise _RestartTCException("Another attempt to start {} cache service".format("tool" if start_tc else "build"))

    # cyclic dependency:
    # 1. yalibrary.tools ->
    # 2. yalibrary.fetcher ->
    # 3. yalibrary.toolscache
    @classmethod
    def _tc_cache_address(cls, thr_ident, diag, verbose=False):
        if verbose:
            logger.debug("Tools cache attempt: {}".format(diag))

        cls._server_params(thr_ident)

        cached_address = cls._tc_full_address
        if cached_address is not None:
            return cached_address

        def read_address():
            from devtools.local_cache.psingleton.python import systemptr
            full_address = systemptr.get_server_info(cls._tc_lock_file, log_file=_log_name(), non_blocking=False)
            pid, ctime, address = full_address
            if address:
                cls._tc_full_address = full_address
                logger.debug("Tools cache {} address: {}".format(diag, str(full_address)))
            return full_address if address else None

        full_address = read_address()
        if full_address is not None:
            return full_address

        with cls._tc_lock:
            return cls._start_with_lock(thr_ident, read_address, diag, start_tc=True)

    @classmethod
    def _ac_cache_address(cls, diag, verbose=False):
        if verbose:
            logger.debug("AC cache attempt: {}".format(diag))

        cached_address = cls._ac_full_address
        if cached_address is not None:
            return cached_address

        def read_address():
            from devtools.local_cache.psingleton.python import systemptr
            full_address = systemptr.get_server_info(cls._ac_lock_file, log_file=_log_name(), non_blocking=False, fresh=True)
            pid, ctime, address = full_address
            if address:
                cls._ac_full_address = full_address
                logger.debug("AC cache {} address: {}".format(diag, str(full_address)))
            return full_address if address else None

        full_address = read_address()
        if full_address is not None:
            return full_address

        with cls._ac_lock:
            return cls._start_with_lock(None, read_address, diag, start_tc=False)

    @classmethod
    def is_tc_enabled(cls, opts):
        cls._do_read_config(opts)
        return cls._tc_enable

    @classmethod
    def is_ac_enabled(cls, opts):
        cls._do_read_config(opts)
        return cls._ac_enable

    @classmethod
    def unary_method_no_retry(cls, method, diag, tc, verbose=True, ignore_error_for_tc=False, **kwargs):
        thr_ident = threading.currentThread().ident if tc else None
        if tc and thr_ident == cls._in_progress_thread:
            return

        # If ya-tc in maintenance mode, then notification requests should be disabled.
        def ignore_ya_tc_notification(pid, grpc_exc):
            if not ignore_error_for_tc:
                return False
            if grpc_exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
                return False

            resource_id = kwargs.get("sbid")
            resource_dir = kwargs.get("sbpath")
            if resource_dir != cls._sb_path:
                return False

            if resource_id not in (cls._sb_id, cls._sb_path_id):
                return False

            logger.debug("Ignore grpc error {} for ya-tc notification about itself, remote: pid={}".format(grpc_exc, pid))
            return True

        full_address = cls._tc_cache_address(thr_ident, diag, verbose=verbose) if tc else cls._ac_cache_address(diag, verbose=verbose)
        pid, ctime, address = full_address
        if address is not None:
            try:
                return method(full_address, timeout=cls._dead_line, **kwargs)
            except grpc.RpcError as e:
                if tc and ignore_ya_tc_notification(pid, e):
                    return
                if hasattr(e, 'details') and hasattr(e, 'code'):
                    logger.debug("grpc error accessing {} cache {} {}, remote: pid={}".format("TC" if tc else "AC", e.code(), e.details(), pid))
                else:
                    logger.debug("grpc error accessing {} cache {}, remote: pid={}".format("TC" if tc else "AC", e, pid))
                if tc:
                    cls._tc_full_address = None
                else:
                    cls._ac_full_address = None
                cls._Report._grpc_exceptions += 1
                raise
        else:
            msg = "Cache service not found in {} for {}".format(cls._tc_lock_file if tc else cls._ac_lock_file, diag)
            logger.debug(msg)
            raise AssertionError(msg)

    @classmethod
    def unary_method(cls, method, diag, tc, verbose=True, ignore_error_for_tc=False, ignore_exception=False, retries=4, **kwargs):
        @retry.retrying(max_times=retries, raise_exception=lambda e: not _retry_grpc(e), retry_sleep=lambda i, t: i)
        def wrapper(*args, **kwargs):
            return cls.unary_method_no_retry(*args, **kwargs)

        try:
            return wrapper(method, diag=diag, verbose=verbose, ignore_error_for_tc=ignore_error_for_tc, tc=tc, **kwargs)
        except (grpc.RpcError, subprocess.CalledProcessError, _RestartTCException):
            if not ignore_exception:
                raise

    @classmethod
    def notify_tool_cache(cls, where):
        resource_dir, resource_id = os.path.split(os.path.normpath(where).rstrip('/\\'))

        from devtools.local_cache.toolscache.python.toolscache import notify_resource_used

        kwargs = {'sbpath': resource_dir, 'sbid': resource_id}

        return cls.unary_method(notify_resource_used, diag='(request for resource {})'.format(resource_id), tc=True, ignore_error_for_tc=True, **kwargs)

    @classmethod
    def tc_force_gc(cls, max_cache_size):
        from devtools.local_cache.toolscache.python.toolscache import force_gc

        kwargs = {'disk_limit': max_cache_size}

        return cls.unary_method(force_gc, diag='(force gc tools cache {})'.format(max_cache_size), tc=True, verbose=True, **kwargs)

    @classmethod
    def lock_resource(cls, where):
        resource_dir, resource_id = os.path.split(os.path.normpath(where).rstrip('/\\'))

        from devtools.local_cache.toolscache.python.toolscache import lock_resource

        kwargs = {'sbpath': resource_dir, 'sbid': resource_id}

        return cls.unary_method(lock_resource, diag='(lock resource {})'.format(resource_id), tc=True, **kwargs)

    @classmethod
    def get_task_tc_stats(cls):
        from devtools.local_cache.toolscache.python.toolscache import get_task_stats

        return cls.unary_method(get_task_stats, diag='(request for tc stats)', tc=True, ignore_exception=True)

    @classmethod
    def check_status(cls):
        from devtools.local_cache.toolscache.python.toolscache import check_status

        return cls.unary_method(check_status, diag='(check status)', tc=True)

    @classmethod
    def unlock_all(cls):
        from devtools.local_cache.toolscache.python.toolscache import unlock_all

        return cls.unary_method(unlock_all, diag='(unlock all)', tc=True)

    @classmethod
    def has_uid(cls, uid, is_result):
        from devtools.local_cache.ac.python.ac import has_uid

        kwargs = {'uid': six.ensure_binary(uid), 'is_result': is_result}

        return cls.unary_method(has_uid, diag='(has uid {})'.format(uid), tc=False, verbose=False, **kwargs)

    @classmethod
    def get_uid(cls, uid, dest_path, hardlink, is_result, release):
        from devtools.local_cache.ac.python.ac import get_uid

        kwargs = {'uid': uid, 'dest_path': dest_path, 'hardlink': hardlink, 'is_result': is_result, 'release': release}

        return cls.unary_method(get_uid, diag='(get uid {})'.format(uid), tc=False, verbose=True, **kwargs)

    @classmethod
    def put_uid(cls, uid, root_path, blobs, weight, hardlink, replace, is_result, file_names):
        from devtools.local_cache.ac.python.ac import put_uid

        kwargs = {'uid': uid, 'root_path': root_path, 'blobs': blobs, 'weight': weight, 'hardlink': hardlink, 'replace': replace, 'is_result': is_result, 'file_names': file_names}

        return cls.unary_method(put_uid, diag='(put uid {})'.format(uid), tc=False, verbose=True, **kwargs)

    @classmethod
    def remove_uid(cls, uid, forced_removal):
        from devtools.local_cache.ac.python.ac import remove_uid

        kwargs = {'uid': uid, 'forced_removal': forced_removal}

        return cls.unary_method(remove_uid, diag='(remove uid {})'.format(uid), tc=False, **kwargs)

    @classmethod
    def get_task_ac_stats(cls):
        from devtools.local_cache.ac.python.ac import get_task_stats

        return cls.unary_method(get_task_stats, diag='(request for ac stats)', tc=False, ignore_exception=True)

    @classmethod
    def put_dependencies(cls, uid, deps):
        from devtools.local_cache.ac.python.ac import put_dependencies

        kwargs = {'uid': uid, 'deps': deps}

        return cls.unary_method(put_dependencies, diag='(graph info {})'.format(uid), tc=False, verbose=False, **kwargs)

    @classmethod
    def ac_force_gc(cls, max_cache_size):
        from devtools.local_cache.ac.python.ac import force_gc

        kwargs = {'disk_limit': max_cache_size}

        return cls.unary_method(force_gc, diag='(force gc {})'.format(max_cache_size), tc=False, verbose=True, **kwargs)

    @classmethod
    def release_all(cls):
        from devtools.local_cache.ac.python.ac import release_all

        return cls.unary_method(release_all, diag='(release all data)', tc=False, verbose=True, ignore_exception=True)

    @classmethod
    def analyze_ac(cls):
        from devtools.local_cache.ac.python.ac import analyze_du

        return cls.unary_method(analyze_du, diag='(analyze disk usage)', tc=False, verbose=True, ignore_exception=True)

    @classmethod
    def synchronous_gc(cls, total_size=None, min_last_access=None, max_object_size=None):
        from devtools.local_cache.ac.python.ac import synchronous_gc

        kwargs = {'total_size': total_size, 'min_last_access': min_last_access, 'max_object_size': max_object_size}

        return cls.unary_method(synchronous_gc, diag='(synchronous gc {})'.format(kwargs), tc=False, verbose=True, **kwargs)


_SERVER = _Server()


def toolscache_enabled(opts=None):
    if not supported():
        return False
    return _SERVER.is_tc_enabled(opts)


def buildcache_enabled(opts=None):
    if not supported():
        return False
    return _SERVER.is_ac_enabled(opts)


def toolscache_version(opts=None):
    if not toolscache_enabled(opts):
        return None
    return _SERVER._tool_version


def notify_tool_cache(where):
    if not toolscache_enabled():
        return None

    out = _SERVER.notify_tool_cache(where)
    logger.debug("tc cache stats: {}".format(out))
    if os.path.islink(where):
        try:
            target = os.readlink(where)
        except EnvironmentError:
            return
        _SERVER.notify_tool_cache(target)


def tc_force_gc(max_cache_size):
    if not toolscache_enabled():
        return None

    return _SERVER.tc_force_gc(max_cache_size)


def lock_resource(where):
    if not toolscache_enabled():
        return None

    _SERVER.lock_resource(where)
    if os.path.islink(where):
        try:
            target = os.readlink(where)
        except EnvironmentError:
            return
        _SERVER.lock_resource(target)


def unlock_all():
    if not toolscache_enabled():
        return None

    _SERVER.unlock_all()


def get_task_stats():
    out_stats = {}
    if toolscache_enabled():
        out = _SERVER.get_task_tc_stats()
        logger.debug("tc stats: {}".format(out))
        if hasattr(out, 'TotalKnownSize'):
            out_stats['tools_disk_usage'] = out.TotalKnownSize
        if hasattr(out, 'NonComputedCount'):
            out_stats['tools_unknown_disk_usage'] = out.NonComputedCount

    if buildcache_enabled():
        out = _SERVER.get_task_ac_stats()
        logger.debug("ac stats: {}".format(out))
        if hasattr(out, 'TotalFSSize'):
            out_stats['build_cache_disk_usage'] = out.TotalFSSize
        if hasattr(out, 'TotalSize'):
            out_stats['build_cache_apparent_disk_usage'] = out.TotalSize

    return out_stats


def post_local_cache_report():
    if supported() and _Server._is_user_build():
        core.report.report('ya_tc_stats', _SERVER._Report.get_stat_dict())


def release_all_data():
    if buildcache_enabled():
        _SERVER.release_all()


class ACCache(object):
    def __init__(self, store_path):
        self._fs_size = 0
        self._size = 0
        self._blob_count = 0
        self._uid_count = 0
        self._store_path = store_path
        self._max_age = None

        self.timers = {'has': 0, 'put': 0, 'get': 0, 'remove': 0, 'deps': 0}
        self.counters = {'has': 0, 'put': 0, 'get': 0, 'remove': 0, 'deps': 0}
        self.failures = {'has': 0, 'put': 0, 'get': 0, 'remove': 0, 'deps': 0}

    def _set_stats(self, stats):
        self._fs_size = stats.TotalFSSize + stats.TotalDBSize
        self._size = stats.TotalSize
        self._blob_count = stats.BlobCount
        self._uid_count = stats.UidCount

    def _inc_time(self, x, tag):
        self.timers[tag] += x
        self.counters[tag] += 1

    def _acccount_failure(self, tag, r, metadata=None, count_failure=True):
        if r and r.Success:
            return True

        if r and metadata:
            for key, value in metadata:
                if key == 'io-exception':
                    logger.warning("IO error in local cache: %s", value)

        if count_failure:
            self.failures[tag] += 1
        return False

    def put(self, uid, root_path, files, codec=None, weight=0, hardlink=True, replace=True, is_result=False):
        # Compute blob hashes on cache side.
        with AccumulateTime(lambda x: self._inc_time(x, 'put')):
            # Make sure files are unique
            blobs = list(set((x, None) for x in files))
            r, metadata = [None] * 2
            out = _SERVER.put_uid(uid, root_path, blobs, weight, hardlink, replace, is_result, file_names=None)
            if out:
                r, metadata = out
                self._set_stats(r.Stats)
        return self._acccount_failure('put', r, metadata=metadata)

    def has(self, uid, is_result=False):
        with AccumulateTime(lambda x: self._inc_time(x, 'has')):
            r = _SERVER.has_uid(uid, is_result)
            if r:
                self._set_stats(r.Stats)
        return self._acccount_failure('has', r, count_failure=False)

    def try_restore(self, uid, dest_path, hardlink=True, is_result=False, release=True):
        with AccumulateTime(lambda x: self._inc_time(x, 'get')):
            r, metadata = [None] * 2
            out = _SERVER.get_uid(uid, dest_path, hardlink, is_result, release)
            if out:
                r, metadata = out
                self._set_stats(r.Stats)
        return self._acccount_failure('get', r, metadata=metadata)

    def clear_uid(self, uid, forced_removal=False):
        with AccumulateTime(lambda x: self._inc_time(x, 'remove')):
            r = _SERVER.remove_uid(uid, forced_removal)
            if r:
                self._set_stats(r.Stats)
        return self._acccount_failure('remove', r)

    def put_dependencies(self, uid, deps):
        with AccumulateTime(lambda x: self._inc_time(x, 'deps')):
            r = _SERVER.put_dependencies(uid, deps)
            if r:
                self._set_stats(r.Stats)
        return self._acccount_failure('deps', r)

    def size(self):
        return self._fs_size

    def compact(self, interval, max_cache_size, state):
        if self._max_age is not None:
            r = _SERVER.synchronous_gc(min_last_access=self._max_age * 1000)
            max_cache_size = min(8223372036854775808, long(max_cache_size) if six.PY2 else int(max_cache_size))  # noqa
        else:
            r = _SERVER.ac_force_gc(max_cache_size)
        if r:
            self._set_stats(r)

        new_store_path = os.path.join(os.path.dirname(self._store_path), '6')
        if not os.path.exists(new_store_path):
            return

        import hashlib

        try:
            def convert(uid, info):
                blobs = []
                file_names = []
                for rel_path, info in six.iteritems(info):
                    store_path, mode, fhash = info
                    if not os.path.exists(store_path):
                        return

                    sha = hashlib.sha1()
                    sha.update('{fhash}mode: {hexmode}'.format(fhash=fhash, hexmode=hex(mode)))
                    blobs.append((store_path, sha.hexdigest()))

                    file_names.append(rel_path)

                r, metadata = [None] * 2
                out = _SERVER.put_uid(uid, root_path='', blobs=blobs, weight=0, hardlink=True, replace=True, is_result=False, file_names=file_names)
                if out:
                    r, metadata = out
                    self._set_stats(r.Stats)

                self._acccount_failure('put', r, metadata=metadata)

            from yalibrary.store import new_store
            new_store.NewStore(new_store_path).convert(convert, state)
        except Exception as e:
            logger.debug("Did not complete conversion for %s", e)

    def set_max_age(self, max_age):
        self._max_age = max_age

    def sieve(self, stopper, state):
        pass

    def analyze(self, display):
        r = _SERVER.analyze_ac()
        if not r:
            return

        self._set_stats(r.Stats)

        def __fmt_size(size):
            for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
                if size < 1024.0:
                    return '{:8.3f}{}'.format(size, unit)
                size /= 1024.0

            return '{:8.3f}{}'.format(size, unit)

        display.emit_message('Total disk size: {:10}, # of files: {:7}, # of graph nodes: {:7}'.format(__fmt_size(self._fs_size), self._blob_count, self._uid_count))
        display.emit_message('')
        display.emit_message('{:10} {:5} - {}'.format('Disk size', 'freq', 'file path'))
        for item in r.FileStats:
            size = max(item.Size, item.FSSize)
            display.emit_message('{:10} {:5} - {}'.format(__fmt_size(size), item.Freq, item.Path))

    def clear_tray(self):
        pass

    def strip_total_size(self, total_size):
        r = _SERVER.synchronous_gc(total_size=total_size)
        if r:
            self._set_stats(r)

    def strip_max_object_size(self, max_object_size):
        r = _SERVER.synchronous_gc(max_object_size=max_object_size)
        if r:
            self._set_stats(r)

    def strip_max_age(self, max_age):
        r = _SERVER.synchronous_gc(min_last_access=(time.time() - max_age) * 1000)
        if r:
            self._set_stats(r)

    def flush(self):
        pass

    def stats(self, execution_log):
        for k in self.timers.keys():
            stat_dict = {'timing': (0, self.timers[k]), 'total_time': True, 'count': self.counters[k], 'prepare': '', 'type': 'AC', 'failures': self.failures[k]}
            core.report.report('AC_stats-{}'.format(k), stat_dict)
            execution_log["$(AC-{})".format(k)] = stat_dict
