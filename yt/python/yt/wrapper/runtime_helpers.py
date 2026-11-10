import atexit
import importlib.util
import logging
import sys

from collections import namedtuple

import yt.logger as logger

from yt.wrapper.common import is_arcadia_python


_ENV_DEBUG_INFO = namedtuple("_ENV_DEBUG_INFO", [
    "is_arcadia", "wrapper_version", "python_major", "python_minor", "python_micro", "executable", "yson_info", "rpc_info",
])


def _get_env_debug_info():
    try:
        import yt.yson as yson
        from .common import get_version

        if yson.TYPE == "BINARY":
            import yt_yson_bindings
            yson_bindings_info = getattr(yt_yson_bindings, "VERSION", "unknown")
            if yson_bindings_info == "unknown":
                if is_arcadia_python():
                    yson_bindings_info = get_version()
                else:
                    yson_bindings_info = "loaded (unknown version)"
        else:
            yson_bindings_info = "none (use native python)"

        if "yt_driver_rpc_bindings" in sys.modules:
            try:
                import yt_driver_rpc_bindings.version
                rpc_bindings_info = yt_driver_rpc_bindings.version.VERSION
            except Exception:
                if is_arcadia_python():
                    rpc_bindings_info = get_version()
                else:
                    rpc_bindings_info = "loaded (unknown version)"
        elif importlib.util.find_spec("yt_driver_rpc_bindings") is not None:
            rpc_bindings_info = "installed (not loaded)"
        else:
            rpc_bindings_info = "none"

        return _ENV_DEBUG_INFO(
            is_arcadia=is_arcadia_python(),
            wrapper_version=get_version(),
            python_major=sys.version_info.major,
            python_minor=sys.version_info.minor,
            python_micro=sys.version_info.micro,
            executable=sys.executable,
            yson_info=yson_bindings_info,
            rpc_info=rpc_bindings_info,
        )
    except Exception:
        return None


def _log_welcome_debug_info():
    env_info = _get_env_debug_info()
    if env_info:
        logger.debug(
            "YT wrapper version: %s; python: %s.%s.%s (%s%s); yson bindings: %s; rpc bindings: %s",
            env_info.wrapper_version,
            env_info.python_major,
            env_info.python_minor,
            env_info.python_micro,
            "Arcadia, " if env_info.is_arcadia else "",
            env_info.executable,
            env_info.yson_info,
            env_info.rpc_info,
        )
        return True
    else:
        return False


def _log_error_debug_info():
    env_info = _get_env_debug_info()
    if env_info:
        logger.error(
            "YT wrapper version: %s; python: %s.%s.%s (%s%s); yson bindings: %s; rpc bindings: %s",
            env_info.wrapper_version,
            env_info.python_major,
            env_info.python_minor,
            env_info.python_micro,
            "Arcadia, " if env_info.is_arcadia else "",
            env_info.executable,
            env_info.yson_info,
            env_info.rpc_info,
        )
        return True
    else:
        return False


_process_exiting_with_error = False
_original_excepthook = sys.excepthook


def mark_process_exiting_with_error():
    global _process_exiting_with_error
    _process_exiting_with_error = True


def _mark_uncaught_exception(exc_type, exc_value, traceback):
    mark_process_exiting_with_error()
    _original_excepthook(exc_type, exc_value, traceback)


def _log_error_debug_info_on_exit():
    if _process_exiting_with_error:
        _log_error_debug_info()


sys.excepthook = _mark_uncaught_exception
atexit.register(_log_error_debug_info_on_exit)


logger.add_before_first_log_callback(logging.DEBUG, _log_welcome_debug_info)
