import logging
import os
import sys

import yalibrary.find_root
import exts.process

import core.config

RESPAWNS_PARAM = 'YA_RESPAWNS'
YA_PYVER_REQUIRE = "YA_PYVER_REQUIRE"  # Required python version from ya script
YA_PYVER_SET_FORCED = "YA_PYVER_SET_FORCED"  # ya script was called with flag `-2` or `-3`

# If you enable this, respawn to ya2 will add `-2` to command line for ya script
# and can break backward compatibility with ya scripts older than https://a.yandex-team.ru/arcadia/commit/r10186426
SPECIFY_YA_2_WHEN_RESPAWN = False

logger = logging.getLogger(__name__)
logger_pyver = logging.getLogger(__name__ + ".pyver")


class IncompatiblePythonMajorVersion(Exception):
    pass


def _get_current_respawns():
    return [_f for _f in os.environ.get(RESPAWNS_PARAM, "").split(os.pathsep) if _f]


def _create_respawn_env(env, respawns):
    if respawns:
        env[RESPAWNS_PARAM] = os.pathsep.join(respawns)
    return env


def _get_new_respawns(reasons):
    return [r for r in reasons if r and r not in _get_current_respawns()]


def _src_root_respawn(arc_dir, handler_python_major_version=None):
    if os.environ.get('YA_NO_RESPAWN'):
        logger.debug('Skipping respawn by YA_NO_RESPAWN variable')
        return

    py_path = sys.executable
    target = None
    for t in [os.path.join(arc_dir, 'ya'), os.path.join(arc_dir, 'devtools', 'ya', 'ya')]:
        if os.path.exists(t):
            target = t
            break

    if target is None:
        raise Exception('Cannot find ya in ' + arc_dir)

    reasons = [target]

    # Check here for respawn

    reason_py3, expected_ya_bin_python_version, ya_script_python_force = check_py23_respawn_reason(handler_python_major_version)

    if reason_py3:
        reasons.append(reason_py3)

    new_respawns = _get_new_respawns(reasons)
    if not new_respawns:
        logger.debug("New reasons for respawn not found, skip")
        return

    # Specify ya-bin version for ya-bin3
    ya_script_prefix = []

    if (
        (SPECIFY_YA_2_WHEN_RESPAWN and expected_ya_bin_python_version == 2)  # Specify `ya -2` when respawn
        or expected_ya_bin_python_version == 3  # Respawn to ya-bin3 required somehow
        or ya_script_python_force  # `ya` was called with `-2`/`-3` flag
    ):
        ya_script_prefix.append("-{}".format(expected_ya_bin_python_version))
        logger_pyver.info("Respawn to ya-bin%d", expected_ya_bin_python_version)

    cmd = [target] + ya_script_prefix + sys.argv[1:]

    env = _create_respawn_env(os.environ.copy(), _get_current_respawns() + new_respawns)
    env['PY_IGNORE_ENVIRONMENT'] = 'x'
    env['YA_SOURCE_ROOT'] = arc_dir
    env['Y_PYTHON_ENTRY_POINT'] = ':main'

    # -E     : ignore PYTHON* environment variables (such as PYTHONPATH)
    # -s     : don't add user site directory to sys.path; also PYTHONNOUSERSITE
    # -S     : don't imply 'import site' on initialization
    full_cmd = ["-E", "-s", "-S"] + cmd
    logger.debug('Respawn %s %s (triggered by: %s)', py_path, ' '.join(full_cmd), new_respawns)

    exts.process.execve(py_path, full_cmd, env)


class EmptyValue:
    pass


def check_py23_respawn_reason(handler_pyver, _current_ya_bin_version=EmptyValue, _ya_major_version=EmptyValue, _ya_version_force=EmptyValue):
    """ For more info check https://st.yandex-team.ru/YA-255 """
    reason = None

    ya_pyver_require = _get_ya_pyver_require() if _ya_major_version is EmptyValue else _ya_major_version
    ya_pyver_set_forced = _get_ya_pyver_forced() if _ya_version_force is EmptyValue else _ya_version_force
    ya_bin_version = sys.version_info.major if _current_ya_bin_version is EmptyValue else _current_ya_bin_version
    handler_pyver = int(handler_pyver or ya_bin_version)

    if ya_pyver_require is None:
        logger.debug("Detecting directly run ya-bin")

    logger_pyver.info(
        "Checking respawn necessity with major python versions: "
        "ya script require: `%s`; ya set force: `%s`, current ya-bin: `%s`; handler: `%s`;",
        ya_pyver_require, ya_pyver_set_forced, ya_bin_version, handler_pyver
    )

    expected_ya_bin_python_version = ya_bin_version

    # We must respawn from here only when:
    # ya_pyver_require == 23 (default, ya script don't know, which version need)
    # current ya-bin python version != handler version

    if ya_pyver_require is not None:
        # Running ya-bin from ya script

        if ya_pyver_require == 23:
            # ya script don't know exact version of handler
            if ya_bin_version != handler_pyver:
                reason = "ya{}handler{}{}".format(
                    ya_bin_version, handler_pyver, '_force' if ya_pyver_set_forced else '',
                )
                expected_ya_bin_python_version = handler_pyver
            # Nothing to do here
        elif ya_pyver_require != ya_bin_version:
            # Something wrong happens in ya script
            raise IncompatiblePythonMajorVersion(
                "Incompatible required and ya-bin python major version: `{}` vs `{}`".format(
                    ya_pyver_require, ya_bin_version
                ))
    # Nothing to do when we run `ya-bin/3` directly (`ya_pyver_require is None`)

    if ya_bin_version != handler_pyver:
        logger_pyver.info("Incompatible ya-bin and handler python major version: `{}` vs `{}`".format(
            ya_bin_version, handler_pyver
        ))

    return reason, expected_ya_bin_python_version, ya_pyver_set_forced


def _get_ya_pyver_require():
    version = os.environ.get(YA_PYVER_REQUIRE, None)

    return int(version) if version is not None else None


def _get_ya_pyver_forced():
    return os.environ.get(YA_PYVER_SET_FORCED, None) == 'yes'


def check_for_respawn(new_root, handler_python_major_version=None):
    prev_root = core.config.find_root(fail_on_error=False)

    root_change = prev_root != new_root
    reason_py3, expected_ya_bin_python_version, ya_pyver_set_forced = check_py23_respawn_reason(handler_python_major_version)

    if not (root_change or reason_py3):
        logger.debug('Same as prev source root %s', new_root)
        logger_pyver.debug("No need to respawn to other ya-bin version")
        return

    if root_change:
        logger.debug('New source root %s is not the same as prev %s', new_root, prev_root)
    if reason_py3:
        logger_pyver.debug(
            "Expected ya-bin version different from current: expect `%s`, current is `%s`%s, will be respawned ",
            expected_ya_bin_python_version,
            ' setted force' if ya_pyver_set_forced else '',
            sys.version_info.major)

    entry_root = yalibrary.find_root.detect_root(core.config.entry_point_path())

    entry_root_change = entry_root != new_root

    if entry_root_change or reason_py3:
        if root_change and handler_python_major_version:
            logger_pyver.info(
                "Disable python version (%d) setting for handler because of arcadia root changed (`%s` vs `%s`)",
                handler_python_major_version, prev_root, new_root)
            handler_python_major_version = None
        _src_root_respawn(arc_dir=new_root, handler_python_major_version=handler_python_major_version)

    # It's needed only when a respawn happens.
    if 'YA_STDIN' in os.environ:
        del os.environ['YA_STDIN']


def filter_env(env_vars):
    copy_vars = dict(env_vars)
    copy_vars.pop(RESPAWNS_PARAM, None)
    return copy_vars
