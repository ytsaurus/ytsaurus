import signal
import subprocess

import yatest.common as yc

import os
import copy
import six
import exts
import tempfile

from core import config
from test.system import process


# Copypaste from devtools/ya/test/tests/lib/common/__init__.py,
# for some reason can't fix inheritance of DDEBUGINFO_LINES_ONLY using params.
def run_ya(
        command,
        build_root=None, cwd=None, use_ya_bin=True, ya_cache_dir=None,
        env=None, stdin=None, keep_env_keys=None, verbose=True,
        restart_on_error=True, no_respawn=False, fake_root=False, source_root=None, use_ymake_bin=False, strace=False,
        timeout=None, stderr=None, encoding=None
):

    path_to_ya_bin = os.path.join('devtools', 'ya', 'bin', 'ya-bin')
    ya = yc.binary_path(path_to_ya_bin)

    if not isinstance(command, list):
        command = [command]

    if not isinstance(ya, list):
        ya = [ya]

    cmd = ya
    cmd += command
    cwd = cwd or os.getcwd()
    keep_env_keys = copy.deepcopy(keep_env_keys) or []
    keep_env_keys.extend(['YA_CUSTOM_FETCHER', 'YA_TOKEN', 'YA_TOKEN_PATH', 'YA_TEST_CONFIG_PATH'])
    if not env:
        env = os.environ.copy()
    for key in list(six.viewkeys(env)):
        if key.startswith("YA") and key not in keep_env_keys:
            del env[key]

    default_tool_root = os.path.dirname(config.tool_root())
    if "YA_CACHE_DIR_TOOLS" not in env and os.path.exists(default_tool_root):
        env["YA_CACHE_DIR_TOOLS"] = default_tool_root

    ya_temp_cache_dir = None
    if not ya_cache_dir and not exts.windows.on_win():
        ya_temp_cache_dir = tempfile.mkdtemp()
        os.chmod(ya_temp_cache_dir, 0o755)
        ya_cache_dir = ya_temp_cache_dir
        env['YA_NO_LOGS'] = '1'
    if ya_cache_dir:
        env["YA_CACHE_DIR"] = ya_cache_dir

    env["YA_LOAD_USER_CONF"] = "0"
    env["ANSI_COLORS_DISABLED"] = "1"
    if "DO_NOT_USE_LOCAL_CONF" in env:
        del env["DO_NOT_USE_LOCAL_CONF"]

    if no_respawn and "YA_NO_RESPAWN" not in env:
        env["YA_NO_RESPAWN"] = "1"
        keep_env_keys.append("YA_NO_RESPAWN")

    if source_root:
        env["YA_SOURCE_ROOT"] = source_root
        keep_env_keys.append("YA_SOURCE_ROOT")

    if 'YA_TC' not in env:
        env['YA_TC'] = '1'
    if 'YA_AC' not in env:
        env['YA_AC'] = '1'

    env['YA_YT_STORE4'] = '0'

    cmd += ["--ymake-bin", yc.binary_path('devtools/ymake/bin/ymake')]

    cmd = list(map(str, cmd))

    process.execute(
        cmd, cwd=cwd, env=env, timeout=timeout,
    )


gdbpath = yc.gdb_path()
gdbinit = yc.source_path('devtools/gdb/__init__.py')
gdbtest = os.path.join(yc.source_path(), 'yt/yt/tests/integration/compat/print-yt-fibers-test/gdbtest')

signal.signal(signal.SIGTTOU, signal.SIG_IGN)


def build_exe():
    global gdbtest
    run_ya(
        command=[
            'make',
            '--force-build-depends',
            # '--rebuild',
            '-DDEBUGINFO_LINES_ONLY=no',
            '--build', 'debug'
        ],
        cwd=os.path.join(yc.source_path(), gdbtest),
        env=[],
        use_ymake_bin=True,
        keep_env_keys=[],
        restart_on_error=False,
        no_respawn=True,
        fake_root=True,
    )
    gdbtest = os.path.join(gdbtest, 'gdbtest')


def copy_exe():
    try:
        subprocess.check_output(['cp', gdbtest, os.path.join(yc.output_path(), "executable")], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print('Failed to copy exe\n{}'.format(exc.output.decode('UTF-8')))


def gdb(*commands):
    cmd = [gdbpath, '-nx', '-batch', '-ix', gdbinit, '-ex', 'set charset UTF-8']
    for c in commands:
        cmd += ['-ex', c]
    cmd += [gdbtest]
    env = os.environ.copy()
    # strings are not printed correctly in gdb overwise.
    env['LC_ALL'] = 'en_US.UTF-8'
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    except subprocess.CalledProcessError as exc:
        with open(os.path.join(yc.output_path(), "gdb-errlog.txt"), "w") as f_p:
            f_p.write('gdb return code: {}\n'.format(exc.returncode))
            f_p.write(exc.output.decode('UTF-8'))
            f_p.close()
        copy_exe()
        raise
    return out


def print_fibers():
    return gdb('b StopHere', 'run', 'print_yt_fibers')


def test_fibers_printer():
    build_exe()
    actual_output = print_fibers().decode('UTF-8')
    with open(os.path.join(yc.output_path(), "actual_output.txt"), "w") as f_p:
        f_p.write(actual_output)
        f_p.close()
    assert len(actual_output) > 0
    fibers_count = 0
    foo_count = 0
    bar_count = 0
    async_stop_count = 0
    for line in actual_output.split('\n'):
        if line.find('0x0000000000000000 in ?? ()') != -1:
            fibers_count += 1
        if line.find('Foo') != -1:
            foo_count += 1
        if line.find('Bar') != -1:
            bar_count += 1
        if line.find('AsyncStop') != -1:
            async_stop_count += 1
    assert fibers_count == 3
    assert foo_count == 6
    assert bar_count == 5
    assert async_stop_count == 1
