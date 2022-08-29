import signal
import subprocess

import yatest.common as yc

import os


gdbpath = yc.gdb_path()
gdbinit = yc.source_path('devtools/gdb/__init__.py')
gdbtest = yc.binary_path('yt/yt/tests/integration/yt_fibers_printer/gdbtest/gdbtest')

signal.signal(signal.SIGTTOU, signal.SIG_IGN)


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
    assert fibers_count >= 2
    assert foo_count == 6
    assert bar_count == 5
    assert async_stop_count == 1
