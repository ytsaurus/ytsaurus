# coding: utf-8

import time
import subprocess

import exts.process
import exts.windows
import yatest.common


def child_haldlers_count():
    res = yatest.common.execute(yatest.common.binary_path("devtools/ya/exts/tests/data/data"), check_exit_code=False)
    count = res.std_out.strip()
    if not count.isdigit():
        raise Exception(res.std_out)
    count = int(count)
    assert count >= 3
    return count


def test_set_close_on_exec():
    afile = open("file.txt", "w")

    import fcntl

    flags = fcntl.fcntl(afile.fileno(), fcntl.F_GETFD)
    flags &= ~fcntl.FD_CLOEXEC
    fcntl.fcntl(afile.fileno(), fcntl.F_SETFD, flags)
    count = child_haldlers_count()
    exts.process.set_close_on_exec(afile.fileno())

    assert child_haldlers_count() < count


def test_wait_for_proc():
    oneliner = 'import time, sys; sys.stderr.write("123"); time.sleep({delay}); sys.stderr.write("321");'
    proc = subprocess.Popen([yatest.common.python_path(), '-c', oneliner.format(delay=1)], stderr=subprocess.PIPE)
    started = time.time()
    out, err = exts.process.wait_for_proc(proc, 10)
    assert (time.time() - started) < 8
    assert err.decode() == '123321'
    assert proc.returncode == 0

    proc = subprocess.Popen([yatest.common.python_path(), '-c', oneliner.format(delay=10)], stderr=subprocess.PIPE)
    started = time.time()
    try:
        exts.process.wait_for_proc(proc, 1)
    except exts.process.TimeoutExpired as e:
        assert e.stderr.decode() == '123'
    else:
        raise Exception('expected TimeoutExpired to be raised')
    assert (time.time() - started) < 3
    assert proc.returncode != 0
