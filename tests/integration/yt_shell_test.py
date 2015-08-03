import os
import subprocess

from contextlib import contextmanager

from yt.environment import YTEnv
from yt_env_setup import resolve_test_paths

import pytest


def sh(command):
    return subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()[0]


class ExecutableItem(pytest.Item):
    def __init__(self, parent):
        assert parent.fspath is not None
        name = parent.fspath.basename.rsplit('.', 1)[0]
        super(ExecutableItem, self).__init__(name, parent)
        self.sandbox_path, self.environment_path = resolve_test_paths(name)
        self.pids_file = os.path.join(self.environment_path, 'pids.txt')

    @staticmethod
    def extract_attrs(path):
        with open(path, 'rt') as handle:
            for line in handle:
                if not line.startswith('#'):
                    break
                if line.startswith('#%'):
                    line = line.lstrip('#%').rstrip('\r\n').replace(' ', '')
                    try:
                        key, value = line.split('=', 2)
                        yield key, eval(value)
                    except ValueError:
                        print '%s: Unable to interpret line "%s"' % \
                            (path, line)

    def runtest(self):
        print ''
        print 'Running', self.name, 'from', self.fspath
        print 'Sandbox path: ' + self.sandbox_path
        print 'Environment path: ' + self.environment_path

        class CurrentYTEnvironment(YTEnv):
            pass

        for key, value in self.extract_attrs(str(self.fspath)):
            print 'Setting "%s" to "%s"' % (key, value)
            setattr(CurrentYTEnvironment, key, value)

        env = CurrentYTEnvironment()
        try:
            env.start(self.environment_path, self.pids_file)
            self.on_runtest(env)
        finally:
            env.clear_environment()

    def reportinfo(self):
        return self.fspath, 0, '%s: %s (%s)' % \
            (self.__class__.__name__, self.name, self.fspath)


class ShellItem(ExecutableItem):
    EXTS = ['.stdout', '.stderr']

    def on_runtest(self, env):
        stdout_actual, stderr_actual = \
            [os.path.join(self.sandbox_path, self.name) + e for e in self.EXTS]
        stdout_expected, stderr_expected = \
            [str(self.fspath)[:-3] + e for e in self.EXTS]

        os.system("touch {stdout_expected}".format(**vars()))
        os.system("touch {stderr_expected}".format(**vars()))

        the_sandbox = self.sandbox_path
        str(the_sandbox)  # Shut flake8.
        the_script = str(self.fspath)
        str(the_script)  # Shut flake8.
        exit_code = os.system("cd {the_sandbox} && {the_script} >{stdout_actual} 2> {stderr_actual}".format(**vars()))

        stdout_diff = sh(["diff", "-ui", stdout_actual, stdout_expected])
        stderr_diff = sh(["diff", "-ui", stderr_actual, stderr_expected])

        if stdout_diff or stderr_diff or exit_code != 0:
            raise ShellException(
                "{script_path} finished with errors".format(**vars()),
                exit_code,
                stdout_diff,
                stderr_diff)

    def repr_failure(self, excinfo):
        exc = excinfo.value
        if isinstance(exc, ShellException):
            return "\n".join([
                'shell test execution failed (exit code %s)' % exc.exit_code,
                '-' * 80,
                'stdout diff:',
                exc.stdout_diff,
                '-' * 80,
                'stderr diff:',
                exc.stderr_diff,
                '-' * 80
            ])
        else:
            return super(ShellItem, self).repr_failure(excinfo)


class ShellException(Exception):
    def __init__(self, message, exit_code, stdout_diff, stderr_diff):
        Exception.__init__(self, message)
        self.exit_code = exit_code
        self.stdout_diff = stdout_diff
        self.stderr_diff = stderr_diff


class ShellFile(pytest.File):
    def collect(self):
        yield ShellItem(self)


class PerlItem(ExecutableItem):
    def on_runtest(self, env):
        # XXX(sandello): This is a hacky way to set proper include path.
        inc = os.path.abspath(
            os.path.join(os.path.dirname(str(self.fspath)), ".."))
        child = subprocess.Popen(
            ["perl", "-I" + inc, str(self.fspath)],
            cwd=self.sandbox_path,
            env={"YT_MASTERS": ",".join(env.get_master_addresses())},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = child.communicate()
        child.wait()

        if child.returncode != 0:
            raise PerlException(
                self.name,
                child.returncode,
                stdout,
                stderr)

    def repr_failure(self, excinfo):
        exc = excinfo.value
        if isinstance(exc, PerlException):
            return "\n".join([
                str(exc),
                "-- STDOUT " + "-" * 70,
                exc.stdout,
                "-- STDERR " + "-" * 70,
                exc.stderr,
                "-" * 80
            ])
        return super(PerlItem, self).repr_failure(excinfo)

    def reportinfo(self):
        return self.fspath, 0, "perl:%s" % self.name


class PerlFile(pytest.File):
    def collect(self):
        yield PerlItem(self)


class PerlException(Exception):
    def __init__(self, name, exit_code, stdout, stderr):
        Exception.__init__(
            self,
            "Perl test '%s' has failed (exit code %s)" % (name, exit_code))
        self.name = name
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
