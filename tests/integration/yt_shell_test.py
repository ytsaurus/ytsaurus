import os
import subprocess

from yt.environment import YTInstance
from yt_env_setup import resolve_test_paths

import pytest


class ExecutableItem(pytest.Item):
    def __init__(self, parent):
        assert parent.fspath is not None
        name = parent.fspath.basename.rsplit('.', 1)[0]
        super(ExecutableItem, self).__init__(name, parent)
        self.sandbox_path, self.environment_path = resolve_test_paths(name)
        self.pids_file = os.path.join(self.environment_path, 'pids.txt')

    def extract_attrs(self, path):
        with open(path, 'rt') as handle:
            for line in handle:
                if not line.startswith(self.comment_line_begin):
                    break
                config_mark = self.comment_line_begin + '%'
                if line.startswith(config_mark):
                    line = line.lstrip(config_mark).rstrip('\r\n').replace(' ', '')
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

        params_map = {
            "NUM_MASTERS": "master_count",
            "NUM_SCHEDULERS": "scheduler_count",
            "NUM_NODES": "node_count"
        }

        kwargs = {}
        for key, value in self.extract_attrs(str(self.fspath)):
            print 'Setting "%s" to "%s"' % (key, value)
            kwargs[params_map[key]] = value

        env = YTInstance(self.environment_path, **kwargs)
        try:
            env.start()
            self.on_runtest(env)
        finally:
            env.stop()

    def repr_failure(self, excinfo):
        exc = excinfo.value
        if isinstance(exc, YtShellTestException):
            return exc.repr_failure()
        return super(ExecutableItem, self).repr_failure(excinfo)

    def reportinfo(self):
        return self.fspath, 0, '%s: %s (%s)' % \
            (self.__class__.__name__, self.name, self.fspath)


class YtShellTestException(Exception):
    def __init__(self, lang, name, exit_code, stdout=None, stderr=None):
        Exception.__init__(
            self,
            "%s test '%s' has failed (exit code %s)" % (lang, name, exit_code))
        self.name = name
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

    def repr_failure(self):
        out = [str(self)]
        if self.stdout is not None:
            out += ["-- STDOUT " + "-" * 70, self.stdout]
        if self.stderr is not None:
            out += ["-- STDERR " + "-" * 70, self.stdout]
        if len(out) > 1:
            out += ["-" * 80]
        return "\n".join(out)


class PerlItem(ExecutableItem):
    def __init__(self, parent):
        super(PerlItem, self).__init__(parent)
        self.comment_line_begin = "#"

    def on_runtest(self, env):
        # XXX(sandello): This is a hacky way to set proper include path.
        inc = os.path.abspath(
            os.path.join(os.path.dirname(str(self.fspath)), ".."))

        environment = {}
        if env.master_count > 0:
            environment["YT_DRIVER_CONFIG_PATH"] = env.config_paths["driver"]

        child = subprocess.Popen(
            ["perl", "-I" + inc, str(self.fspath)],
            cwd=self.sandbox_path,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = child.communicate()
        child.wait()

        if child.returncode != 0:
            raise YtShellTestException(
                "Perl",
                self.name,
                child.returncode,
                stdout,
                stderr)

    def reportinfo(self):
        return self.fspath, 0, "perl:%s" % self.name


class CppItem(ExecutableItem):
    def __init__(self, parent):
        super(CppItem, self).__init__(parent)
        self.comment_line_begin = "//"

    def on_runtest(self, env):
        environment = {}
        environment["PATH"] = os.environ["PATH"]
        if env.master_count > 0:
            environment["YT_CONSOLE_DRIVER_CONFIG_PATH"] = env.config_paths["console_driver"][0]

        execs = [self.name]

        gt_filter = pytest.config.getoption("--gtest_filter")
        if gt_filter:
            execs += ["--gtest_filter=%s" % (gt_filter)]

        child = subprocess.Popen(
            execs,
            cwd=self.sandbox_path,
            env=environment)
        child.wait()

        if child.returncode != 0:
            raise YtShellTestException(
                "C++",
                self.name,
                child.returncode)

    def reportinfo(self):
        return self.fspath, 0, "c++:%s" % self.name


class PerlFile(pytest.File):
    def collect(self):
        yield PerlItem(self)


class CppFile(pytest.File):
    def collect(self):
        yield CppItem(self)

