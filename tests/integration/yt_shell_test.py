import sys
import os
import subprocess

from yt_environment import YTEnv
from yt_env_setup import SANDBOX_ROOTDIR

import pytest

def get_output(command):
    return subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]

class ShellException(Exception):
    def __init__(self, message, exit_code, stdout_diff, stderr_diff):
        Exception.__init__(self, message)
        self.exit_code = exit_code
        self.stdout_diff = stdout_diff
        self.stderr_diff = stderr_diff

class ShellFile(pytest.File):
    def collect(self):
        name = self.fspath.basename[:-3]
        yield ShellItem(name, self, self.fspath)

class ShellItem(pytest.Item):
    def __init__(self, name, parent, path):
        self.name = name
        self.path = str(path)
        self.basepath = self.path[:-3]
        super(ShellItem, self).__init__(name, parent)
        
    def get_options(self, source_path):
        with open(source_path, 'rt') as f:
            for l in f.xreadlines():
                if l.startswith('#%'):
                    other = l.lstrip('#%').rstrip('\n').replace(' ', "")
                    name, value = other.split('=')
                    yield (name, eval(value))
        
    def runtest(self):
        print "Executing test", self.name
        print 'basepath:', self.basepath
        print SANDBOX_ROOTDIR

        path_to_actual = os.path.join(SANDBOX_ROOTDIR, self.name)
        path_to_run = os.path.join(path_to_actual, "run")
        script_path = self.path
        
        print path_to_run

        class CurYTEnv(YTEnv):
            pass

        for name, value in self.get_options(script_path):
            print 'Setting', name, 'equal to', value
            setattr(CurYTEnv, name, value)

        Env = CurYTEnv()
        Env.set_environment(path_to_run)

        ext = [".stdout", ".stderr"]

        stdout_actual, stderr_actual = [os.path.join(path_to_actual, self.name) + e for e in ext]
        stdout_expected, stderr_expected = [self.basepath + e for e in ext]

        os.system('touch {stdout_expected}'.format(**vars()))
        os.system('touch {stderr_expected}'.format(**vars()))

        exit_code = os.system("cd {path_to_actual} && {script_path} >{stdout_actual} 2> {stderr_actual}".format(**vars()))
        Env.clear_environment()

        stdout_diff = get_output(["diff", "-ui", stdout_actual, stdout_expected])
        stderr_diff = get_output(["diff", "-ui", stderr_actual, stderr_expected])
        
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
                "shell test execution failed (exit code {0})".format(exc.exit_code),
                "-" * 80,
                "stdout diff:",
                exc.stdout_diff,
                "-" * 80,
                "stderr diff:",
                exc.stderr_diff,
                "-" * 80
            ])
        else:
            return super(ShellItem, self).repr_failure(excinfo)

    def reportinfo(self):
        return self.fspath, 0, "shell: %s (%s)" % (self.name, self.fspath)
