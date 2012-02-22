import sys
import os
import subprocess

from yt_env import YTEnv, SANDBOX_ROOTDIR

import pytest

def get_output(command):
    return subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]

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

        class CurYTEnv(YTEnv): pass
        for name, value in self.get_options(script_path):
            print 'Setting', name, 'equal to', value
            setattr(CurYTEnv, name, value)

        Env = CurYTEnv()
        Env.setUp(path_to_run)

        ext = [".stdout", ".stderr"]

        stdout_actual, stderr_actual = [os.path.join(path_to_actual, self.name) + e for e in ext]
        stdout_expected, stderr_expected = [self.basepath + e for e in ext]

        os.system('touch {stdout_expected}'.format(**vars()))
        os.system('touch {stderr_expected}'.format(**vars()))

        exit_code = os.system("cd {path_to_actual} && {script_path} >{stdout_actual} 2> {stderr_actual}".format(**vars()))
        Env.tearDown()

        stdout_diff = get_output(["diff", "-ui", stdout_actual, stdout_expected])
        stderr_diff = get_output(["diff", "-ui", stderr_actual, stderr_expected])
        
        if stdout_diff:
            print '-' * 70
            print 'stdout_diff:'
            print stdout_diff
            print '-' * 70

        if stderr_diff:
            print '-' * 70
            print 'stderr_diff:'
            print stderr_diff
            print '-' * 70

        assert exit_code == 0,  "{script_path} finished with errors".format(**vars())
        assert not stdout_diff, "Stdout differs"
        assert not stderr_diff, "Stderr differs"
        