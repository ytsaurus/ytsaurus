
import unittest
import os
import sys
import re
import subprocess

from yt_env import YTEnv

TEST_ROOTDIR = os.path.abspath('tests')
SANDBOX_ROOTDIR = os.path.abspath('sandbox')

def get_output(command):
    return subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]

class SomeTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        self.env.tearDown()

    def RunOneTest(self, folder):
        print "Executing test:", folder

        run_path = os.path.join(SANDBOX_ROOTDIR, folder, "run")

        path_to_actual = os.path.join(SANDBOX_ROOTDIR, folder)
        path_to_expected = os.path.join(TEST_ROOTDIR, folder)

        env_py = os.path.join(path_to_expected, "env.py")
        if os.path.exists(env_py):
            path_to_env = os.path.abspath(path_to_expected)
            sys.path = [path_to_env] + sys.path
            if 'env' in sys.modules:
                env = reload(sys.modules['env'])
            else:
                import env
            self.env = env.Env(TEST_ROOTDIR, run_path)
            sys.path.remove(path_to_env)
        else:
            self.env = YTEnv(TEST_ROOTDIR, run_path)

        test_sh = os.path.join(path_to_expected, "test.sh")

        self.assertTrue(os.path.exists(test_sh), "%s test doesn't contain test.sh" % folder)

        output_names = ['stdout.txt', 'stderr.txt']
        stdout_actual, stderr_actual = [os.path.join(path_to_actual, f) for f in output_names]
        stdout_expected, stderr_expected = [os.path.join(path_to_expected, f) for f in output_names]

        os.system('touch {stdout_expected}'.format(**vars()))
        os.system('touch {stderr_expected}'.format(**vars()))
        #self.assertTrue(os.path.exists(stdout_expected), "%s test doesn't contain stdout.txt" % folder)
        #self.assertTrue(os.path.exists(stderr_expected), "%s test doesn't contain stderr.txt" % folder)

        self.env.setUp()

        #raw_input('press any key')
        exit_code = os.system("cd {run_path} && {test_sh} >{stdout_actual} 2> {stderr_actual}".format(**vars()))
        self.assertTrue(exit_code == 0, "test.sh finished with errors")

        stdout_diff = get_output(["diff", stdout_actual, stdout_expected])
        stderr_diff = get_output(["diff", stderr_actual, stderr_expected])
        
        if stdout_diff:
            print '-----------------------'
            print 'actual stdout:'
            os.system('cat ' + stdout_actual)
            print '-----------------------'

        if stderr_diff:
            print '-----------------------'
            print 'actual stderr:'
            os.system('cat ' + stderr_actual)
            print '-----------------------'

        self.assertFalse(stdout_diff, "Stdout differs")
        self.assertFalse(stderr_diff, "Stderr differs")

def RegisterTests():
    module = sys.modules[__name__]
    dirnames = os.listdir(TEST_ROOTDIR)
    if not dirnames:
        sys.exit('No tests found in %s!' % os.path.abspath(TEST_ROOTDIR))
 	
    for dirname in dirnames:
        if not os.path.isdir(os.path.join(TEST_ROOTDIR, dirname)): continue # skip non dirs
        class_name = re.sub('[^0-9a-zA-Z_]', '_', dirname)  # python-clean

        if class_name[0].isdigit(): # classes can't start with a number
            class_name = '_' + class_name

        test_class = type(class_name,          # class name
                        (SomeTest,),          # superclass
                        {'runTest': lambda self, f=dirname: self.RunOneTest(f)})

        setattr(module, test_class.__name__, test_class)

  		
if __name__ == "__main__":
    # setting path for ytdriver
    os.environ['PATH'] = TEST_ROOTDIR + ':' + os.environ['PATH']
    RegisterTests()
    unittest.main()
