from ytbase import *

cmd_run = '''TASKLIST /FI "WINDOWTITLE eq %(path)s" | find /I "PID"
IF ERRORLEVEL 1 (START "%(path)s" %(bin_path)s %(params)s)
'''

cmd_stop = 'TASKKILL /F /T /FI "WINDOWTITLE eq %(path)s"'

class WinNode(ServerNode):
    files = [Config, OldConfig, Run, Clean, Stop]
    
    @initmethod
    def init(cls):
        cls._init_path()
    
    run_tmpl = Template(cmd_run)
    def run(cls, fd):
        print >>fd, cls.run_tmpl

    stop_tmpl = Template(cmd_stop)
    def stop(cls, fd):
        print >>fd, cls.stop_tmpl
        
    def clean(cls, fd):
        pass


def configure(root):
    make_files(root)
    make_aggregate(root, lambda x: 'call ' + x)
    
