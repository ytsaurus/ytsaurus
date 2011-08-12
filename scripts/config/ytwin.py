from ytbase import *

Files = [Config, Start, Clean, Stop]

cmd_start = 'START "%(path)s" %(binpath)s %(params)s'
cmd_stop = 'TASKKILL /F /IM cmd.exe /T /FI "WINDOWTITLE eq %(path)s"'

class WinNode(Node, ServerNode):
    start_tmpl = Template(cmd_start)
    def start(cls, fd):
        print >>fd, cls.start_tmpl

    stop_tmpl = Template(cmd_stop)
    def stop(cls, fd):
        print >>fd, cls.stop_tmpl
        
    def clean(cls, fd):
        pass


def configure(root):
	make_files(roo
	