from ytbase import *

cmd_start = 'START "%(path)s" %(bin_path)s %(params)s'
cmd_stop = 'TASKKILL /F /T /FI "WINDOWTITLE eq %(path)s"'

class WinNode(ServerNode):
	files = [Config, Start, Clean, Stop]
	
	@initmethod
	def init(cls):
		cls._init_path()
		for descr in cls.files:
			setattr(cls, '_'.join((descr.name, 'path')),
					cls.local_path(descr.filename))
	
	start_tmpl = Template(cmd_start)
	def start(cls, fd):
		print >>fd, cls.start_tmpl

	stop_tmpl = Template(cmd_stop)
	def stop(cls, fd):
		print >>fd, cls.stop_tmpl
		
	def clean(cls, fd):
		pass


def configure(root):
	make_files(root)
	make_aggregate(root, lambda x: 'START /B ' + x)
	