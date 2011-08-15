from ytbase import *

GetLog = FileDescr('get_log', ('aggregate', 'exec'))
Prepare = FileDescr('prepare', ('aggregate', 'exec', ))
DoRun = FileDescr('do_run', ('remote', 'exec'))
DoStop = FileDescr('do_stop', ('remote', 'exec'))
DoClean = FileDescr('do_clean', ('remote', 'exec'))

Files = [Config, Prepare, DoRun, Run, DoStop, Stop, Clean, DoClean, GetLog]

#################################################################

shebang = '#!/bin/bash -v'
cmd_run = 'start-stop-daemon -d ./ -b --exec %(work_dir)s/%(binary)s ' + \
			'--pidfile %(work_dir)s/pid -m -S -- %(params)s'
cmd_stop = 'start-stop-daemon --pidfile %(work_dir)s/pid -K'

class RemoteNode(Node):
	files = Files
	
	def remote_path(cls, filename):
		return os.path.join(cls.remote_dir, filename)
	
	@initmethod
	def init(cls):
		cls._init_path()
		cls.remote_dir = os.path.join('.', cls.path)
		cls.work_dir = cls.remote_dir
		
		for descr in cls.files:
			if 'remote' in descr.attrs:
				setattr(cls, '_'.join((descr.name, 'path')),
						cls.remote_path(descr.filename))
			else:
				setattr(cls, '_'.join((descr.name, 'path')),
						cls.local_path(descr.filename))
						
		
	prepare_tmpl = Template('''#!/bin/bash -v
		ssh %(host)s mkdir -p %(remote_dir)s 
		rsync --copy-links %(bin_path)s %(host)s:%(remote_dir)s
	''')
	
	def prepare(cls, fd):
		print >>fd, cls.prepare_tmpl
		
		for descr in cls.files:
			if 'remote' in descr.attrs:
				print >>fd, "rsync %s %s:%s" % \
					(os.path.join(cls.local_path(descr.filename)),
					 cls.host, cls.remote_dir)
				
	run_tmpl = Template(cmd_run)
	def do_run(cls, fd):
		print >>fd, shebang
		print >>fd, cls.run_tmpl
	
	def run(cls, fd):
		print >>fd, shebang
		print >>fd, 'ssh %s %s' % (cls.host, cls.do_run_path)
		
	stop_tmpl = Template(cmd_stop)
	def do_stop(cls, fd):
		print >>fd, shebang
		print >>fd, cls.stop_tmpl
	
	def stop(cls, fd):
		print >>fd, shebang
		print >>fd, 'ssh %s %s' % (cls.host, cls.do_stop_path)
		
	def clean(cls, fd):
		print >>fd, shebang
		print >>fd, 'ssh %s %s' % (cls.host, cls.do_clean_path)
		

class RemoteServer(RemoteNode, ServerNode):
	pass
							
##################################################################

def configure(root):
	make_files(root)
	make_aggregate(root, lambda x:x)
	
	hosts = set()
	def append_hosts(node):
		for l in node.__leafs:
			append_hosts(l)
		if not node.__leafs:
			hosts.add(node.host)
	append_hosts(root)
		
	with open(root.local_path('remove_all.' + SCRIPT_EXT), 'w') as fd:
		print >>fd, shebang
		for host in hosts:
			print >>fd, 'ssh %s rm -rf %s' % (host, root.path)
	make_executable(fd.name)
		