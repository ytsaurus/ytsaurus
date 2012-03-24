from ytbase import *

Prepare = FileDescr('prepare', ('aggregate', 'exec', ))

shebang = '#!/bin/bash'

def wrap_cmd(cmd, silent=False):
    res = ['cmd="%s"' % cmd]
    if silent:
        res.append('$cmd 2&>1 1>/dev/null')
    else:
        res.append('$cmd')
    res.append('''if [ $? -ne 0 ]; then
        echo "Command failed: " $cmd
        exit
fi''')
    return '\n'.join(res)

ulimit = 'ulimit -c unlimited'
cmd_run = 'start-stop-daemon -d ./ -b --exec %(bin_path)s --pidfile %(work_dir)s/pid -m -S -- %(params)s'
cmd_stop = 'start-stop-daemon --pidfile %(work_dir)s/pid -K'

class UnixNode(ServerNode):
    files = [Config, Run, Clean, Stop]

    @initmethod
    def init(cls):
        cls._init_path()

    run_tmpl = Template(cmd_run)
    def run(cls, fd):
        print >>fd, ulimit
        print >>fd, cls.run_tmpl

    stop_tmpl = Template(cmd_stop)
    def stop(cls, fd):
        print >>fd, cls.stop_tmpl

    def clean(cls, fd):
        pass


def configure(root):
    make_files(root)
    make_aggregate(root, lambda x: x + '&', 'wait')
