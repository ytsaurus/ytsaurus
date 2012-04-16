#!/usr/bin/python
from cfglib.ytremote import *
import cfglib.opts as opts

class Base(AggrBase):
    path = opts.get_string('--name', 'bus_test')
    base_dir = '/yt/disk1/data'
    bin_path = '/home/yt/build/bin/rpc'

    libs = [
        '/home/yt/build/lib/libstlport.so.5.2',
        '/home/yt/build/lib/libyajl.so.2',
        '/home/yt/build/lib/libytext-fastlz.so.0.1',
        '/home/yt/build/lib/libytext-json.so',
        '/home/yt/build/lib/libytext-lz4.so.0.1',
        '/home/yt/build/lib/libytext-minilzo.so.2.0',
        '/home/yt/build/lib/libytext-quality-misc.so',
        '/home/yt/build/lib/libytext-quality-netliba_v6.so',
        '/home/yt/build/lib/libytext-snappy.so.1.0',
        '/home/yt/build/lib/libytext-zlib.so.1.2.3'
    ]
    
class Server(Base, RemoteServer):
    files = [Run, DoRun, Stop, DoStop, Prepare]

    host = "meta01-001g"
    params = '--bus-server'

class Client(Base, RemoteNode):
    files = [Run, DoRun, Stop, DoStop, Prepare]

    host = Subclass(["n01-0%dg" % i for i in xrange(400, 700)])
    params = '--bus-client --address ' + Server.host + ':8888'

    def run(cls, fd):
        print >>fd, shebang
        print >>fd, cmd_ssh % (cls.host,
            'start-stop-daemon -d ./ -b --exec %s --pidfile %s/pid -m -S' % (cls.do_run_path, cls.work_dir))

    def do_run(cls, fd):
        print >>fd, shebang
        print >>fd, ulimit
        print >>fd, cls.export_ld_path
        run_cmd = '%s/%s %s' % (cls.work_dir, cls.binary, cls.params)
        print >>fd, 'for i in {1..300}'
        print >>fd, 'do'
        print >>fd, ' '.join(['''echo "smth\ndone\n" ''', '|', run_cmd])
        print >>fd, 'done'

    def do_stop(cls, fd):
        print >>fd, shebang
        print >>fd, 'killall rpc'

configure(Base)
    

