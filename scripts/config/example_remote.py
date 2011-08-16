#!/usr/bin/python
from ytremote import *
import opts

Logging = {
    'Writers' : [
        {
            'Name' : "File",
            'Type' : "File",
            'FileName' : "%(log_path)s",
            'Pattern' : "$(datetime) $(level) $(category) $(message)"
        }
    ],
    'Rules' : [
        { 
            'Categories' : [ "*" ], 
            'MinLevel' : "Debug", 
            'Writers' : [ "File" ] 
        } 
    ]
}

Port = 9091
HostPattern = 'yt-dev%0.2dd:%d'
MasterAddresses = opts.limit_iter('--masters',
        [HostPattern % (x, Port) for x in xrange(2, 5)])

class Base(AggrBase):
    path = opts.get_string('--name', 'control')
    
class Server(Base):
    bin_path = '/home/psushin/yt/trunk/yt/server/server'
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['Logging']['Writers'][0]['FileName'], cls.local_dir)
    
    
class Master(RemoteServer, Server):
    address = Subclass(MasterAddresses)
    params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s')

    config = Template({
        'Cell' : {
            'PeerAddresses' : MasterAddresses
        },
        'MetaState' : {
            'SnapshotLocation' : '%(work_dir)s/snapshots',
            'LogLocation' : '%(work_dir)s/logs',
        },            
        'Logging' : Logging
    })
    
    def do_run(cls, fd):
        print >>fd, shebang
        print >>fd, 'mkdir -p %s' % cls.config['MetaState']['SnapshotLocation']
        print >>fd, 'mkdir -p %s' % cls.config['MetaState']['LogLocation']
        print >>fd, cls.run_tmpl
        
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        print >>fd, 'rm %s/*' % cls.config['MetaState']['SnapshotLocation']
        print >>fd, 'rm %s/*' % cls.config['MetaState']['LogLocation']
    
    
class Holder(RemoteServer, Server):
    address = Subclass(opts.limit_iter('--holders',
            [HostPattern % (x, Port) for x in xrange(5, 10)]))
    
    params = Template('--chunk-holder --config %(config_path)s --port %(port)d')
    
    config = Template({ 
        'Masters' : { 'Addresses' : MasterAddresses },
        'Locations' : ['%(work_dir)s/node'],
        'Logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        for location in cls.config['Locations']:
            print >>fd, 'rm -rf %s' % location

class Client(Base, Node):
    files = [Config, Run]
    bin_path = '/home/psushin/yt/trunk/yt/experiments/send_chunk/send_chunk'
    params = Template('-stream -config %(config_path)s')
    ids = Subclass(xrange(3))

    config = Template({ 
        'Masters' : { 'Addresses' : MasterAddresses },
        'ReplicationFactor' : 2,
        'GroupSize': 8048000,
        'BlockSize': 1024000,
        'WindowSize': 40,
        'File': '/home/psushin/test.chunk',
        'Logging' : Logging
    })

    def run(cls, fd):
        print >>fd, shebang
        print >>fd, cls.bin_path + ' ' + cls.params

configure(Base)
    
