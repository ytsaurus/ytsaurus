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
HostPattern = 'n01-04%0.2dg:%d'
MasterAddresses = opts.limit_iter('--masters', [HostPattern % (0, Port)])

class Base(AggrBase):
    path = opts.get_string('--name', 'control')
    
class Server(Base):
    bin_path = '/home/psushin/yt-svn/yt/yt/server/server'
    
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
            [HostPattern % (x, Port) for x in xrange(1, 41)]))
    
    params = Template('--chunk-holder --config %(config_path)s --port %(port)d')
    
    config = Template({ 
        'Masters' : { 'Addresses' : MasterAddresses },
        'Locations' : ['/yt/disk1/node'],
        'Logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        for location in cls.config['Locations']:
            print >>fd, 'rm -rf %s' % location

class Client(Base, Node):
    files = [Config, Run]
    bin_path = '/home/psushin/yt-svn/yt/yt/experiments/send_chunk/send_chunk'
    params = Template('--config %(config_path)s')

    Logging = {
        'Writers' : [
            {
                'Name' : "File",
                'Type' : "File",
                'FileName' : "%(log_path)s",
                'Pattern' : "$(datetime) $(level) $(category) $(message)"
            },
            {
                'Name' : "ChunkWriter",
                'Type' : "StdErr",
                'Pattern' : "$(datetime) $(level) $(category) $(message)"
            }
        ],
        'Rules' : [
            { 
                'Categories' : [ "*" ], 
                'MinLevel' : "Debug", 
                'Writers' : [ "File" ] 
            }, 
            { 
                'Categories' : [ "DumbTransaction", "ChunkWriter" ], 
                'MinLevel' : "Debug", 
                'Writers' : [ "ChunkWriter" ] 
            } 
        ]
    }

    config = Template({ 
        'ReplicationFactor' : 1,
        'BlockSize' : 1048576,

        'ThreadPool' : {
            'PoolSize' : 1,
            'TaskCount' : 1 
        },

        'Logging' : Logging,

        'Masters' : { 
            'Addresses' : MasterAddresses 
        },

        'DataSource' : {
            'Size' : 1048576 * 200 
        },

        'ChunkWriter' : {
            'WindowSize' : 40,
            'GroupSize' : 8 * 2 ** 20 
        } 
    })

    def run(cls, fd):
        print >>fd, shebang
        print >>fd, cls.bin_path + ' ' + cls.params

configure(Base)
    
