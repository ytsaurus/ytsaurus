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
MasterAddresses = opts.limit_iter('--masters', ['meta01-00%dg:%d' % (i, Port) for i in xrange(3)])

class Base(AggrBase):
    path = opts.get_string('--name', 'control')
    base_dir = '/yt/disk1/'
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['Logging']['Writers'][0]['FileName'], cls.local_dir)
    
class Server(Base):
    bin_path = '/home/yt/src/yt/server/server'
    
class Master(RemoteServer, Server):
    address = Subclass(MasterAddresses)
    params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s')

    config = Template({
        'MetaState' : {
	        'Cell' : {
    	        'Addresses' : MasterAddresses
        	},
            'SnapshotLocation' : '/yt/disk2/snapshots',
            'LogLocation' : '/yt/disk2/logs',
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
            ['n01-04%0.2dg:%d' % (x, Port) for x in xrange(100)]))
    
    params = Template('--chunk-holder --config %(config_path)s --port %(port)d')
    
    config = Template({ 
        'Masters' : { 'Addresses' : MasterAddresses },
        'Locations' : ['/yt/disk2/node', '/yt/disk3/node', '/yt/disk4/node'],
        'Logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        for location in cls.config['Locations']:
            print >>fd, 'rm -rf %s' % location

class Client(Base, RemoteNode):
    bin_path = '/home/yt/src/yt/experiments/send_chunk/send_chunk'

    params = Template('--config %(config_path)s')

    host = Subclass(opts.limit_iter('--clients', 
        ['n01-04%0.2dg' % d for d in xrange(5, 90)]))

    config = Template({ 
        'ReplicationFactor' : 2,
        'BlockSize' : 2 ** 20,

        'ThreadPool' : {
            'PoolSize' : 1,
            'TaskCount' : 1000 
        },

        'Logging' : Logging,

        'Masters' : { 
            'Addresses' : MasterAddresses 
        },

        'DataSource' : {
            'Size' : (2 ** 20) * 256 
        },

        'ChunkWriter' : {
            'WindowSize' : 40,
            'GroupSize' : 8 * (2 ** 20)
        } 
    })

    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path

configure(Base)
    
