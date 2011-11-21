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
MasterAddresses = opts.limit_iter('--masters', ['meta01-00%dg:%d' % (i, Port) for i in xrange(1, 4)])

class Base(AggrBase):
    path = opts.get_string('--name', 'control')
    base_dir = '/yt/disk1/'
    libs = ['/home/yt/build/debug/extern/STLport/build/cmake/libstlport.so.5.2']
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['Logging']['Writers'][0]['FileName'], cls.local_dir)
    
Yson = FileDescr('new_config', ('remote'), 'yson')
class Server(Base, RemoteServer):
    files = RemoteServer.files + [Yson]
    bin_path = '/home/yt/src/yt/server/server'

    yson_data = '{hello=world}'
    def new_config(cls, fd):
        print >>fd, cls.yson_data

    
class Master(Server):
    base_dir = './'
    address = Subclass(MasterAddresses)
    params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s --new_config %(new_config_path)s')

    config = Template({
        'MetaState' : {
	        'Cell' : {
    	        'Addresses' : MasterAddresses
        	},
            'SnapshotLocation' : '%(work_dir)s/snapshots',
            'LogLocation' : '%(work_dir)s/logs',
            'MaxChangesBetweenSnapshots' : 100000
        },            
        'Logging' : Logging
    })
    
    def do_run(cls, fd):
        print >>fd, shebang
        print >>fd, wrap_cmd('mkdir -p %s' % cls.config['MetaState']['SnapshotLocation'])
        print >>fd, wrap_cmd('mkdir -p %s' % cls.config['MetaState']['LogLocation'])
        print >>fd, ulimit
        print >>fd, cls.export_ld_path
        print >>fd, wrap_cmd(cls.run_tmpl)
        
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        print >>fd, 'rm %s/*' % cls.config['MetaState']['SnapshotLocation']
        print >>fd, 'rm %s/*' % cls.config['MetaState']['LogLocation']
    
    
class Holder(Server):
    groupid = Subclass(xrange(10))
    nodeid = Subclass(xrange(10))
    host = Template('n01-04%(groupid)d%(nodeid)dg')
    port = Port
    
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
            'TaskCount' : 100 
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
    
