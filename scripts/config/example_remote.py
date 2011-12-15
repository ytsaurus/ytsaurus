#!/usr/bin/python
from cfglib.ytremote import *
import cfglib.opts as opts

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
    base_dir = '/yt/disk1/data'
    libs = [
        '/home/yt/build/lib/libstlport.so.5.2',
        '/home/yt/build/lib/libsnappy.so.1.0',
        '/home/yt/build/lib/libyajl.so.2',
        '/home/yt/build/lib/libminilzo.so.2.0',
        '/home/yt/build/lib/liblz4.so.0.1',
        '/home/yt/build/lib/libfastlz.so.0.1',
        '/home/yt/build/lib/libjson.so',
        '/home/yt/build/lib/libarczlib.so.1.2.3',
        '/home/yt/build/lib/libqmisc.so',
        '/home/yt/build/lib/libNetLiba.so'
    ]
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['Logging']['Writers'][0]['FileName'], cls.local_dir)
    
class Server(Base, RemoteServer):
    bin_path = '/home/yt/build/bin/server'
    
class Master(Server):
    address = Subclass(MasterAddresses)
    params = Template('--cell-master --config %(config_path)s --old_config %(old_config_path)s --port %(port)d --id %(__name__)s')

    config = Template({
        'meta_state' : {
            'cell' : {
                'addresses' : MasterAddresses
            },
            'snapshot_path' : '%(work_dir)s/snapshots',
            'log_path' : '%(work_dir)s/logs',
            'max_changes_between_snapshots' : 1000
        },            
        'Logging' : Logging
    })
    
    def do_run(cls, fd):
        print >>fd, shebang
        print >>fd, ulimit
        print >>fd, cls.export_ld_path
        print >>fd, wrap_cmd(cls.run_tmpl)
        
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        print >>fd, 'rm %s/*' % cls.config['meta_state']['snapshot_path']
        print >>fd, 'rm %s/*' % cls.config['meta_state']['log_path']
       
class Holder(Server):
    nodeid = Subclass(xrange(10))
    groupid = Subclass(xrange(10))
    host = Template('n01-04%(groupid)d%(nodeid)dg')
    port = Port
    
    params = Template('--chunk-holder --config %(config_path)s --old_config %(old_config_path)s --port %(port)d')

    storeQuota = 1700 * 1024 * 1024 * 1024 # the actual limit is ~1740
    cacheQuota = 1 * 1024 * 1024 * 1024
    config = Template({ 
        'masters' : { 'addresses' : MasterAddresses },
        'chunk_store_locations' : [
            { 'path' : '/yt/disk1/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk2/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk3/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk4/chunk_store', 'quota' : storeQuota }
        ],
        'chunk_cache_location' : {
            'path' : '/yt/disk1/chunk_cache', 'quota' : cacheQuota
        },
        'cache_remote_reader' : { },
        'cache_sequential_reader' : { },
        'max_cached_blocks_size' : 10 * 1024 * 1024 * 1024,
        'max_cached_readers' : 256,
        'Logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        for location in cls.config['chunk_store_locations']:
            print >>fd, 'rm -rf %s' % location['path']
        print >>fd, 'rm -rf %s' % cls.config['chunk_cache_location']['path']

class Client(Base, RemoteNode):
    bin_path = '/home/yt/build/bin/send_chunk'

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
    
