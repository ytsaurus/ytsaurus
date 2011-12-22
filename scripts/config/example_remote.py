#!/usr/bin/python
from cfglib.ytremote import *
import cfglib.opts as opts

Logging = {
    'writers' : { 
        'file' :
        {
            'type' : "File",
            'file_name' : "%(log_path)s",
            'pattern' : "$(datetime) $(level) $(category) $(message)"
        }
    },
    'rules' : [
        { 
            'categories' : [ "*" ],
            'min_level' : "Debug",
            'writers' : [ "file" ]
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
        '/home/yt/build/lib/libnetliba_v6.so'
    ]
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['logging']['writers']['file']['file_name'], cls.local_dir)
    
class Server(Base, RemoteServer):
    bin_path = '/home/yt/build/bin/server'
    
class Master(Server):
    address = Subclass(MasterAddresses)
    params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s')

    config = Template({
        'meta_state' : {
            'cell' : {
                'addresses' : MasterAddresses
            },
            'snapshot_path' : '%(work_dir)s/snapshots',
            'log_path' : '%(work_dir)s/logs',
            'max_changes_between_snapshots' : 1000
        },            
        'logging' : Logging
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
       

CleanCache = FileDescr('clean_cache', ('aggregate', 'exec'))
DoCleanCache = FileDescr('do_clean_cache', ('remote', 'exec'))

class Holder(Server):
    files = Server.files + [CleanCache, DoCleanCache]

    groupid = Subclass(xrange(10))
    nodeid = Subclass(xrange(30), 1)
    
    @propmethod
    def host(cls):
        return 'n01-0%dg' % (400 + 30 * cls.groupid + cls.nodeid)
    
    port = Port
    params = Template('--chunk-holder --config %(config_path)s --port %(port)d')

    storeQuota = 1700 * 1024 * 1024 * 1024 # the actual limit is ~1740
    cacheQuota = 1 * 1024 * 1024 * 1024
    config = Template({ 
        'masters' : { 'addresses' : MasterAddresses },
        'chunk_store_locations' : [
            { 'path' : '/yt/disk1/data/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk2/data/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk3/data/chunk_store', 'quota' : storeQuota },
            { 'path' : '/yt/disk4/data/chunk_store', 'quota' : storeQuota }
        ],
        'chunk_cache_location' : {
            'path' : '/yt/disk1/data/chunk_cache', 'quota' : cacheQuota
        },
        'cache_remote_reader' : { 
            'holder_rpc_timeout' : 5 * 60 * 1000
        },
        'cache_sequential_reader' : { },
        'max_cached_blocks_size' : 10 * 1024 * 1024 * 1024,
        'max_cached_readers' : 256,
        'logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        for location in cls.config['chunk_store_locations']:
            print >>fd, 'rm -rf %s' % location['path']
        print >>fd, 'rm -rf %s' % cls.config['chunk_cache_location']['path']

    def do_clean_cache(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -rf %s' % cls.config['chunk_cache_location']['path']


configure(Base)
    
