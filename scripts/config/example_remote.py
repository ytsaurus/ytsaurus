#!/usr/bin/python
from cfglib.ytremote import *
import cfglib.opts as opts

Logging = {
    'writers' : {
        'raw' :
        {
            'type' : 'raw',
            'file_name' : "/yt/disk1/data/logs/%(debug_log_path)s"
        },
        'file' :
        {
            'type' : "file",
            'file_name' : "/yt/disk1/data/logs/%(log_path)s",
            'pattern' : "$(datetime) $(level) $(category) $(message)"
        }
    },
    'rules' : [
        { 
            'categories' : [ "*" ],
            'min_level' : "debug",
            'writers' : [ "raw" ]
        },
        {
            'categories' : [ "*" ],
            'min_level' : "info",
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
    
    def get_log(cls, fd):
        print >>fd, shebang
        print >>fd, 'rsync %s:%s %s' % (cls.host, cls.config['logging']['writers']['file']['file_name'], cls.local_dir)
    
class Server(Base, RemoteServer):
    bin_path = '/home/yt/build/bin/ytserver'
    
class Master(Server):
    address = Subclass(MasterAddresses)
    params = Template('--master --config %(config_path)s --port %(port)d --id %(__name__)s')

    log_path = Template("master-%(__name__)s.log")
    debug_log_path = Template("master-%(__name__)s.debug.log")

    config = Template({
        'meta_state' : {
            'leader_committer' : {
                'max_batch_delay': 50
            },
            'cell' : {
                'addresses' : MasterAddresses
            },
            'snapshot_path' : '/yt/disk1/data/snapshots',
            'log_path' : '/yt/disk2/data/changelogs',
            'max_changes_between_snapshots' : 1000000,
            'change_log_downloader' : {
                'records_per_request' : 10240
            },
        },
        'chunks' : {
            'registered_holder_timeout' : 180000,
            'jobs' : {
                'min_online_holder_count' : 250,
                'max_lost_chunk_fraction' : 0.01
            }
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
        print >>fd, 'rm -f %s' % cls.debug_log_path
        print >>fd, 'rm %s/*' % cls.config['meta_state']['snapshot_path']
        print >>fd, 'rm %s/*' % cls.config['meta_state']['log_path']
        

CleanCache = FileDescr('clean_cache', ('aggregate', 'exec'))
DoCleanCache = FileDescr('do_clean_cache', ('remote', 'exec'))

class Holder(Server):
    files = Server.files + [CleanCache, DoCleanCache]

    # XXX(sandello): I have taken one group for my personal experiments
    # for deploying YT via Chef. Hence nodes 670-699 are unused in this deployment.
    groupid = Subclass(xrange(9))
    nodeid = Subclass(xrange(30), 1)

    log_path = Template("holder-%(groupid)d-%(nodeid)d.log")
    debug_log_path = Template("holder-%(groupid)d-%(nodeid)d.debug.log")
    
    @propmethod
    def host(cls):
        return 'n01-0%dg' % (400 + 30 * cls.groupid + cls.nodeid)
    
    port = Port
    params = Template('--node --config %(config_path)s --port %(port)d')

    storeQuota = 1700 * 1024 * 1024 * 1024 # the actual limit is ~1740
    cacheQuota = 1 * 1024 * 1024 * 1024
    config = Template({ 
        'masters' : {
            'addresses' : MasterAddresses
        },
        'chunk_holder' : {
            'store_locations' : [
                { 'path' : '/yt/disk1/data/chunk_store', 'quota' : storeQuota },
                { 'path' : '/yt/disk2/data/chunk_store', 'quota' : storeQuota },
                { 'path' : '/yt/disk3/data/chunk_store', 'quota' : storeQuota },
                { 'path' : '/yt/disk4/data/chunk_store', 'quota' : storeQuota }
            ],
            'cache_location' : {
                'path' : '/yt/disk1/data/chunk_cache', 'quota' : cacheQuota
            },
            'cache_remote_reader' : { 
                'publish_peer' : 'true'
            },
            'full_heartbeat_timeout' : 180000,
            'max_cached_blocks_size' : 10 * 1024 * 1024 * 1024,
            'max_cached_readers' : 256,
            'response_throttling_size' : 500 * 1024 * 1024
        },
        'exec_agent' : {
        },
        'logging' : Logging
    })
    
    def do_clean(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -f %s' % cls.log_path
        print >>fd, 'rm -f %s' % cls.debug_log_path
        for location in cls.config['chunk_holder']['store_locations']:
            print >>fd, 'rm -rf %s' % location['path']
        print >>fd, 'rm -rf %s' % cls.config['chunk_holder']['cache_location']['path']

    def do_clean_cache(cls, fd):
        print >>fd, shebang
        print >>fd, 'rm -rf %s' % cls.config['chunk_holder']['cache_location']['path']


configure(Base)
    
