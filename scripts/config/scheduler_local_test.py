#!/usr/bin/python
import sys
sys.path.insert(0, "../yson")

from cfglib.ytunix import *
import cfglib.opts as opts
import yson
from copy import deepcopy

BIN_DIR = '/home/psushin/yt2/yt/bin/'
localhost = 'build01-01g'

Logging = {
    'writers' : {
        'raw' : {
            'type' : 'raw',
            'file_name' : "%(debug_log_path)s"
        },
        'file' : {
            'type' : "file",
            'file_name' : "%(log_path)s",
            'pattern' : "$(datetime) $(level) $(category) $(message)"
        }
    },
    'rules' : [
        { 
            'categories' : [ "*" ],
            'min_level' : "debug",
            'writers' : [ "raw" ]
        }, {
            'categories' : [ "*" ],
            'min_level' : "info",
            'writers' : [ "file" ]
        }
    ]
}

MasterAddresses = ['%s:8123' % localhost]

class Base(AggrBase):
    path = opts.get_string('--name', 'localtest')

class Server(Base):
    bin_path = os.path.join(BIN_DIR, 'ytserver')

class Master(UnixNode, Server):
    address = Subclass(MasterAddresses)
    params = Template('--master --config %(config_path)s --port %(port)d')

    config = Template({
        'meta_state' : {
            'leader_committer' : {
                'max_batch_delay': 50
            },
            'cell' : {
                'addresses' : MasterAddresses
            },
            'snapshot_path' : '%(work_dir)s/snapshots',
            'log_path' : '%(work_dir)s/changelogs',
            'max_changes_between_snapshots' : 1000000,
            'change_log_downloader' : {
                'records_per_request' : 10240
            },
        },
        'chunks' : {
            'registered_holder_timeout' : 180000,
            'jobs' : {
                'min_online_holder_count' : 3, # for local testing run
                'max_lost_chunk_fraction' : 0.01
            }
        },
        'logging' : Logging
    })

class Scheduler(UnixNode, Server):
    host = localhost
    params = Template('--scheduler --config %(config_path)s')

    config = Template({
        'masters' : {
                'addresses' : MasterAddresses
        },
        'scheduler' : {
            'strategy' : 'Fifo'
        },
        'rpc_port' : 8666,
        'logging' : Logging
    })

class Holder(UnixNode, Server):
    files = UnixNode.files

    address = Subclass(opts.limit_iter('--holders',
                    [('%s:%d' % (localhost, p)) for p in range(8900, 8903)]))
    params = Template('--node --config %(config_path)s --port %(port)d')
    
    proxyLogging = deepcopy(Logging)
    proxyLogging['writers']['raw']['file_name'] = 'raw.log'
    proxyLogging['writers']['file']['file_name'] = 'file.log'
    proxyLogging['rules'][0]['min_level'] = 'trace'

    storeQuota = 1700 * 1024 * 1024 * 1024 # the actual limit is ~1740
    cacheQuota = 1 * 1024 * 1024 * 1024
    config = Template({ 
        'masters' : {
            'addresses' : MasterAddresses
        },
        'chunk_holder' : {
            'store_locations' : [
                { 'path' : '%(work_dir)s/chunk_store', 'quota' : storeQuota },
            ],
            'cache_location' : {
                'path' : '%(work_dir)s/chunk_cache', 'quota' : cacheQuota
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
            'environment_manager' : {
                'environments' : {
                    'default' : {
                        'type' : 'unsafe'
                    }
                }
            },
            'job_manager': {
                'slot_location' : r'%(work_dir)s/slots',
            },
            'job_proxy_logging' : proxyLogging,
        },
        'logging' : Logging
    })

configure(Base)
