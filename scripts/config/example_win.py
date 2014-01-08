#!/usr/bin/python
from cfglib.ytremote import *
from cfglib.ytwin import *
import cfglib.opts as opts
import socket

build_dir = os.environ['YT_BUILD_DIR']


Logging = {
    'flush_period' : 0,
    'writers' : {
        'raw' :
            {
                'type' : 'raw',
                'file_name' : "%(debug_log_path)s"
            },
        'file' :
            {
                'type' : "file",
                'file_name' : "%(log_path)s",
                'pattern' : "$(datetime) $(level) $(category) $(message)"
            },
        'stderr' :
            {
                'type' : "stderr",
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
            'writers' : [ "stderr", "file" ]
        }
    ]
}

MasterAddresses = opts.limit_iter('--masters',
        ['%s:%d' % (socket.getfqdn(), port) for port in xrange(8001, 8004)])

class Base(AggrBase):
        path = opts.get_string('--name', 'control')

class Server(Base):
        bin_path = os.path.join(build_dir, r'bin\Debug\ytserver.exe')

        @propmethod
        def monport(cls):
            return cls.port + 2000

class Master(WinNode, Server):
        address = Subclass(MasterAddresses)
        params = Template('--master --config %(config_path)s')

        config = Template({
                'masters' : {
                    'addresses' : MasterAddresses
                },
                'timestamp_provider' : {
                    'addresses' : MasterAddresses
                },
                'snapshots' : {
                    'path' : r'%(work_dir)s\snapshots'
                },
                'changelogs' : {
                    'path' : r'%(work_dir)s\changelogs'
                },
                'node_tracker' : {
                    'registered_node_timeout' : 5000,
                    'online_node_timeout' : 10000
                },
                'tablet_manager' : {
                    'peer_failover_timeout' : 15000
                },
                'rpc_port' : r'%(port)d',
                'monitoring_port' : r'%(monport)d',
                'logging' : Logging
        })

        def run(cls, fd):
                print >>fd, 'mkdir %s' % cls.config['snapshots']['path']
                print >>fd, 'mkdir %s' % cls.config['changelogs']['path']
                print >>fd, cls.run_tmpl

        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, 'del %s' % cls.debug_log_path
                print >>fd, r'del /Q %s\*' % cls.config['snapshots']['path']
                print >>fd, r'del /Q %s\*' % cls.config['changelogs']['path']


class Holder(WinNode, Server):
        address = Subclass(opts.limit_iter('--holders',
            ['%s:%d' % (socket.getfqdn(), p) for p in range(9000, 9100)]))

        params = Template('--node --config %(config_path)s')

        config = Template({
            'masters' : {
              'addresses' : MasterAddresses
            },
            'timestamp_provider' : {
                'addresses' : MasterAddresses
            },
            'query_agent': {
            },
            'data_node' : {
                'incremental_heartbeat_period' : 500,
                'store_locations' : [
                    { 'path' : r'%(work_dir)s\chunk_store.0' }
                ],
                'cache_location' : {
                    'path' : r'%(work_dir)s\chunk_cache',
                    'quota' : 10 * 1024 * 1024
                },
                'session_timeout' : 10000
            },
            'exec_agent' : {
                'job_controller': {
                },
                'slot_manager': {
                    'path' : r'%(work_dir)s\slots'
                },
                'environment_manager' : {
                    'environments' : {
                        'default' : {
                            'type' : 'unsafe'
                        }
                    }
                }
            },
            'tablet_node' : {
                'changelogs' : {
                    'path' : r'%(work_dir)s\changelogs'
                },
                'snapshots' : {
                    'path' : r'%(work_dir)s\snapshots'
                },
                'tablet_manager' : {
                    'value_count_rotation_threshold' : 10
                }
            },
            'rpc_port' : r'%(port)d',
            'monitoring_port' : r'%(monport)d',
            'logging' : Logging
        })

        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, 'del %s' % cls.debug_log_path
                print >>fd, 'rmdir /S /Q %s' % cls.config['tablet_node']['snapshots']['path']
                print >>fd, 'rmdir /S /Q %s' % cls.config['tablet_node']['changelogs']['path']
                for location in cls.config['data_node']['store_locations']:
                        print >>fd, 'rmdir /S /Q   %s' % location['path']
                print >>fd, 'rmdir /S /Q   %s' % cls.config['data_node']['cache_location']['path']

configure(Base)
