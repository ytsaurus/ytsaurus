#!/usr/bin/python
from cfglib.ytremote import *
from cfglib.ytwin import *
import cfglib.opts as opts
import socket

build_dir = os.environ['YT_BUILD_DIR']


Logging = {
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
        'std_err' :
            {
                'type' : "std_err",
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
            'writers' : [ "std_err", "file" ]
        }
    ]
}

MasterAddresses = opts.limit_iter('--masters',
        ['%s:%d' % (socket.gethostname(), port) for port in xrange(8001, 8004)])

class Base(AggrBase):
        path = opts.get_string('--name', 'control')
        
class Server(Base):
        bin_path = os.path.join(build_dir, r'bin\Debug\ytserver.exe')
                
class Master(WinNode, Server):
        address = Subclass(MasterAddresses)
        params = Template('--master --config %(config_path)s --port %(port)d')

        config = Template({
                'meta_state' : {
                        'cell' : {
                                'addresses' : MasterAddresses,
                                'rpc_port' : 8001
                        },
                        'snapshots' : {
                            'path' : r'%(work_dir)s\snapshots'
                        },
                        'changelogs' : {
                            'path' : r'%(work_dir)s\changelogs'
                        }
                },                      
                'logging' : Logging
        })
        
        def run(cls, fd):
                print >>fd, 'mkdir %s' % cls.config['meta_state']['snapshots']['path']
                print >>fd, 'mkdir %s' % cls.config['meta_state']['changelogs']['path']
                print >>fd, cls.run_tmpl
                
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, 'del %s' % cls.debug_log_path
                print >>fd, r'del /Q %s\*' % cls.config['meta_state']['snapshots']['path']
                print >>fd, r'del /Q %s\*' % cls.config['meta_state']['changelogs']['path']
        
        
class Holder(WinNode, Server):
        address = Subclass(opts.limit_iter('--holders',
            ['%s:%d' % (socket.getfqdn(), p) for p in range(9000, 9100)]))
        
        params = Template('--node --config %(config_path)s --port %(port)d')
        
        config = Template({ 
            'masters' : {
              'addresses' : MasterAddresses
            },
            'data_node' : {
                'store_locations' : [
                    { 'path' : r'%(work_dir)s\chunk_store.0' }
                ],
                'cache_location' : {
                    'path' : r'%(work_dir)s\chunk_cache',
                    'quota' : 10 * 1024 * 1024
                },
            },
            'exec_agent' : {
                'job_manager': {
                    'slot_location' : r'%(work_dir)s\slots',
                    'scheduler_address' : 'locahost:7000'
                },
                'environment_manager' : {
                    'environments' : {
                        'default' : {
                            'type' : 'unsafe'
                        }
                    }
                }
            },
            'logging' : Logging
        })
        
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, 'del %s' % cls.debug_log_path
                for location in cls.config['data_node']['store_locations']:
                        print >>fd, 'rmdir /S /Q   %s' % location['path']
                print >>fd, 'rmdir /S /Q   %s' % cls.config['data_node']['cache_location']['path']

configure(Base)
