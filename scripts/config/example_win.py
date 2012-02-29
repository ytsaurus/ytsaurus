#!/usr/bin/python
from cfglib.ytremote import *
from cfglib.ytwin import *
import cfglib.opts as opts

build_dir = r'c:\Users\Max\Work\Yandex\YT\build'


Logging = {
    'writers' : {
        'raw' :
            {
                'type' : 'raw',
                'file_name' : "%(log_path)s.debug"
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
        ['localhost:%d' % port for port in xrange(8001, 8004)])

class Base(AggrBase):
        path = opts.get_string('--name', 'control')
        
class Server(Base):
        bin_path = os.path.join(build_dir, r'bin\Debug\ytserver.exe')
                
class Master(WinNode, Server):
        address = Subclass(MasterAddresses)
        params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s')

        config = Template({
                'meta_state' : {
                                'cell' : {
                                'addresses' : MasterAddresses
                        },
                        'snapshot_path' : r'%(work_dir)s\snapshots',
                        'log_path' : r'%(work_dir)s\logs',
                },                      
                'logging' : Logging
        })
        
        def run(cls, fd):
                print >>fd, 'mkdir %s' % cls.config['meta_state']['snapshot_path']
                print >>fd, 'mkdir %s' % cls.config['meta_state']['log_path']
                print >>fd, cls.run_tmpl
                
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, r'del /Q %s\*' % cls.config['meta_state']['snapshot_path']
                print >>fd, r'del /Q %s\*' % cls.config['meta_state']['log_path']
        
        
class Holder(WinNode, Server):
        address = Subclass(opts.limit_iter('--holders',
                        [('localhost:%d' % p) for p in range(9000, 9100)]))
        
        params = Template('--chunk-holder --config %(config_path)s --port %(port)d')
        
        config = Template({ 
                'masters' : { 'addresses' : MasterAddresses },
                'chunk_store_locations' : [
                    { 'path' : r'%(work_dir)s\chunk_store.0' }
                ],
                'chunk_cache_location' : {
                    'path' : r'%(work_dir)s\chunk_cache',
                    'quota' : 10 * 1024 * 1024
                },
                'cache_remote_reader' : { },
                'cache_sequential_reader' : { },
                'logging' : Logging
        })
        
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                for location in cls.config['chunk_store_locations']:
                        print >>fd, 'rmdir /S /Q   %s' % location['path']
                print >>fd, 'rmdir /S /Q   %s' % cls.config['chunk_cache_location']['path']


class Client(WinNode, Base):
    bin_path = os.path.join(build_dir, r'bin\Debug\send_chunk.exe') 

    params = Template('--config %(config_path)s')

    config_base = { 
        'replication_factor' : 2,
        'block_size' : 2 ** 20,

        'thread_pool' : {
            'pool_size' : 1,
            'task_count' : 1
        },

        'logging' : Logging,

        'masters' : {
            'addresses' : MasterAddresses
        },

        'chunk_writer' : {
            'window_size' : 40,
            'group_size' : 8 * (2 ** 20)
        } 
    }

    def clean(cls, fd):
        print >>fd, 'del %s' % cls.log_path

def client_config(d):
    d.update(Client.config_base)
    return d

class PlainChunk(Client):
    config = Template(client_config({
        'ypath' : '/files/plain_chunk',
        'input' : {
            'type' : 'zero', # stream of zeros
            'size' : (2 ** 20) * 256
        }
    }))

class TableChunk(Client):
    config = Template(client_config({ 
        'schema' : '[["berlin"; "madrid"; ["xxx"; "zzz"]]; ["london"; "paris"; ["b"; "m"]]]',
        'ypath' : '/files/table_chunk',
        'input' : {
            'type' : 'random_table',
            'size' : (2 ** 20) * 20
        }
    }))

class TableChunkSequence(Client):
    config = Template(client_config({ 
        'schema' : '[["berlin"; "madrid"; ["xxx"; "zzz"]]; ["london"; "paris"; ["b"; "m"]]]',
        'chunk_size' : (2 ** 20) * 7,
        'ypath' : '/table',
        'input' : {
            'type' : 'random_table',
            'size' : (2 ** 20) * 20
        }
    }))

configure(Base)
