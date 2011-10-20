#!/usr/bin/python
#!-*-coding:utf-8-*-

from ytwin import *
import opts

Logging = {
        'Writers' : [
                {
                        'Name' : "File",
                        'Type' : "File",
                        'FileName' : "%(log_path)s",
                        'Pattern' : "$(datetime) $(level) $(category) $(message)"
                },
                {
                        'Name' : "StdErr",
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
                        'Categories' : [ "*" ], 
                        'MinLevel' : "Info", 
                        'Writers' : [ "StdErr" ] 
                }
        ]
}

MasterAddresses = opts.limit_iter('--masters',
        ['localhost:%d' % port for port in xrange(8001, 8004)])

class Base(AggrBase):
        path = opts.get_string('--name', 'control')
        
class Server(Base):
        bin_path = r'c:\Users\Max\Work\Yandex\YT\build\bin\Debug\server.exe'
                
class Master(WinNode, Server):
        address = Subclass(MasterAddresses)
        params = Template('--cell-master --config %(config_path)s --port %(port)d --id %(__name__)s')

        config = Template({
                'MetaState' : {
                                'Cell' : {
                                'Addresses' : MasterAddresses
                        },
                        'SnapshotLocation' : r'%(work_dir)s\snapshots',
                        'LogLocation' : r'%(work_dir)s\logs',
                },                      
                'Logging' : Logging
        })
        
        def run(cls, fd):
                print >>fd, 'mkdir %s' % cls.config['MetaState']['SnapshotLocation']
                print >>fd, 'mkdir %s' % cls.config['MetaState']['LogLocation']
                print >>fd, cls.run_tmpl
                
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                print >>fd, r'del /Q %s\*' % cls.config['MetaState']['SnapshotLocation']
                print >>fd, r'del /Q %s\*' % cls.config['MetaState']['LogLocation']
        
        
class Holder(WinNode, Server):
        address = Subclass(opts.limit_iter('--holders',
                        [('localhost:%d' % p) for p in range(9000, 9100)]))
        
        params = Template('--chunk-holder --config %(config_path)s --port %(port)d')
        
        config = Template({ 
                'Masters' : { 'Addresses' : MasterAddresses },
                'Locations' : [r'%(work_dir)s\node'],
                'MaxChunksSpace' : 100000000,
                'Logging' : Logging
        })
        
        def clean(cls, fd):
                print >>fd, 'del %s' % cls.log_path
                for location in cls.config['Locations']:
                        print >>fd, 'rmdir /S /Q   %s' % location

configure(Base)
        