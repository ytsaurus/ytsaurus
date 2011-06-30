# -*- coding: utf-8 -*-

from os.path import join, expanduser

from util import traverse_up
from conf_map import Map

class Configuration(Map):
    def __init__(self, source_root, project, target):
        Map.__init__(self)
        
        self.map.update({'source_root': source_root, 'project': project,
                         'target': target})

        self.load_resource(source_root)
        self.read_file('/etc/om.conf', source_root)
        self.read_file('/usr/local/etc/om.conf', source_root)
        self.read_file(expanduser('~/.om.conf'), source_root)

        project_dir = join(source_root, project)
        files = [ join(project_dir, 'om.conf.local') ]
        for dir in traverse_up(source_root, project_dir):
            files.append(join(dir, 'om.conf'))
        for file in reversed(files):
            self.read_file(file)

    def load_resource(self, source_root):
        try:
            from om_configuration import configuration_path
            self.read_file(configuration_path(), source_root)
        except ImportError:
            pass
        except:
            pass
