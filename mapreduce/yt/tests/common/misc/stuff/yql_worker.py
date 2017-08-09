import os
import signal
import getpass
import uuid

import yatest.common

from devtools.swag.daemon import Daemon
import yql_utils


YQLWORKER_PATH = yql_utils.yql_binary_path('yql/tools/yqlworker/yqlworker')
UDF_RESOLVER_PATH = yql_utils.yql_binary_path('yql/tools/udf_resolver/udf_resolver')

YQLWORKER_CONFIG_TEMPLATE = '''
User: '%(user)s'
Group: '%(user)s'

Logging: {
    LogTo: FILE
    LogFile: '%(log_file)s'
    AllComponentsLevel: DEBUG
}

Inspector: {
    Hosts: [
        'localhost'
    ]
    Port: %(inspector_server_port)d

    MaxInFlight: 1000
    SendTimeoutSeconds: 30
    TotalTimeoutSeconds: 60
}

Server: {
    Threads: 2
    MaxInFlight: 1000
    SendTimeoutSeconds: 30
    TotalTimeoutSeconds: 60
}

Executor: {
    Threads: %(threads)s
    QueueMaxSize: 1000
    LoadUdfsDir: '%(udfs_dir)s'
    UdfResolverPath : '%(udf_resolver)s'
    MountConfig {
        %(mount_config)s
    }
}
ProcessMode: %(mode)s
'''


class YQLWorker(Daemon):
    def __init__(self, gateways_config_file, inspector_server_port, udfs_dir,
                 threads=2, filestorage_config_file=None, *args, **kwargs):
        self.working_dir = '%s_%s' % (yql_utils.yql_output_path('yqlworker'), uuid.uuid4())
        if not os.path.isdir(self.working_dir):
            os.mkdir(self.working_dir, 0o755)

        user = getpass.getuser()
        self.config_file = os.path.join(self.working_dir, 'worker.cfg')
        mount_config = yql_utils.default_mount_point_config_content
        udf_resolver = UDF_RESOLVER_PATH
        log_file = os.path.join(self.working_dir, 'worker.log')
        mode = yql_utils.get_param('worker_process_mode') or 'MULTI_PROCESS'

        with open(self.config_file, 'w') as f:
            f.write(YQLWORKER_CONFIG_TEMPLATE % locals())

        self.command = [
            YQLWORKER_PATH,
            '--cfg=' + self.config_file,
            '--gateways-cfg=' + gateways_config_file,
        ]

        if filestorage_config_file:
            self.command.append('--fs-cfg=' + filestorage_config_file)

        yql_utils.log('Starting yqlworker at %s' % self.working_dir)

        Daemon.__init__(self, self.command, timeout=180, cwd=self.working_dir, *args, **kwargs)

    def stop(self, kill=False):
        self.daemon.process.send_signal(signal.SIGTERM)
        try:
            yatest.common.process.wait_for(lambda: not self.is_alive(), 5, '', sleep_time=0.1)
        except yatest.common.TimeoutError:
            pass
        if self.is_alive():
            Daemon.stop(self, kill=kill)
