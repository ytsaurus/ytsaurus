import os
import tarfile
import tempfile
import time
import subprocess
import uuid

import pytest
import yatest

from yql_utils import get_param, yql_output_path, log
from yql_ports import get_yql_port
from devtools.swag.daemon import run_daemon

MONGO_TGZ = 'mongodb-linux-x86_64-ubuntu1204-3.2.1.tgz'

MONGO_CONFIG_TEMPLATE = '''
storage:
    dbPath: %(working_dir)s
    engine: wiredTiger

net:
    port: %(port)s
    bindIp: ::1
    ipv6: true

systemLog:
    destination: file
    path: %(working_dir)s/mongo.log
    logAppend: true
    verbosity: 5

'''


@pytest.fixture(scope='module')
def mongo(request):
    mongo_dir = tempfile.mkdtemp(prefix='mongo_', dir=yatest.common.work_path())
    working_dir = '%s_%s' % (yql_output_path('mongo'), uuid.uuid4())
    os.mkdir(working_dir, 0o755)

    tgz = tarfile.open(MONGO_TGZ)
    tgz.extractall(path=mongo_dir)

    bin_dir = os.path.join(mongo_dir, 'mongodb-linux-x86_64-ubuntu1204-3.2.1', 'bin')
    port = get_yql_port()
    mongod_path = os.path.join(bin_dir, 'mongod')

    if get_param('system_mongo'):
        mongod_path = subprocess.check_output('which mongod', shell=True).strip()

    config_file = os.path.join(working_dir, 'mongo.cfg')
    with open(config_file, 'w') as f:
        f.write(MONGO_CONFIG_TEMPLATE % locals())

    log('Starting mongo at %s' % working_dir)

    cmd = [mongod_path, '--config', config_file]
    server = run_daemon(
        cmd,
        cwd=working_dir
    )
    time.sleep(2)  # TODO: Daemon.check_run

    def stopper():
        try:
            server.stop(kill=True)
        except Exception:
            pass

    request.addfinalizer(stopper)
    server.port = port
    return server
