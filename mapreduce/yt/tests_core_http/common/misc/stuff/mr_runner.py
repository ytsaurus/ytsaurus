import json
import os
import shlex
import pytest
import shutil
import tempfile
import urllib2

import yatest.common
import time

from daemon import run_daemon  # devtools/swag/daemon
from yql_ports import get_yql_port
from yql_utils import *

try:
    from yql_runner import KiKiMRForYQL
except ImportError:
    KiKiMRForYQL = object

MAPREDUCE_PATH = yql_binary_path('quality/mapreduce/mapreduce')


def mapreduce_fixture(request, tmpdir_module, cluster_name=None):
    # mapreduce -runserver 9800 -http 9998 -log server.log
    # mapreduce -port 9801 -runfileserv localhost:9800 -log fileserv.log
    # mapreduce -runhost localhost:9800 --log=host.log

    # mapreduce -server localhost:9800 -drop Output
    # mapreduce -server localhost:9800 -drop Input
    # mapreduce -server localhost:9800 -write Output -replicas=1 -subkey < input.txt

    # mapreduce -server localhost:9800 -read Output -subkey

    ports = {
        'SERVER_PORT': get_yql_port('yamr'),
        'HTTP_PORT': get_yql_port(),
        'FILESERV_PORT': get_yql_port(),
        'NETWORK_BATCHER_PORT': get_yql_port(),
        'HOST_HTTP_PORT': get_yql_port()
    }

    working_dir = tempfile.mkdtemp(
        prefix='mapreduce_' + ((cluster_name + '_') if cluster_name else ''),
        dir=tmpdir_module
    )

    def run_mr_daemon(cmd):
        log(cmd % ports)
        return run_daemon(
            MAPREDUCE_PATH + ' ' + cmd % ports,
            cwd=working_dir,
            check_exit_code=False
        )

    with open(
        os.path.join(
            working_dir,
            'server.cfg'
        ),
        'w'
    ) as mr_cfg_file:
        mr_cfg_file.write(
            '<Main>\nUseSeparateUserForJobs=false\nMaxJobMemoryLimit = 23089744183296\nMaxUserProcessCoreLimit = 23089744183296\nChunkReplicas = 1\nNetworkBatcherPort = %d\nMasterStateUpdateIntervalInMilliseconds = 100\n</Main>' %
            ports['NETWORK_BATCHER_PORT'])
    with open(
        os.path.join(
            working_dir,
            'fileserv.cfg'
        ),
        'w'
    ) as mr_cfg_file:
        mr_cfg_file.write('<Main>\nChunkReplicas = 1\n</Main>')

    server = run_mr_daemon('-runserver %(SERVER_PORT)d -http %(HTTP_PORT)d')
    host = run_mr_daemon('-runhost localhost:%(SERVER_PORT)d -http %(HOST_HTTP_PORT)d')
    fileserv = run_mr_daemon('-port %(FILESERV_PORT)d -runfileserv localhost:%(SERVER_PORT)d')

    def fin():
        exceptions = []
        for service in fileserv, host, server:
            try:
                service.stop()
            except Exception as e:
                exceptions.append(str(e))
        try:
            output_dir = yql_output_path('.')
            if not os.path.isdir(output_dir):
                os.mkdir(output_dir)
            mapreduce_logs = os.path.join(output_dir, 'mapreduce_logs' +
                                          (('_' + cluster_name) if cluster_name else ''))
            shutil.copytree(working_dir, mapreduce_logs)
            os.system('chmod -R 0775 ' + mapreduce_logs)
        except Exception as e:
            exceptions.append(str(e))

        assert not exceptions, 'While stopping mapreduce: ' + '\n\n'.join(exceptions)

    request.addfinalizer(fin)

    def mr_command(cmd, stdin=None):
        if stdin is not None:
            stdinf = tempfile.NamedTemporaryFile(delete=False)
            stdinf.write(stdin)
            stdinf.close()
            stdinf = open(stdinf.name)  # TODO
        else:
            stdinf = None
        return yatest.common.execute(
            shlex.split(MAPREDUCE_PATH + ' ' + cmd + ' -server localhost:%(SERVER_PORT)d' % ports),
            cwd=working_dir,
            stdin=stdinf,
        )

    mr_command.server = server
    mr_command.host = host
    mr_command.ports = ports

    mr_command.write_metaattrs = lambda name, metaattrs: mr_command(
        '-writemetaattrs %s -fs==' % name,
        stdin=metaattrs.strip() + '\n'
    )

    def write_table(name, content, attrs=None, format='csv'):
        if format != 'csv':
            pytest.skip('Can not write table in %s format to mapreduce' % format)
        mr_command('-write %s -subkey -fs=;' % name, stdin=content) if content.strip() else None
        if attrs is not None:
            mr_command.write_metaattrs(name, attrs)

    def sort_table(name):
        mr_command('-sort %s' % name)

    mr_command.sort_table = sort_table
    mr_command.write_table = write_table
    mr_command.read_table = lambda name: mr_command('-read %s -subkey -fs=;' % name).std_out.strip()
    mr_command.remove_table = lambda name: mr_command('-drop ' + name)

    def list_tables(prefix=None):
        if not prefix or len(prefix) == 0:
            return mr_command('-list').std_out.strip().splitlines()
        else:
            cmd = '-list -prefix %s' % prefix
            return mr_command(cmd).std_out.strip().splitlines()

    mr_command.list_tables = list_tables

    def drop_all():
        res = mr_command('-list')
        for table in res.std_out.strip().splitlines():
            mr_command.remove_table(table)

    mr_command.drop_all = drop_all

    def get_user_transactions():
        resp = urllib2.urlopen('http://localhost:%d/json?info=transactionlist' % ports['HTTP_PORT']).read()
        resp_json = json.loads(resp)
        try:
            log(resp_json['transactionInfo']['transactionList'])
            return resp_json['transactionInfo']['transactionList']
        except KeyError:
            log('No transactions')
            return []

    def is_transaction_cancelled(transaction):
        transaction_id = transaction['id']
        history = urllib2.urlopen('http://localhost:%d/json?info=history' % ports['HTTP_PORT']).read()
        history = json.loads(history)
        for history_transaction in history['transactionLog']['transactionsList']:
            if history_transaction['id'] == transaction_id:
                return history_transaction['state'] == 'Canceled'
        assert 0, 'Can not find transaction %r in history %r' % (transaction, history)

    def get_meta():
        meta = urllib2.urlopen('http://localhost:%d/json?info=main' % ports['HTTP_PORT']).read()
        meta = json.loads(meta)
        return meta

    mr_command.get_meta = get_meta

    mr_command.get_user_transactions = get_user_transactions
    mr_command.is_transaction_cancelled = is_transaction_cancelled

    mr_command.ports = ports

    return mr_command


class KiKiMRWithMR(KiKiMRForYQL):

    def __init__(self, mapreduce, *args, **kwargs):
        kwargs.pop('mr_service', None)
        super(KiKiMRWithMR, self).__init__(mr_service=mapreduce, *args, **kwargs)
        self.mapreduce = mapreduce
        gateway_config = self.gateway_config.Yamr
        self.set_default_gateway_config(gateway_config)
        self.set_yamr_params(gateway_config)
        gateway_config.MaxTableCacheCount = 10
        gateway_config.MaxTableCacheSize = 1024
        gateway_config.MaxTableSizeToCache = 1024

        for mapping in gateway_config.ClusterMapping:
            mapping.Default = False
        has_clusters = (len(gateway_config.ClusterMapping) != 0)

        mapping = gateway_config.ClusterMapping.add()
        mapping.Name = 'local_yamr' if has_clusters else 'cedar'
        mapping.User = 'mapreduce'
        mapping.JsonApiAddr = 'http://localhost:%d/json' % self.mapreduce.ports['HTTP_PORT']
        mapping.Cluster = 'localhost:%d' % self.mapreduce.ports['SERVER_PORT']
        mapping.Default = True


@pytest.fixture(scope='module')
def mapreduce(request, tmpdir_module):
    return mapreduce_fixture(request, tmpdir_module)


@pytest.fixture(scope='module')
def kikimr_with_mr(request, tmpdir_module, mapreduce):

    kikimr_instance = KiKiMRWithMR(mapreduce)
    kikimr_instance.start()
    kikimr_instance.create_yql_pool()

    request.addfinalizer(kikimr_instance.stop)

    return kikimr_instance
