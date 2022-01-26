import os
import time
import shutil
import tarfile
import tempfile
import thread
import traceback
import uuid
import SimpleHTTPServer
import SocketServer
from getpass import getuser

import pytest
import yatest.common
import yql_utils

from jre import get_java_executable
from devtools.swag.daemon import run_daemon
from yql_ports import get_yql_port
from yql_worker import YQLWorker
from yql_py_cli import yql_exec
from yql.client.request import YqlApiRequestBase
from yql.config import config

from yt_runner import YT_TOKEN, YT_SERVER_HOST, YT_SERVER_PORT

from google.protobuf import text_format
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

YQLAPI_PATH = yql_utils.yql_binary_path('yql/api')
YT_MRJOB_PATH = yql_utils.yql_binary_path('yql/tools/mrjob/mrjob')


class YqlApiReadyRequest(YqlApiRequestBase):
    def __init__(self):
        super(YqlApiReadyRequest, self).__init__('ready_to_test')

    @property
    def base_url(self):
        cut = len(config.base_url) - len('api/v1/')
        return super(YqlApiReadyRequest, self).base_url[:cut]


class YQLAPI(object):
    def __init__(self, mongo, tmpdir_module, yt=None, kikimr=None):
        self.ready = False
        self.mongo = mongo
        self.tmpdir_module = tmpdir_module
        self.workers = []
        self.yt = yt
        self.kikimr = kikimr

        self.udfs_path = yql_utils.get_udfs_path()

        self.config_path = yql_utils.yql_output_path('yql_configs')
        if not os.path.isdir(self.config_path):
            os.mkdir(self.config_path)

        self.gateway_config = gateways_config_pb2.TGatewaysConfig()
        if not yql_utils.get_param('LOCAL_BENCH'):
            os.environ['YQL_DETERMINISTIC_MODE'] = '1'  # for temp tables ids

        # TODO
        os.environ['YQL_ALLOW_LOCAL_FILES'] = '1'

        self.add_external_gateway_config = yql_utils.get_param('add_external_gateway_config')
        self.external_gateway_config_path = yql_utils.get_param('external_gateway_config_path')
        self.filestorage_config_file = None
        if self.external_gateway_config_path is None:
            try:
                self.external_gateway_config_path = yql_utils.yql_source_path('yql/cfg/local/gateways.conf')
                self.filestorage_config_file = yql_utils.yql_source_path('yql/cfg/local/fs.conf')
            except Exception:
                pass
        if yql_utils.get_param('LOCAL_BENCH'):
            assert os.path.exists(self.external_gateway_config_path), \
                'Gateway config not found (%s)' % self.external_gateway_config_path
            with open(self.external_gateway_config_path) as f:
                text_format.Merge(f.read(), self.gateway_config)

            # patching agent path and other settings
            self.set_default_gateway_config(self.gateway_config.Yt)
            self.set_yt_params(self.gateway_config.Yt)
            self.try_set_local_yt_token(self.gateway_config.Yt)

        if self.yt:
            self.set_local_yt_params()

        if self.kikimr:
            self.set_local_kikimr_params()

        with open(os.path.join(self.config_path, 'gateways.conf'), 'w') as f:
            f.write(str(self.gateway_config))

        self.default_provider = None
        if self.yt is not None:
            self.default_provider = yt
        elif self.kikimr is not None:
            self.default_provider = kikimr

        self.start_http_server()

    http_server_port = None
    httpd = None

    def start_http_server(self):
        self.http_server_port = get_yql_port()
        handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        self.httpd = SocketServer.TCPServer(('', self.http_server_port), handler, bind_and_activate=False)
        self.httpd.allow_reuse_address = True
        self.httpd.server_bind()
        self.httpd.server_activate()
        thread.start_new_thread(self.httpd.serve_forever, ())

    def set_local_kikimr_params(self):
        gateway_config = self.gateway_config.Kikimr

        default_settings = gateway_config.DefaultSettings.add()
        default_settings.Name = '_UseKqp'
        default_settings.Value = 'true'
        default_settings = gateway_config.DefaultSettings.add()
        default_settings.Name = '_RestrictModifyPermissions'
        default_settings.Value = 'false'
        default_settings = gateway_config.DefaultSettings.add()
        default_settings.Name = 'IsolationLevel'
        default_settings.Value = 'Serializable'
        default_settings = gateway_config.DefaultSettings.add()
        default_settings.Name = 'Profile'
        default_settings.Value = 'false'
        default_settings = gateway_config.DefaultSettings.add()
        default_settings.Name = 'UnwrapReadTableValues'
        default_settings.Value = 'false'

        mapping = gateway_config.ClusterMapping.add()
        for mapping in gateway_config.ClusterMapping:
            mapping.Default = False
        mapping.Name = 'local_kikimr'
        mbus = mapping.MessageBus.add()
        mbus.Host = 'localhost'
        mbus.Port = self.kikimr.kikimr.nodes[1].port
        mapping.Default = True

    def set_local_yt_params(self):
        gateway_config = self.gateway_config.Yt
        self.set_default_gateway_config(gateway_config)
        self.set_yt_params(gateway_config)

        for mapping in gateway_config.ClusterMapping:
            mapping.Default = False
        has_clusters = (len(gateway_config.ClusterMapping) != 0)
        mapping = gateway_config.ClusterMapping.add()
        mapping.Name = 'local_yt' if has_clusters else 'plato'
        mapping.Cluster = YT_SERVER_HOST + ':' + \
            str(self.yt.yt_proxy_port if YT_SERVER_PORT is None else YT_SERVER_PORT)
        mapping.Default = True
        if YT_TOKEN:
            mapping.YTToken = YT_TOKEN

    def set_default_gateway_config(self, config):
        config.GatewayThreads = 0
        config.YtLogLevel = 2  # == YL_INFO. Level YL_DEBUG is too noisy

    def set_yt_params(self, config):
        config.MrJobBin = YT_MRJOB_PATH
        attr = config.DefaultSettings.add()
        attr.Name = 'KeepTempTables'
        attr.Value = 'no'

    def try_set_local_yt_token(self, config):
        token = os.environ.get('YT_TOKEN')
        if token:
            for cluster in config.ClusterMapping:
                cluster.YTToken = token

    def set_table_prefix(self, prefix):
        self.default_provider.table_prefix = prefix

    working_dir = None
    http_port = None
    port = None
    inspector_server_port = None
    server = None

    def start(self):
        api_dir = tempfile.mkdtemp(prefix='yql_api_', dir=yatest.common.work_path())
        self.working_dir = '%s_%s' % (yql_utils.yql_output_path('yql_api'), uuid.uuid4())
        os.mkdir(self.working_dir, 0o755)

        if not yql_utils.get_param('LOCAL_BENCH_XX'):
            jars_tar_path = os.path.join(YQLAPI_PATH, 'yql-api.tar')
            jars_tar = tarfile.open(jars_tar_path)
            jars_tar.extractall(path=api_dir)
        else:
            jars_dir = os.path.join(YQLAPI_PATH, 'yql-api')
            for f in os.listdir(jars_dir):
                shutil.copy2(os.path.join(jars_dir, f), api_dir)

        yql_utils.log('Starting java api at %s' % self.working_dir)

        self.http_port = int(yql_utils.get_param('API_PORT', 0)) or get_yql_port('api')
        self.port = self.http_port
        self.inspector_server_port = get_yql_port()
        self.clickhouse_runner_port = get_yql_port('clickhouse_runner')

        proto_cfg = '''
            ClickHouse {
                DefaultRetries: 10
                DefaultDelay: 1000
                SystemRetries: 20
                SystemDelay: 20
                Threads: 2
                Runner {
                    Port: %(clickhouse_runner_port)d
                    Threads: 2
                }
            }
            HttpServer {
                Bind: '[::]'
                Port: %(http_port)d
                Threads: 0
            }
            InspectorServer {
                Port: %(inspector_server_port)d
                Threads: 4
            }
            MongoDb {
                Hosts: 'localhost'
                Port: %(mongo_port)d
                Name: 'yql_api'
                Username: ''
                Password: ''
                Threads: 2
            }
            YqlWorker {
                ClientThreads: 2
                MaxHeartbeatGap: { Value: 30  Unit: SECONDS }
                MaxHealthCheckGap: { Value: 20  Unit: HOURS }

                LoseUnhealthyWorkerAfter: { Value: 30  Unit: MINUTES }
            }
            BlackBox {
                Url: 'https://blackbox.yandex-team.ru'

                CacheTtl: { Value: 10  Unit: MINUTES }
            }

            Staff {
                Url: 'https://staff-api.yandex-team.ru'

                CacheTtl: { Value: 30  Unit: MINUTES }
            }
            OperationIdKey: 'secret-key'
            SystemYqlUsername: '%(user)s'
            OauthSecret: '<UNUSED>'
            SystemOauthToken: '<UNUSED>'
        ''' % {
            'http_port': self.http_port,
            'clickhouse_runner_port': self.clickhouse_runner_port,
            'mongo_port': self.mongo.port,
            'configs_dir': self.config_path,
            'udfs_dir': self.udfs_path,
            'inspector_server_port': self.inspector_server_port,
            'user': getuser(),
        }

        cfg_file = os.path.join(self.working_dir, 'api.cfg')
        with open(cfg_file, 'w') as file_handle:
            file_handle.write(proto_cfg)

        self.server = run_daemon(
            command=[
                get_java_executable(),
                '-Xmx768m', '-XX:NewRatio=2', '-Xms256m', '-server', '-showversion',
                '-classpath', ':'.join([os.path.join(api_dir, f) for f in os.listdir(api_dir) if f.endswith('.jar')]),
                '-Dfile.encoding=UTF-8',
                '-Djava.io.tmpdir=%s' % self.tmpdir_module,
                '-Djava.net.preferIPv6Addresses=true',
                '-Dapp.package=yandex-yql-api-service',
                '-Dyandex.environment.type=' + ('development' if yql_utils.get_param('LOCAL_BENCH') else 'tests'),
                '-Dyql-api.log.dir=' + self.working_dir,
                'ru.yandex.yql.YqlApiService',
                '--cfg', cfg_file,
                '--gateways-cfg', os.path.join(self.config_path, 'gateways.conf'),
            ],
            cwd=self.working_dir,
            env={'yandex.environment.type': 'TESTS'}
        )

    def start_worker(self):
        worker = YQLWorker(
            gateways_config_file=os.path.join(self.config_path, 'gateways.conf'),
            inspector_server_port=self.inspector_server_port,
            udfs_dir=self.udfs_path,
            filestorage_config_file=self.filestorage_config_file,
        )
        self.workers.append(worker)
        worker.run()

    def check_process(self, process, name):
        if not process.is_alive():
            reason = '%s is dead' % name
            yql_utils.log(reason)
            process.raise_on_death(reason)

    def check_alive(self):
        self.check_process(self.server, 'API')
        self.check_process(self.mongo, 'Mongo')
        for worker in self.workers:
            self.check_process(worker, 'Worker')

    def wait_ready(self):
        config.server = 'localhost'
        config.port = self.port
        config.token = 'x' * 32

        request = YqlApiReadyRequest()
        # 2 mins to start
        for i in range(120):
            request.run(force=True)
            if request.is_ok:
                self.ready = True
                return
            self.check_alive()
            yql_utils.log('YQL API not ready: %s' % request.status_code)
            time.sleep(1)
        raise Exception('YQL API failed to initialize: %s' % request.status_code)

    def stop(self):
        exceptions = []

        try:
            self.server.stop(kill=True)
        except Exception:
            exceptions.append(traceback.format_exc())
        for worker in self.workers:
            try:
                worker.stop()
            except Exception:
                exceptions.append(traceback.format_exc())
        try:
            self.httpd.shutdown()
        except Exception:
            exceptions.append(traceback.format_exc())
        if exceptions and self.ready:
            raise Exception(
                'Errors on teardown:\n\n' + '\n'.join(exceptions)
            )

    def write_tables(self, tables):
        for table in tables:
            if table.exists:
                yql_utils.log('Writing provider table ' + table.name)
                self.default_provider.write_table(
                    name=table.name,
                    content=table.content,
                    attrs=table.attr,
                    format=table.format
                )

    def get_tables(self, tables):
        res = {}
        for table in tables:
            yson = self.default_provider.read_table(table.name)
            attr = self.default_provider.get_important_metaattrs(table.name)
            res[table.full_name] = yql_utils.new_table(table.full_name, content=yson, attr=attr)

            yql_utils.log('YQL API table ' + table.full_name)
            yql_utils.log(res[table.full_name].content)

        return res

    def yql_exec(self, program=None, program_file=None, files=None, urls=None, run_sql=False,
                 run_mkql=False, verbose=False, check_error=True, tables=None, args=None, async=False):

        res_dir = yql_utils.get_yql_dir(prefix='yql_exec_')
        if run_sql:
            lang = 'sql'
        elif run_mkql:
            lang = 'mkql'
        else:
            lang = 'yql'

        program, program_file = self.default_provider.prepare_program(program, program_file, res_dir, lang=lang)

        _files = {}
        if files is not None:
            for f in files:
                with open(files[f]) as fd:
                    try:
                        fd.read().encode('utf-8')
                    except UnicodeDecodeError:
                        if os.path.exists(os.path.basename(files[f])):
                            os.remove(os.path.basename(files[f]))
                        shutil.copy2(files[f], os.path.basename(files[f]))
                        if urls is None:
                            urls = {}
                        urls[f] = 'http://localhost:%d/%s' % (
                            self.http_server_port,
                            os.path.basename(files[f])
                        )
                    else:
                        _files[f] = files[f]

        return yql_exec(
            server='localhost',
            port=self.port,
            token='x' * 32,
            program=program,
            files=_files,
            urls=urls,
            run_sql=run_sql,
            run_mkql=run_mkql,
            verbose=verbose,
            check_error=check_error,
            attrs={'test_prefix': self.default_provider.table_prefix} if self.default_provider.table_prefix and self.default_provider.table_prefix != '//' else {}
        )


@pytest.fixture(scope='module')
def yql_api(request, mongo, tmpdir_module):
    try:
        yt = request.getfuncargvalue('yt')
    except BaseException:
        yt = None
    try:
        kikimr = request.getfuncargvalue('kikimr')
    except BaseException:
        kikimr = None

    api = YQLAPI(mongo=mongo, tmpdir_module=tmpdir_module, yt=yt, kikimr=kikimr)
    api.start()
    api.start_worker()
    request.addfinalizer(api.stop)
    api.wait_ready()

    yql_utils.log('Java api started. Working dir: %s. YqlWorker dir: %s' %
                  (api.working_dir, api.workers[0].working_dir))

    return api
