import json

import pytest
import yt
from yt.wrapper.cypress_commands import create
from yt.wrapper.file_commands import write_file
from yt.wrapper.spark import build_spark_operation_spec, find_spark_cluster, SparkDiscovery, _read_launch_config, \
    _parse_memory, _format_memory
from yt.wrapper.spec_builders import VanillaSpecBuilder


@pytest.mark.usefixtures('yt_env_with_rpc')
class TestSpark(object):
    @property
    def config(self):
        return {
            'port_max_retries': 200,
            'shuffle_service_port': 27000,
            'spark_launcher_name': 'spark-test.jar',
            'spark_name': 'spark-test',
            'spark_yt_base_path': '//base/path',
            'start_port': 27001
        }

    def test_build_spark_operation_spec(self):
        if yt.wrapper.config["backend"] == "native" or yt.wrapper.config["backend"] == "rpc":
            return
        actual = build_spark_operation_spec(operation_alias='test_alias',
                                            spark_discovery=SparkDiscovery('//tmp/spark-discovery'),
                                            dynamic_config=self.config,
                                            spark_worker_core_count=1,
                                            spark_worker_memory_limit=2147483648,
                                            spark_worker_count=1,
                                            spark_worker_tmpfs_limit=2147483648,
                                            spark_master_memory_limit=2147483648,
                                            spark_history_server_memory_limit=2147483648,
                                            pool='test_pool')

        task_spec = {
            'environment': {'IS_SPARK_CLUSTER': 'true',
                            'JAVA_HOME': '/opt/jdk8',
                            'SPARK_DISCOVERY_PATH': '//tmp/spark-discovery/discovery',
                            'SPARK_HOME': 'spark-test'},
            'file_paths': ['//base/path/spark-test.tgz', '//base/path/spark-test.jar'],
            'layer_paths': ['//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz',
                            '//home/sashbel/delta/python/layer_with_python37.tar.gz',
                            '//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz'],
            'memory_reserve_factor': 1.0,
            'restart_completed_jobs': True
        }

        expected = VanillaSpecBuilder() \
            .begin_task('master') \
            .job_count(1) \
            .command('tar --warning=no-unknown-keyword -xf spark-test.tgz && '
                     '/opt/jdk8/bin/java -Xmx512m -cp spark-test.jar ru.yandex.spark.launcher.MasterLauncher '
                     '--port 27001 --opts "\'-Dspark.master.rest.enabled=true -Dspark.master.rest.port=27001 '
                     '-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem -Dspark.port.maxRetries=200 '
                     '-Dspark.shuffle.service.port=27000 \'" --operation-id $YT_OPERATION_ID --web-ui-port 27001') \
            .memory_limit(2147483648) \
            .cpu_limit(2) \
            .end_task() \
            .begin_task('history') \
            .job_count(1) \
            .command('tar --warning=no-unknown-keyword -xf spark-test.tgz && '
                     '/opt/jdk8/bin/java -Xmx512m -cp spark-test.jar ru.yandex.spark.launcher.HistoryServerLauncher '
                     '--port 27001 --opts "\'-Dspark.history.fs.cleaner.enabled=true '
                     '-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem '
                     '-Dspark.port.maxRetries=200 -Dspark.shuffle.service.port=27000 \'" '
                     '--log-path yt:///tmp/spark-discovery/logs/event_log') \
            .memory_limit(2147483648) \
            .cpu_limit(1) \
            .end_task() \
            .begin_task('workers') \
            .job_count(1) \
            .command('tar --warning=no-unknown-keyword -xf spark-test.tgz && '
                     '/opt/jdk8/bin/java -Xmx512m -cp spark-test.jar ru.yandex.spark.launcher.WorkerLauncher '
                     '--port 27001 --opts "\'-Dspark.worker.cleanup.enabled=true -Dspark.shuffle.service.enabled=true '
                     '-Dspark.hadoop.fs.yt.impl=ru.yandex.spark.yt.fs.YtFileSystem -Dspark.port.maxRetries=200 '
                     '-Dspark.shuffle.service.port=27000 \'" --cores 1 --memory 2G --web-ui-port 27001') \
            .memory_limit(4294967296) \
            .cpu_limit(3) \
            .tmpfs_path('tmpfs') \
            .end_task() \
            .secure_vault({'YT_TOKEN': None, 'YT_USER': 'root'}) \
            .max_failed_job_count(5) \
            .max_stderr_count(150) \
            .title('test_alias') \
            .spec({
                'annotations': {'is_spark': True},
                'pool': 'test_pool',
                'stderr_table_path': '//tmp/spark-discovery/logs/stderr'
            })

        actual_tasks = {}
        for task_name, task in actual._spec['tasks'].items():
            del task._user_spec["environment"]["YT_PROXY"]  # exclude YT_PROXY from assert because of random port
            actual_tasks[task_name] = (task._spec, task._user_spec)
        expected_tasks = dict([(k, (v._spec, task_spec)) for k, v in expected._spec['tasks'].items()])
        assert actual_tasks == expected_tasks

        actual._spec['tasks'] = {}
        expected._spec['tasks'] = {}
        assert actual._spec == expected._spec
        assert actual._user_spec == expected._user_spec

    def test_find_spark_cluster(self):
        create("map_node", "//tmp/spark-discovery/discovery/spark_address/host1:1", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/webui/host1:2", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/rest/host1:3", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/operation/1234-5678", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/shs/host2:4", recursive=True)

        res = find_spark_cluster('//tmp/spark-discovery')
        assert res.master_endpoint == "host1:1"
        assert res.master_web_ui_url == "host1:2"
        assert res.master_rest_endpoint == "host1:3"
        assert res.operation_id == "1234-5678"
        assert res.shs_url == "host2:4"

    def test_read_launch_config(self):
        path = "//tmp/spark-launch.json"
        write_file(path, json.dumps(self.config).encode('utf-8'))
        res = _read_launch_config(path)
        assert res == self.config

    def test_parse_memory(self):
        assert _parse_memory(128) == 128
        assert _parse_memory("128") == 128
        assert _parse_memory("256") == 256
        assert _parse_memory("256b") == 256
        assert _parse_memory("256B") == 256
        assert _parse_memory("128k") == 128 * 1024
        assert _parse_memory("256k") == 256 * 1024
        assert _parse_memory("256K") == 256 * 1024
        assert _parse_memory("128kb") == 128 * 1024
        assert _parse_memory("256kb") == 256 * 1024
        assert _parse_memory("256Kb") == 256 * 1024
        assert _parse_memory("256KB") == 256 * 1024
        assert _parse_memory("256m") == 256 * 1024 * 1024
        assert _parse_memory("256M") == 256 * 1024 * 1024
        assert _parse_memory("256mb") == 256 * 1024 * 1024
        assert _parse_memory("256Mb") == 256 * 1024 * 1024
        assert _parse_memory("256MB") == 256 * 1024 * 1024
        assert _parse_memory("256g") == 256 * 1024 * 1024 * 1024
        assert _parse_memory("256G") == 256 * 1024 * 1024 * 1024
        assert _parse_memory("256gb") == 256 * 1024 * 1024 * 1024
        assert _parse_memory("256Gb") == 256 * 1024 * 1024 * 1024
        assert _parse_memory("256GB") == 256 * 1024 * 1024 * 1024

    def test_format_memory(self):
        assert _format_memory(128) == "128B"
        assert _format_memory(256) == "256B"
        assert _format_memory(256 * 1024) == "256K"
        assert _format_memory(128 * 1024) == "128K"
        assert _format_memory(256 * 1024 * 1024) == "256M"
        assert _format_memory(256 * 1024 * 1024 * 1024) == "256G"
