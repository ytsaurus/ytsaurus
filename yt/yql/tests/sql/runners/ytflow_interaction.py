import json
import os
import re
import pytest
import yatest.common

from google.protobuf.text_format import Parse
from google.protobuf.text_format import MessageToString

import yql.essentials.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

from library.python.port_manager import PortManager
from yql_utils import KSV_ATTR, execute_sql, get_supported_providers, get_test_prefix, log, is_xfail, \
    normalize_source_code_path, get_tables, \
    get_files, get_http_files, get_yt_files, get_pragmas, skip_test_if_required

from test_utils import get_config, get_parameters_json, get_case_file, replace_vars

from yt.yql.tests.sql.runners.common import DATA_PATH, resolve_langver
from yt.yql.tests.sql.runners.yt_setup import upload_yt_files

from yt.yson import load as yson_load, dump as yson_dump
from yt.yson.convert import yson_to_json

from yt.yql.tests.common.test_framework.test_utils import (
    wait_pipeline_state_or_failed_jobs,
    create_flow_logs_replicators,
    dump_pipeline_jobs_stderr,
    wait_for_debug,
    FlowDebugHelper,
)

from yt.wrapper.flow_commands import PipelineState


YSON_STRUCT_DIFF_PATH = yatest.common.binary_path('yt/yql/tests/sql/ytflow/yson_struct_diff/yson_struct_diff')
YTFLOW_WORKER_PATH = yatest.common.binary_path('yt/yql/tools/ytflow_worker/ytflow_worker')

VOLATILE_COLUMNS = ['$timestamp', '$cumulative_data_weight']


def build_extra_gateway_config(yt):
    flow_debug_requested = os.getenv("DEBUG_FLOW_OUTPUT_DIRECTORY") is None
    output_path = os.path.join(yatest.common.output_path(), "ytflow_logs")
    os.makedirs(output_path, exist_ok=True)

    return '''
Yt {
    DefaultSettings {
        Name: "_ParseExpressionColumns"
        Value: "true"
    }
}

Ytflow {
    YtflowWorkerBin: "%s"
    GatewayThreads: 1

    ClusterMapping {
        Name: "plato"
        RealName: "plato"
        ProxyUrl: "%s"
        Token: "does_not_matter"
    }

    DefaultSettings {
        Name: "_RpcTimeout"
        Value: "10s"
    }

    DefaultSettings {
        Name: "_MasterLockTimeout"
        Value: "2m"
    }

    DefaultSettings {
        Name: "_MasterLockPingPeriod"
        Value: "30s"
    }

    DefaultSettings {
        Name: "_FiniteStreams"
        Value: "%s"
    }

    DefaultSettings {
        Name: "UpdateTimeout"
        Value: "600s"
    }

    DefaultSettings {
        Name: "ControllerCount"
        Value: "1"
    }

    DefaultSettings {
        Name: "ControllerCpuLimit"
        Value: "1.0"
    }

    DefaultSettings {
        Name: "ControllerMemoryLimit"
        Value: "1G"
    }

    DefaultSettings {
        Name: "WorkerCount"
        Value: "1"
    }

    DefaultSettings {
        Name: "WorkerCpuLimit"
        Value: "1.0"
    }

    DefaultSettings {
        Name: "WorkerMemoryLimit"
        Value: "1G"
    }

    DefaultSettings {
        Name: "LookupJoinInflightRowLimit"
        Value: "2"
    }

    DefaultSettings {
        Name: "LookupJoinInflightLookupLimit"
        Value: "2"
    }

    DefaultSettings {
        Name: "LookupJoinLookupTimeout"
        Value: "1s"
    }

    DefaultSettings {
        Name: "_UseCpuAwareBalancer"
        Value: "false"
    }

    DefaultSettings {
        Name: "_ControllerWriteFullLogsToYT"
        Value: "true"
    }

    DefaultSettings {
        Name: "_ControllerWriteLogsToFile"
        Value: "false"
    }

    DefaultSettings {
        Name: "_ControllerLogLevel"
        Value: "debug"
    }

    DefaultSettings {
        Name: "_ControllerEnableStderrLogging"
        Value: "false"
    }

    DefaultSettings {
        Name: "_WorkerWriteLogsToYT"
        Value: "true"
    }

    DefaultSettings {
        Name: "_WorkerWriteLogsToFile"
        Value: "false"
    }

    DefaultSettings {
        Name: "_WorkerLogLevel"
        Value: "debug"
    }

    DefaultSettings {
        Name: "_WorkerEnableStderrLogging"
        Value: "false"
    }

    DefaultSettings {
        Name: "_LogsDirectory"
        Value: "logs"
    }

    DefaultSettings {
        Name: "_SwitchComputationNodeBufferSizeBytes"
        Value: "0"
    }

    DefaultSettings {
        Name: "_RunVanillaOperation"
        Value: "%s"
    }

    DefaultSettings {
        Name: "_DumpPipelineSpecToDirectory"
        Value: "%s"
    }
}
''' % (YTFLOW_WORKER_PATH, yt.get_server(), flow_debug_requested, flow_debug_requested, output_path)


def setup_yql_debug_environment(query, query_destination_path, gateways_destination_path, yql_api):
    gateways_config = gateways_config_pb2.TGatewaysConfig()
    with open(os.path.join(yql_api.config_path, "gateways.conf"), "r") as f:
        Parse(f.read(), gateways_config)

    for cluster in gateways_config.Yt.ClusterMapping:
        cluster.YTToken = "does_not_matter"

    with open(gateways_destination_path, "w") as f:
        f.write(MessageToString(gateways_config))

    with open(query_destination_path, "w") as f:
        f.write(query)

    wait_for_debug()


def get_extra_result_files(config):
    extra_result_files = []

    for line in config:
        if line[0] == 'res':
            extra_result_files.append(line[1])

    return extra_result_files


def setup_yt_cluster(yt, cluster_name):
    cluster_connection = yt.yt_client.get('//sys/@cluster_connection')

    batch_client = yt.yt_client.create_batch_client(raise_errors=True)
    batch_client.set("//sys/@cluster_name", cluster_name)
    batch_client.set("//sys/clusters", {cluster_name: cluster_connection})
    batch_client.commit_batch()

    yt.yt_client.config['proxy']['aliases'] = {cluster_name: yt.get_server()}


def abort_ytflow_operation(pipeline_path, yt_client):
    vanilla_info = yt_client.get_attribute(
        pipeline_path, "_yql_ytflow_vanilla_info", None)

    if vanilla_info is None:
        log("Ytflow vanilla info attribute not found for pipeline path: " + pipeline_path)
        return

    operation_id = vanilla_info['operation_id']
    if operation_id not in map(lambda operation: operation['id'], yt_client.list_operations()['operations']):
        log(f"Ytflow vanilla operation doesn't exist (id: {operation_id})")
        return

    yt_client.abort_operation(operation_id)
    assert yt_client.get_operation_state(operation_id).is_finished()
    log(f"Aborted ytflow vanilla operation: {operation_id}")


def run_test(provider, prepare, suite, case, cfg, tmpdir, tmpdir_module, mongo, yt, yql_api_custom):
    yql_api = yql_api_custom

    setup_yt_cluster(yt, "plato")

    yql_api.default_provider = yt
    yql_api.httpd.forget_files()

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    skip_test_if_required(config)

    if provider not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % provider)

    test_id = suite + '-' + case + '-' + cfg
    log('===' + test_id)

    files = get_files(suite, config, DATA_PATH)
    http_files = get_http_files(suite, config, DATA_PATH)
    http_files_urls = yql_api.httpd.register_files({}, http_files)
    pragmas = get_pragmas(config)

    yt_files = get_yt_files(suite, config, DATA_PATH)
    upload_yt_files(yt, yt_files)

    extra_result_files = get_extra_result_files(config)

    sql_file = get_case_file(DATA_PATH, suite, case)
    with open(sql_file, encoding='utf-8') as program_file_descr:
        sql_query = prepare(program_file_descr.read())

    sort_outputs = False
    if '/* sort outputs */' in sql_query:
        sort_outputs = True

    with PortManager() as port_manager:
        pragmas.extend([
            f'pragma Ytflow.PathPrefix = "//{get_test_prefix()}/"',
            f'pragma Ytflow.ControllerRpcPort = "{port_manager.get_port()}"',
            f'pragma Ytflow.ControllerMonitoringPort = "{port_manager.get_port()}"',
            f'pragma Ytflow.WorkerRpcPort = "{port_manager.get_port()}"',
            f'pragma Ytflow.WorkerMonitoringPort = "{port_manager.get_port()}"',
            sql_query,
        ])

        sql_query = ';\n'.join(pragmas)
        sql_query = replace_vars(sql_query, 'yt_local_var')

        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
        xfail = is_xfail(config)

        langver = resolve_langver(config)

        parameters = get_parameters_json(suite, config, DATA_PATH)

        if provider + ' can not' in sql_query:
            pytest.skip(provider + ' can not execute this')

        yql_debug_directory = os.getenv("DEBUG_YQL_OUTPUT_DIRECTORY")
        if yql_debug_directory is not None:
            yql_api.write_tables(in_tables + out_tables)

            setup_yql_debug_environment(
                query=sql_query,
                query_destination_path=os.path.join(yql_debug_directory, "query.yql"),
                gateways_destination_path=os.path.join(yql_debug_directory, "gateways.conf"),
                yql_api=yql_api)

        # YT run
        yql_api.set_table_prefix('//' + get_test_prefix() + '/')

        yt_res, yt_tables_res = execute_sql(
            yql_api,
            program=sql_query,
            langver=langver,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            urls=http_files_urls,
            check_error=not xfail,
            verbose=True,
            pretty_plan=False,
            parameters=parameters,
        )

        output_logs_path = os.path.join(yatest.common.output_path(), "ytflow_logs")
        flow_debug_directory = os.getenv("DEBUG_FLOW_OUTPUT_DIRECTORY")
        if flow_debug_directory is not None:
            flowDebugHelper = FlowDebugHelper(
                "plato",
                yt.get_server(),
                yt.yt_client,
                pipeline_path="//" + get_test_prefix() + "/pipelines/test",
                ytflow_worker_bin_path=YTFLOW_WORKER_PATH,
                port_manager=port_manager)

            flowDebugHelper.setup_flow_debug_environment(
                os.path.join(output_logs_path, "setup_pipeline_spec_config.yson"),
                flow_debug_directory,
                controller_wait_retries=5,
                controller_retry_delay=5,
                flow_command_timeout=600)

        expected_error_substring = None
        if xfail:
            custom_error = re.search(r"/\* custom error: (.*) \*/", sql_query)
            if custom_error:
                expected_error_substring = custom_error.group(1)

        def handle_error(error_string):
            log('XFail errors: ' + error_string)
            if expected_error_substring is not None:
                log('expecting custom error: ' + expected_error_substring)
                assert expected_error_substring in error_string
                return

            return normalize_source_code_path(error_string)

        if yt_res.std_err and xfail:
            return handle_error(yt_res.std_err)

        pipeline_path = "//" + get_test_prefix() + "/pipelines/test"

        try:
            controller_logs_replicator, worker_logs_replicator = create_flow_logs_replicators(
                pipeline_path,
                output_logs_path,
                logs_batch_size=1000,
                output_file_prefix=test_id,
                yt_client=yql_api.yt.yt_client)

            with controller_logs_replicator, worker_logs_replicator:
                try:
                    wait_pipeline_state_or_failed_jobs(
                        PipelineState.Completed, pipeline_path,
                        client=yql_api.yt.yt_client,
                        timeout=600)

                finally:
                    dump_pipeline_jobs_stderr(
                        pipeline_path,
                        os.path.join(output_logs_path, f"{test_id}_pipeline_jobs.stderr"),
                        client=yql_api.yt.yt_client)

                    abort_ytflow_operation(pipeline_path, yql_api.yt.yt_client)

        except Exception as ex:
            if xfail:
                return handle_error(str(ex))

            raise

        if xfail:
            assert False, "Expected error didn't happen"

    yt_tables_res = yql_api.get_tables(out_tables)

    yt_res_yson = yt_res.results.get('data', [])

    to_canonize = []
    if os.path.exists(yt_res.results_file):
        with open(yt_res.results_file, 'w') as f:
            f.write(json.dumps(yson_to_json(yt_res_yson), sort_keys=True, ensure_ascii=False, indent=4))
        to_canonize.append(yatest.common.canonical_file(yt_res.results_file))

    for table in yt_tables_res:
        table_res_file = yt_tables_res[table].file

        if not os.path.exists(table_res_file):
            continue

        with open(table_res_file, 'rb') as file_:
            data = list(yson_load(file_, yson_type='list_fragment'))

        for item in data:
            for column in VOLATILE_COLUMNS:
                item.pop(column, None)

        if sort_outputs:
            data.sort(key=lambda item: json.dumps(yson_to_json(item), sort_keys=True))

        with open(table_res_file, 'wb') as file_:
            yson_dump(
                data, file_,
                yson_format='text',
                yson_type='list_fragment',
                sort_keys=True,
            )

        to_canonize.append(yatest.common.canonical_file(table_res_file))

    if os.path.exists(yt_res.plan_file):
        to_canonize.append(yatest.common.canonical_file(yt_res.plan_file))

    # TODO(ngc224): canonize yt-related ids somehow
    # if os.path.exists(yt_res.opt_file):
    #     to_canonize.append(yatest.common.canonical_file(yt_res.opt_file, diff_tool=ASTDIFF_PATH))

    if yt_res.std_err:
        to_canonize.append(normalize_source_code_path(yt_res.std_err))

    assert len(yql_api.workers) == 1
    yqlworker_working_dir = yql_api.workers[0].working_dir

    for extra_result_file in extra_result_files:
        to_canonize.append(
            yatest.common.canonical_file(
                os.path.join(yqlworker_working_dir, extra_result_file),
                diff_tool=[YSON_STRUCT_DIFF_PATH, 'NYT::NFlow::TPipelineSpec'],
            )
        )

    return to_canonize
