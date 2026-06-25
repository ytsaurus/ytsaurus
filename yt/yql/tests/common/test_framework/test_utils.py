import six
import time
import traceback
import threading
import os
import shutil
import subprocess

from datetime import datetime, timedelta

from google.protobuf.json_format import ParseDict
from google.protobuf.text_format import MessageToString

import yql.essentials.providers.common.proto.gateways_config_pb2 as gateways_config_pb2


def row_spec_to_yt_schema(row_spec):
    import yt.yson

    def toYtType(yqlType):
        while yqlType[0] == "TaggedType":
            yqlType = yqlType[2]

        required = True
        if yqlType[0] == "OptionalType":
            yqlType = yqlType[1]
            required = False

        if yqlType[0] != "DataType":
            return {"type": "any", "required": False}

        yqlType = yqlType[1]
        if yqlType in set(
            ["String", "Json", "JsonDocument", "Longint", "Uuid", "Decimal",
                "TzDate", "TzDatetime", "TzTimestamp", "TzDate32", "TzDatetime64", "TzTimestamp64", "DyNumber"]
                ):
            return {"type": "string", "required": required}
        elif yqlType == "Utf8":
            return {"type": "utf8", "required": required}
        elif yqlType == "Int64" or yqlType in ["Interval", "Datetime64", "Timestamp64", "Interval64"]:
            return {"type": "int64", "required": required}
        elif yqlType == "Int32" or yqlType == "Date32":
            return {"type": "int32", "required": required}
        elif yqlType == "Int16":
            return {"type": "int16", "required": required}
        elif yqlType == "Int8":
            return {"type": "int8", "required": required}
        elif yqlType == "Uint64" or yqlType == "Timestamp":
            return {"type": "uint64", "required": required}
        elif yqlType == "Uint32" or yqlType == "Datetime":
            return {"type": "uint32", "required": required}
        elif yqlType == "Uint16" or yqlType == "Date":
            return {"type": "uint16", "required": required}
        elif yqlType == "Uint8":
            return {"type": "uint8", "required": required}
        elif yqlType == "Double" or yqlType == "Float":
            return {"type": "double", "required": required}
        elif yqlType == "Bool":
            return {"type": "boolean", "required": required}
        elif yqlType == "Yson":
            return {"type": "any", "required": False}
        raise Exception("Unknown type %s" % yqlType)

    columns = {name: toYtType(yqlType) for name, yqlType in row_spec["Type"][1]}
    schema = yt.yson.YsonList()
    if 'SortedBy' in row_spec:
        for i in range(len(row_spec['SortedBy'])):
            column = row_spec['SortedBy'][i]
            sColumn = {'name': column, 'sort_order': 'ascending'}
            sColumn.update(toYtType(row_spec['SortedByTypes'][i]))
            schema.append(sColumn)
            columns.pop(column, None)
    for column in six.iterkeys(columns):
        sColumn = {'name': column}
        sColumn.update(columns[column])
        schema.append(sColumn)
    schema.attributes["strict"] = row_spec.get("StrictSchema", True)
    return schema


def infer_yt_schema(attrs):
    import yt.yson

    attrs = yt.yson.loads(attrs.encode())
    if 'schema' not in attrs and '_yql_row_spec' in attrs:
        attrs['schema'] = row_spec_to_yt_schema(attrs['_yql_row_spec'])

    return yt.yson.dumps(attrs, yson_format="pretty").decode()


def wait_pipeline_state_or_failed_jobs(
    target_state, pipeline_path,
    timeout=600,
    client=None,
):
    import yt.logger as logger

    from yt.common import YtError
    from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, flow_execute

    if target_state == PipelineState.Completed:
        target_states = {PipelineState.Completed, }
    elif target_state == PipelineState.Working:
        target_states = {PipelineState.Completed, PipelineState.Working}
    elif target_state == PipelineState.Stopped:
        target_states = {PipelineState.Completed, PipelineState.Stopped}
    elif target_state == PipelineState.Draining:
        target_states = {PipelineState.Completed, PipelineState.Stopped, PipelineState.Draining}
    elif target_state == PipelineState.Paused:
        target_states = {PipelineState.Completed, PipelineState.Stopped, PipelineState.Paused}
    elif target_state == PipelineState.Pausing:
        target_states = {PipelineState.Completed, PipelineState.Stopped, PipelineState.Paused, PipelineState.Pausing}
    else:
        logger.warning("Unknown pipeline state %s", target_state)
        return

    invalid_state_transitions = {
        PipelineState.Stopped: {PipelineState.Paused, },
    }

    deadline = datetime.now() + timedelta(seconds=timeout)

    while True:
        if datetime.now() > deadline:
            raise YtError("Wait timed out", attributes={"timeout": timeout})

        current_state = get_pipeline_state(
            pipeline_path=pipeline_path,
            timeout=timeout,
            client=client)

        if current_state in target_states:
            logger.info("Waiting finished (current state: %s, target state: %s)",
                        current_state, target_state)
            return

        if current_state in invalid_state_transitions.get(target_state, []):
            raise YtError("Invalid state transition", attributes={
                "current_state": current_state,
                "target_state": target_state})

        try:
            # TODO(ngc224): debug occasional computation-related method errors
            pipeline_info = flow_execute(
                pipeline_path=pipeline_path,
                flow_command="describe-pipeline",
                client=client)
        except Exception:
            pipeline_info = {
                "computations": {},
            }

        job_failed_errors = []
        for computation, computation_info in pipeline_info["computations"].items():
            for message in computation_info["messages"]:
                if not message["text"].startswith("Job failed"):
                    continue

                error = message["error"]
                error["attributes"]["computation"] = computation

                job_failed_errors.append(YtError.from_dict(error))

        if job_failed_errors:
            try:
                raise YtError(
                    message="Found failed jobs in some computations",
                    inner_errors=job_failed_errors,
                )
            except Exception:
                logger.exception("Found failed jobs in some computations")
                raise

        logger.info("Still waiting (current state: %s, target state: %s)",
                    current_state, target_state)

        time.sleep(1)


def upper_first(str):
    if not str:
        return str

    return str[:1].upper() + str[1:]


def convert_snake_to_camel(snake_str):
    parts = snake_str.split('_')
    return "".join(upper_first(part) for part in parts)


def convert_snake_dict_keys_to_camel(obj):
    import yt.yson as yson

    result = obj
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if isinstance(value, yson.YsonBoolean):
                new_value = True if value else False
            else:
                new_value = convert_snake_dict_keys_to_camel(value)

            result[convert_snake_to_camel(key)] = new_value
    elif isinstance(obj, list):
        result = []
        for value in obj:
            result.append(convert_snake_dict_keys_to_camel(value))
    return result


def convert_gateways_config_to_proto_text(gateways_config):
    message = ParseDict(convert_snake_dict_keys_to_camel(gateways_config),
                        gateways_config_pb2.TGatewaysConfig())
    return MessageToString(message)


def create_flow_logs_replicators(pipeline_path, output_dir, logs_batch_size, output_file_prefix, yt_client):
    if output_file_prefix:
        output_file_prefix += "_"

    return (FlowLogsReplicator(
        pipeline_path + "/" + file,
        os.path.join(output_dir, output_file_prefix + file + ".log"),
        logs_batch_size,
        yt_client
    ) for file in ("controller_logs", "worker_logs"))


class FlowLogsReplicator(threading.Thread):
    def __init__(self, logs_table_path, target_file_path, logs_batch_size, yt_client):
        super(FlowLogsReplicator, self).__init__()
        self.logs_table_path = logs_table_path
        self.target_file_path = target_file_path
        self.yt_client = yt_client
        self.offset = 0
        self.logs_batch_size = logs_batch_size
        self.stop_event = threading.Event()

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_value, tb):
        self.stop()

    def stop(self):
        self.stop_event.set()
        self.join()

    def run(self):
        table_exists = self._ensure_table_exists()
        if not table_exists:
            return

        with open(self.target_file_path, "w") as target_file:
            while not self.stop_event.is_set():
                self._do_iteration(target_file)
                self.stop_event.wait(1)

            self._do_iteration(target_file, final=True)

    def _do_iteration(self, target_file, final=False):
        end = self.offset + self.logs_batch_size - 1
        if final:
            tablet_infos = self.yt_client.get_tablet_infos(
                self.logs_table_path,
                tablet_indexes=[0])
            total_row_count = tablet_infos["tablets"][0]["total_row_count"]
            end = total_row_count - 1

        result = list(self.yt_client.select_rows(
            "data FROM [{}] WHERE [$tablet_index] = 0 AND [$row_index] BETWEEN {} AND {}"
            .format(self.logs_table_path, self.offset, end),
            raw=False))

        self.offset += len(result)

        for value in result:
            target_file.write(value["data"] + "\n")

    def _ensure_table_exists(self):
        while not self.yt_client.exists(self.logs_table_path):
            if self.stop_event.is_set():
                return False
            self.stop_event.wait(1)

        return True


def dump_pipeline_jobs_stderr(pipeline_path, jobs_stderr_file_path, client):
    import yt.yson

    vanilla_info_attribute = "_yql_ytflow_vanilla_info"

    pipeline_attributes = client.get(
        pipeline_path,
        attributes=[vanilla_info_attribute],
    )

    def write_info(message):
        with open(jobs_stderr_file_path, "w") as jobs_stderr_file:
            jobs_stderr_file.write(message)
            jobs_stderr_file.write("\n")

    if pipeline_attributes.attributes is None:
        write_info("No operation found in pipeline attributes\n")
        return

    operation_id = pipeline_attributes.attributes[vanilla_info_attribute]["operation_id"]

    try:
        jobs = client.list_jobs(operation_id, with_stderr=True)
    except Exception:
        write_info(traceback.format_exc())
        return

    with open(jobs_stderr_file_path, "w") as jobs_stderr_file:
        for job in jobs["jobs"]:
            jobs_stderr_file.write(
                "Job: {id} ({state}, {task_name})\n".format(
                    id=job["id"],
                    state=job["state"],
                    task_name=job["task_name"],
                )
            )

            if job["state"] == "failed":
                formatted_error = yt.yson.dumps(job["error"], yson_format="pretty") \
                    .decode("utf-8")

                jobs_stderr_file.write(formatted_error)
                jobs_stderr_file.write("\n")

            try:
                job_stderr = client.get_job_stderr(operation_id, job["id"]) \
                    .read() \
                    .decode("utf8")

                jobs_stderr_file.write(job_stderr)
            except Exception:
                jobs_stderr_file.write(traceback.format_exc())

            jobs_stderr_file.write("\n\n")


def wait_for_debug():
    event = threading.Event()
    event.wait()


class FlowProcess:
    def __init__(self, flow_command, env, stdout, stderr):
        self.flow_command = flow_command
        self.env = env
        self.stdout = stdout
        self.stderr = stderr

    def __enter__(self):
        self.process = subprocess.Popen(
            self.flow_command,
            env=self.env,
            stdout=self.stdout, stderr=self.stderr
        )

    def __exit__(self, exc_type, exc_value, tb):
        import yt.logger as logger

        if self.process.poll() is None:
            logger.info("Terminating flow process with pid: %s" % self.process.pid)
            self.process.terminate()
            self.process.wait()


class FlowDebugHelper:
    def __init__(self, yt_cluster, yt_proxy_endpoint, yt_client, pipeline_path,
                 ytflow_worker_bin_path, port_manager):
        self.yt_cluster = yt_cluster
        self.yt_proxy_endpoint = yt_proxy_endpoint
        self.yt_client = yt_client
        self.pipeline_path = pipeline_path
        self.ytflow_worker_bin = ytflow_worker_bin_path
        self.port_manager = port_manager

    def get_debug_environment_variables(self, flow_mode):
        import yt.yson as yson

        rpc_port = self.port_manager.get_port()
        monitoring_port = self.port_manager.get_port()

        return {
            "YT_FLOW_CONFIG": yson.dumps(yson.YsonMap(
                {
                    "rpc_port": rpc_port,
                    "bus_server": yson.YsonMap(
                        {
                            "port": rpc_port
                        }
                    ),
                    "monitoring_port": monitoring_port
                }
            )).decode("utf-8"),
            "YT_PROXY_URL_ALIASING_CONFIG": yson.dumps(yson.YsonMap(
                {
                    self.yt_cluster: self.yt_proxy_endpoint
                }
            )).decode("utf-8"),
            "YT_FLOW_MODE": flow_mode,
            "YDB_TOKEN": "does_not_matter",
            "MONIUM_TOKEN": "does_not_matter",
        }

    def wait_leader_controller(self, pipeline_path, timeout, attempts, delay, client):
        import yt.logger as logger

        from yt.common import YtResponseError
        from yt.wrapper.flow_commands import get_pipeline_state

        attempt = 0
        last_exception = None
        while attempt < attempts:
            try:
                return get_pipeline_state(pipeline_path, timeout=timeout, client=client)
            except YtResponseError as error:
                last_exception = error
                logger.info("Retrying exception '%s', delay: %s" % (error, delay))
            time.sleep(delay)
            attempt += 1

        raise last_exception

    def setup_flow_debug_environment(
        self, ytflow_worker_config_path,
        debug_output_directory,
        controller_wait_retries,
        controller_retry_delay,
        flow_command_timeout=600
    ):
        from yt.environment.helpers import read_config
        from yt.wrapper.flow_commands import set_pipeline_spec, set_pipeline_dynamic_spec, start_pipeline

        ytflow_worker_config_destination_path = os.path.join(
            debug_output_directory, "ytflow_worker_config.yson")
        shutil.copy(ytflow_worker_config_path, ytflow_worker_config_destination_path)

        flow_command = [self.ytflow_worker_bin, "--config", ytflow_worker_config_destination_path]
        controller_env, worker_env = [
            self.get_debug_environment_variables(flow_mode)
            for flow_mode in ("Controller", "Worker")
        ]

        stdout_path, stderr_path = [
            os.path.join(debug_output_directory, "controller_" + path_prefix)
            for path_prefix in ("stdout", "stderr")
        ]

        with open(stdout_path, "w") as stdout, open(stderr_path, "w") as stderr, \
             FlowProcess(flow_command, env=controller_env, stdout=stdout, stderr=stderr):

            ytflow_worker_config = read_config(ytflow_worker_config_destination_path)

            self.wait_leader_controller(self.pipeline_path, flow_command_timeout, controller_wait_retries, controller_retry_delay, self.yt_client)
            set_pipeline_spec(self.pipeline_path, ytflow_worker_config['pipeline_spec'], client=self.yt_client)
            set_pipeline_dynamic_spec(self.pipeline_path,
                                      ytflow_worker_config['dynamic_pipeline_spec'], client=self.yt_client)
            start_pipeline(self.pipeline_path, timeout=flow_command_timeout, client=self.yt_client)

            with open(os.path.join(debug_output_directory, "run_worker"), "w") as f:
                worker_env_string = " ".join(["%s='%s'" % (key, value) for key, value in worker_env.items()])
                f.write(worker_env_string + " ya gdb --args " + " ".join(flow_command))

            wait_for_debug()
