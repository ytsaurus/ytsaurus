from .common import set_param
from .driver import make_request, make_formatted_request, get_structured_format
from .dynamic_table_commands import get_tablet_infos, select_rows
from .format import YsonFormat
from .ypath import YPath

from yt.common import YtError

from datetime import datetime, timedelta

import yt.logger as logger
import yt.yson as yson

import enum
import time


def start_pipeline(pipeline_path, timeout=None, client=None):
    """Start YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request(
        "start_pipeline",
        params=params,
        client=client,
        timeout=timeout)


def stop_pipeline(pipeline_path, client=None):
    """Stop YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request("stop_pipeline", params, client=client)


def pause_pipeline(pipeline_path, client=None):
    """Pause YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}

    return make_request("pause_pipeline", params, client=client)


def get_pipeline_spec(pipeline_path, spec_path=None, format=None, client=None):
    """Get YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline spec yson struct, starting with /.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)

    result = make_formatted_request(
        "get_pipeline_spec",
        params=params,
        format=format,
        client=client)
    return result


def set_pipeline_spec(pipeline_path, value, spec_path=None, expected_version=None, force=None, format=None, client=None):
    """Set YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec: new pipeline spec.
    :param spec_path: path inside pipeline spec yson struct, starting with /.
    :param expected_version: current spec expected version.
    :param force: if true, update spec even if pipeline is paused.
    """

    is_format_specified = format is not None
    format = get_structured_format(format, client=client)
    if not is_format_specified:
        value = format.dumps_node(value)

    params = {
        "pipeline_path": YPath(pipeline_path, client=client),
        "input_format": format.to_yson_type(),
    }
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)
    set_param(params, "force", force)

    return make_request(
        "set_pipeline_spec",
        params,
        data=value,
        client=client)


def remove_pipeline_spec(pipeline_path, spec_path=None, expected_version=None, force=None, client=None):
    """Remove YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline spec yson struct, starting with /.
    :param expected_version: current spec expected version.
    :param force: if true, remove spec even if pipeline is paused.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)
    set_param(params, "force", force)

    return make_request(
        "remove_pipeline_spec",
        params,
        client=client)


def get_pipeline_dynamic_spec(pipeline_path, spec_path=None, format=None, client=None):
    """Get YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)

    result = make_formatted_request(
        "get_pipeline_dynamic_spec",
        params=params,
        format=format,
        client=client)
    return result


def set_pipeline_dynamic_spec(pipeline_path, value, spec_path=None, expected_version=None, format=None, client=None):
    """Set YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec: new pipeline spec.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    :param expected_version: current dynamic spec expected version.
    """

    is_format_specified = format is not None
    format = get_structured_format(format, client=client)
    if not is_format_specified:
        value = format.dumps_node(value)

    params = {
        "pipeline_path": YPath(pipeline_path, client=client),
        "input_format": format.to_yson_type(),
    }
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)

    return make_request(
        "set_pipeline_dynamic_spec",
        params,
        data=value,
        client=client)


def remove_pipeline_dynamic_spec(pipeline_path, spec_path=None, expected_version=None, client=None):
    """Remove YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    :param expected_version: current dynamic spec expected version.
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "spec_path", spec_path)
    set_param(params, "expected_version", expected_version)

    return make_request(
        "remove_pipeline_dynamic_spec",
        params,
        client=client)


def get_pipeline_state(pipeline_path, timeout=None, client=None):
    """Get YT Flow pipeline state

    :param pipeline_path: path to pipeline
    """

    params = {
        "pipeline_path": YPath(pipeline_path, client=client),
        "timeout": timeout
    }

    result = make_formatted_request(
        "get_pipeline_state",
        params=params,
        format=YsonFormat(),
        timeout=timeout,
        client=client)
    return result.decode("utf-8").lower()


class PipelineState(str, enum.Enum):
    Unknown = "unknown"
    Stopped = "stopped"
    Paused = "paused"
    Working = "working"
    Draining = "draining"
    Pausing = "pausing"
    Completed = "completed"


def wait_pipeline_state(target_state, pipeline_path, timeout=600, client=None):
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
            raise YtError("Wait time out", attributes={"timeout": timeout})

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

        logger.info("Still waiting (current state: %s, target state: %s)",
                    current_state, target_state)

        time.sleep(1)


def get_flow_view(pipeline_path, view_path=None, cache=None, format=None, client=None):
    """Get YT Flow flow view

    :param pipeline_path: path to pipeline
    :param view_path: path inside flow view yson struct, starting with /
    :param cache: use controller cache
    """

    params = {"pipeline_path": YPath(pipeline_path, client=client)}
    set_param(params, "view_path", view_path)
    set_param(params, "cache", cache)

    result = make_formatted_request(
        "get_flow_view",
        params=params,
        format=format,
        client=client)
    return result


def flow_execute(pipeline_path: str, flow_command: str, flow_argument=None, input_format=None, output_format=None, client=None):
    """Execute YT Flow specific command

    :param pipeline_path: path to pipeline.
    :param flow_command: name of the command to execute.
    :param flow_argument: optional argument of the command.
    """

    if input_format is None:
        input_format = "yson"
        flow_argument = yson.dumps(flow_argument)

    if flow_argument is not None and not isinstance(flow_argument, (str, bytes, bytearray)):
        raise TypeError("Serialized flow_argument must be str, bytes or bytearray, got {}".format(type(flow_argument).__name__))

    params = {
        "pipeline_path": YPath(pipeline_path, client=client),
        "flow_command": flow_command,
        # Validate `input_format` by get_structured_format call.
        "input_format": get_structured_format(input_format, client=client).to_yson_type(),
    }

    return make_formatted_request(
        "flow_execute",
        params,
        data=flow_argument,
        format=output_format,
        use_heavy_proxy=True,
        client=client)


def read_states(pipeline_path: str, computation_id=None, partition_id=None, key=None, name=None, output_format=None, client=None):
    """Read every state row matching the supplied filters.

    Returns a map with two arrays: "key_states" (list of {computation_id, key, entries}) and
    "partition_states" (list of {partition_id, entries}); each "entries" is a {name: state} map.

    At least one of ``computation_id`` or ``partition_id`` must be supplied.

    :param pipeline_path: path to pipeline.
    :param computation_id: filter by computation id (required if `key` is set).
    :param partition_id: filter by partition id; also pulls key_states for the partition's SourceKey, if any.
    :param key: TKey value as either a positional list ``[v0, v1, ...]`` or a named map ``{"col": value, ...}``; expression columns are computed via the column evaluator.
    :param name: optional state name filter, narrows both key_states and partition_states.
    """
    argument = {}
    set_param(argument, "computation_id", computation_id)
    set_param(argument, "partition_id", partition_id)
    set_param(argument, "key", key)
    set_param(argument, "name", name)
    return flow_execute(
        pipeline_path,
        flow_command="read-states",
        flow_argument=argument,
        output_format=output_format,
        client=client)


def read_state(pipeline_path: str, name: str, computation_id=None, partition_id=None, key=None, use_source_key=False, output_format=None, client=None):
    """Read one specific state row and return its raw YSON value.

    The address must be unique: exactly one of ``key``, ``partition_id``, or
    ``partition_id + use_source_key`` is required. Errors out if no row matches.
    ``computation_id`` is required only when reading by ``key``; for partition-based reads it
    is derived from the layout.

    :param pipeline_path: path to pipeline.
    :param name: state name.
    :param computation_id: computation id; required when ``key`` is set.
    :param partition_id: partition id; without ``use_source_key`` reads from partition_states.
    :param key: TKey value (list or map form, see ``read_states``).
    :param use_source_key: when set together with ``partition_id``, reads key_states using the partition's SourceKey instead of partition_states.
    """
    argument = {"name": name}
    set_param(argument, "computation_id", computation_id)
    set_param(argument, "partition_id", partition_id)
    set_param(argument, "key", key)
    if use_source_key:
        argument["use_source_key"] = True
    return flow_execute(
        pipeline_path,
        flow_command="read-state",
        flow_argument=argument,
        output_format=output_format,
        client=client)


def get_controller_logs(pipeline_path, count, offset=None, client=None):
    """Get YT Flow controller logs

    :param pipeline_path: path to pipeline
    :param count: the number of last logs
    :param offset: id of start log row
    """

    assert count > 0, "'count' must be positive"

    if offset is None:
        tablet_infos = get_tablet_infos(f"{pipeline_path}/controller_logs", tablet_indexes=[0], client=client)
        total_row_count = tablet_infos["tablets"][0]["total_row_count"]
        offset = max(total_row_count - count, 0)

    end = offset + count - 1
    result = list(select_rows(
        f"host, data FROM [{pipeline_path}/controller_logs] WHERE [$tablet_index] = 0 AND [$row_index] BETWEEN {offset} AND {end}",
        raw=False,
        client=client))

    return result, offset + len(result)
