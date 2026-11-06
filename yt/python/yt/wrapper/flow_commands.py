from .common import set_param
from .driver import make_request, make_formatted_request, get_structured_format
from .dynamic_table_commands import get_tablet_infos, select_rows
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

    return flow_execute(
        pipeline_path,
        flow_command="set-target-pipeline-state",
        flow_argument={"target_pipeline_state": "completed"},
        client=client)


def stop_pipeline(pipeline_path, client=None):
    """Stop YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    return flow_execute(
        pipeline_path,
        flow_command="set-target-pipeline-state",
        flow_argument={"target_pipeline_state": "stopped"},
        client=client)


def pause_pipeline(pipeline_path, client=None):
    """Pause YT Flow pipeline.

    :param pipeline_path: path to pipeline.
    """

    return flow_execute(
        pipeline_path,
        flow_command="set-target-pipeline-state",
        flow_argument={"target_pipeline_state": "paused"},
        client=client)


def get_pipeline_spec(pipeline_path, spec_path=None, format=None, client=None):
    """Get YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline spec yson struct, starting with /.
    """

    argument = {}
    set_param(argument, "path", spec_path)

    return flow_execute(
        pipeline_path,
        flow_command="get-pipeline-spec",
        flow_argument=argument,
        output_format=format,
        client=client)


def set_pipeline_spec(pipeline_path, value, spec_path=None, expected_version=None, force=None, format=None, client=None):
    """Set YT Flow pipeline spec.

    :param pipeline_path: path to pipeline.
    :param value: new pipeline spec.
    :param spec_path: path inside pipeline spec yson struct, starting with /.
    :param expected_version: current spec expected version.
    :param force: if true, update spec even if pipeline is paused.
    """

    if format is not None:
        # value is pre-serialized bytes; parse it first so it can be wrapped into the argument map.
        value = yson.loads(value)

    params = {"spec": value}
    set_param(params, "path", spec_path)
    set_param(params, "expected_version", expected_version)
    set_param(params, "force", force)

    return flow_execute(
        pipeline_path,
        flow_command="set-pipeline-spec",
        flow_argument=params,
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


def get_pipeline_dynamic_spec(pipeline_path, spec_path=None, format=None, view="effective", blame=False, client=None):
    """Get YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    :param view: effective or base.
    :param blame: include patch provenance for changed fields.
    """

    params = {}
    set_param(params, "path", spec_path)
    if view != "effective":
        params["view"] = view
    if blame:
        params["blame"] = True

    return flow_execute(
        pipeline_path,
        flow_command="get-pipeline-dynamic-spec",
        flow_argument=params,
        output_format=format,
        client=client)


def set_pipeline_dynamic_spec(pipeline_path, value, spec_path=None, expected_version=None, format=None, client=None):
    """Set YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param value: new pipeline dynamic spec.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    :param expected_version: current dynamic spec expected version.
    """

    if format is not None:
        # value is pre-serialized bytes; parse it first so it can be wrapped into the argument map.
        value = yson.loads(value)

    params = {"spec": value}
    set_param(params, "path", spec_path)
    set_param(params, "expected_version", expected_version)

    return flow_execute(
        pipeline_path,
        flow_command="set-pipeline-dynamic-spec",
        flow_argument=params,
        client=client)


def remove_pipeline_dynamic_spec(pipeline_path, spec_path=None, expected_version=None, client=None):
    """Remove YT Flow pipeline dynamic spec.

    :param pipeline_path: path to pipeline.
    :param spec_path: path inside pipeline dynamic spec yson struct, starting with /.
    :param expected_version: current dynamic spec expected version.
    """

    params = {}
    set_param(params, "path", spec_path)
    set_param(params, "expected_version", expected_version)

    return flow_execute(
        pipeline_path,
        flow_command="remove-pipeline-dynamic-spec",
        flow_argument=params,
        client=client)


def get_pipeline_dynamic_spec_patches(pipeline_path, patch=None, format=None, client=None):
    """Get active DynamicSpec patches and the current state version.

    :param pipeline_path: path to pipeline.
    :param patch: optional ID of a single active patch.
    :param format: optional serialized output format.
    """
    argument = {}
    set_param(argument, "patch", patch)
    return flow_execute(
        pipeline_path,
        flow_command="get-pipeline-dynamic-spec-patches",
        flow_argument=argument,
        output_format=format,
        client=client,
    )


def patch_pipeline_dynamic_spec(
    pipeline_path, set=None, patch=None, comment=None, expected_version=None, format=None, set_changes=None, client=None
):
    """Create or update a durable DynamicSpec patch.

    :param pipeline_path: path to pipeline.
    :param set: pair of a DynamicSpec path and its new value.
    :param set_changes: sequence of path/value pairs to set in one patch.
    :param patch: optional ID of an existing patch to update.
    :param comment: reason for the change.
    :param expected_version: required current state version.
    :param format: input format for a serialized set value.
    """
    if expected_version is None:
        raise YtError("expected_version is required for a DynamicSpec patch")
    if set is not None and set_changes is not None:
        raise YtError("Specify either set or set_changes")
    if set_changes is not None and not set_changes:
        raise YtError("set_changes must not be empty")
    if patch is None and set is None and set_changes is None:
        raise YtError("A new patch requires set or set_changes")
    if (patch is None or set is not None or set_changes is not None or comment is not None) and not comment:
        raise YtError("A nonempty comment must be specified for a DynamicSpec patch")

    argument = {"expected_version": expected_version}
    set_param(argument, "patch", patch)
    set_param(argument, "comment", comment)
    if set is not None:
        path, value = set
        if format is not None:
            value = get_structured_format(format, client=client).loads_node(value)
        argument["set_path"] = path
        argument["set_value"] = value
    if set_changes is not None:
        structured_format = get_structured_format(format, client=client) if format is not None else None
        argument["set_changes"] = [
            {"path": path, "value": structured_format.loads_node(value) if structured_format is not None else value}
            for path, value in set_changes
        ]
    return flow_execute(
        pipeline_path,
        flow_command="patch-pipeline-dynamic-spec",
        flow_argument=argument,
        client=client,
    )


def cancel_pipeline_dynamic_spec_patch(pipeline_path, patch, expected_version, client=None):
    """Cancel a DynamicSpec patch.

    :param pipeline_path: path to pipeline.
    :param patch: ID of the patch to cancel.
    :param expected_version: required current dynamic spec state version.
    """
    argument = {"patch": patch, "expected_version": expected_version}
    return flow_execute(
        pipeline_path, flow_command="cancel-pipeline-dynamic-spec-patch", flow_argument=argument, client=client
    )


def reset_pipeline_dynamic_spec(pipeline_path, expected_version, client=None):
    """Cancel all DynamicSpec patches and inherit the release base.

    :param pipeline_path: path to pipeline.
    :param expected_version: current dynamic spec expected version.
    """
    argument = {"expected_version": expected_version}
    return flow_execute(
        pipeline_path, flow_command="reset-pipeline-dynamic-spec", flow_argument=argument, client=client
    )


def get_pipeline_state(pipeline_path, timeout=None, client=None):
    """Get YT Flow pipeline state

    :param pipeline_path: path to pipeline
    """

    result = flow_execute(
        pipeline_path,
        flow_command="get-pipeline-state",
        flow_argument={},
        client=client)
    return result["pipeline_state"].lower()


class PipelineState(str, enum.Enum):
    Unknown = "unknown"
    Stopped = "stopped"
    Paused = "paused"
    Working = "working"
    Draining = "draining"
    Pausing = "pausing"
    Completed = "completed"


def wait_pipeline_state(target_state, pipeline_path, wait_timeout, request_timeout=600, client=None):
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

    deadline = datetime.now() + timedelta(seconds=wait_timeout)

    while True:
        if datetime.now() > deadline:
            raise YtError("Wait time out", attributes={"wait_timeout": wait_timeout})

        current_state = get_pipeline_state(
            pipeline_path=pipeline_path,
            timeout=request_timeout,
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

    params = {}
    set_param(params, "path", view_path)
    set_param(params, "cache", cache)

    return flow_execute(
        pipeline_path,
        flow_command="get-flow-view",
        flow_argument=params,
        output_format=format,
        client=client)


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
        allow_retries=True,
        client=client)


def read_states(pipeline_path: str, computation_id=None, partition_id=None, key=None, name=None, target=None, limit=None, output_format=None, client=None):
    """Read state rows matching the supplied filters.

    Three access modes are derived from the argument combination:
      1. computation_id only          → all tables for every partition of the computation
      2. computation_id + key         → key_states only (target=partition_state is rejected)
      3. partition_id                 → partition_states + key_states under the partition's
                                        SourceKey (when the partition has one)

    Returns a map with two arrays: "key_states" (list of {computation_id, key, entries}) and
    "partition_states" (list of {partition_id, entries}); each "entries" is a {name: state} map.

    :param pipeline_path: path to pipeline.
    :param computation_id: filter by computation id.
    :param partition_id: filter by partition id; mutually exclusive with computation_id.
    :param key: TKey value as either a positional list ``[v0, v1, ...]`` or a named map ``{"col": value, ...}``; expression columns are computed via the column evaluator. Requires computation_id.
    :param name: optional state name filter.
    :param target: which table(s) to read: ``"all"`` (default), ``"key_state"``, or ``"partition_state"``.
    :param limit: upper bound on rows scanned per table; defaults to 10 server-side.
    """
    argument = {}
    set_param(argument, "computation_id", computation_id)
    set_param(argument, "partition_id", partition_id)
    set_param(argument, "key", key)
    set_param(argument, "name", name)
    set_param(argument, "target", target)
    set_param(argument, "limit", limit)
    return flow_execute(
        pipeline_path,
        flow_command="read-states",
        flow_argument=argument,
        output_format=output_format,
        client=client)


def delete_states(pipeline_path: str, computation_id=None, partition_id=None, key=None, name=None, target=None, force=False, commit=False, output_format=None, client=None):
    """Delete state rows matching the supplied filters; pipeline must be stopped/completed.

    Modes match ``read_states``: computation_id only / computation_id + key / partition_id.

    Dry-run by default: without ``commit`` the call counts matching rows and returns a compact
    summary without touching the tables. Pass ``commit=True`` once the preview matches the intent.
    The deletion runs in chunks (List + Erase loop) to avoid blowing through the per-transaction
    row limit and to keep individual erases responsive.

    :param pipeline_path: path to pipeline.
    :param computation_id: filter by computation id; mutually exclusive with partition_id.
    :param partition_id: filter by partition id; mutually exclusive with computation_id.
    :param key: TKey value (list or map form, see ``read_states``). Requires computation_id.
    :param name: optional state name filter.
    :param target: which table(s) to delete from: ``"all"`` (default), ``"key_state"``, or ``"partition_state"``.
    :param force: delete states even if the pipeline is only paused, not stopped.
    :param commit: actually delete; without it the call is a preview.
    """
    argument = {}
    set_param(argument, "computation_id", computation_id)
    set_param(argument, "partition_id", partition_id)
    set_param(argument, "key", key)
    set_param(argument, "name", name)
    set_param(argument, "target", target)
    if force:
        argument["force"] = True
    if commit:
        argument["commit"] = True
    return flow_execute(
        pipeline_path,
        flow_command="delete-states",
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
