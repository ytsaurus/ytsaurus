import six
import time

from datetime import datetime, timedelta


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
