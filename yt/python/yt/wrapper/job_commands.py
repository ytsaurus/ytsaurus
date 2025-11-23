from .config import get_config
from .driver import make_request, make_formatted_request
from .common import set_param
from .ypath import YPath

from typing import Optional, TypedDict, List, Dict, Any


class ListJobJobType(TypedDict, total=False):
    id: str
    type: str
    state: str
    controller_state: str
    archive_state: str
    address: str
    addresses: Dict[str, str]
    start_time: str
    finish_time: str
    has_spec: bool
    job_competition_id: str
    probing_job_competition_id: str
    has_competitors: bool
    brief_statistics: Dict[str, Any]
    core_infos: List[Any]
    exec_attributes: Dict[str, Any]
    task_name: str
    pool_tree: str
    pool: str
    is_stale: bool
    job_cookie: int
    allocation_id: str


class GetJobJobType(ListJobJobType):
    operation_id: str

    class JobEventsType(TypedDict, total=False):
        time: str
        state: Optional[str]
        phase: str

    events: List[JobEventsType]

    class JobStatisticsType(TypedDict, total=False):
        chunk_reader_statistics: Dict[str, Any]
        chunk_writer_statistics: Dict[str, Any]
        codec: Dict[str, Any]
        data: Dict[str, Any]
        exec_agent: Dict[str, Any]
        job_proxy: Dict[str, Any]
        latency: Dict[str, Any]
        time: Dict[str, Any]
        user_job: Dict[str, Any]

    statistics: JobStatisticsType


class ListJobsType(TypedDict, total=False):
    jobs: List[ListJobJobType]
    cypress_job_count: Optional[int]
    scheduler_job_count: Optional[int]
    controller_agent_job_count: Optional[int]
    archive_job_count: Optional[int]
    type_counts: Dict[str, Any]
    state_counts: Dict[str, Any]
    errors: List[Any]
    continuation_token: Any


class ListJobTracesType(TypedDict, total=False):
    class JobTraceType(TypedDict, total=False):
        trace_id: str
        progress: str
        health: str
        process_trace_metas: Dict[str, str]

    traces: List[JobTraceType]


class JobSpecType(TypedDict, total=False):
    type: int
    version: int
    resource_limits: Dict[str, Any]
    job_spec_ext: Dict[str, Any]
    reduce_job_spec_ext: Dict[str, Any]


def list_jobs(
    operation_id: str,
    job_type=None,
    job_state=None,
    address=None,
    job_competition_id=None,
    with_competitors=None,
    sort_field=None,
    sort_order=None,
    limit=None,
    offset=None,
    with_stderr=None,
    with_spec=None,
    with_fail_context=None,
    with_monitoring_descriptor=None,
    with_interruption_info=None,
    include_cypress=None,
    include_runtime=None,
    include_archive=None,
    data_source=None,
    attributes=None,
    operation_incarnation=None,
    monitoring_descriptor=None,
    format=None,
    client=None,
) -> ListJobsType:
    """List jobs of operation."""
    params = {"operation_id": operation_id}
    set_param(params, "job_type", job_type)
    set_param(params, "job_state", job_state)
    set_param(params, "address", address)
    set_param(params, "job_competition_id", job_competition_id)
    set_param(params, "sort_field", sort_field)
    set_param(params, "sort_order", sort_order)
    set_param(params, "limit", limit)
    set_param(params, "offset", offset)
    set_param(params, "with_stderr", with_stderr)
    set_param(params, "with_spec", with_spec)
    set_param(params, "with_fail_context", with_fail_context)
    set_param(params, "with_competitors", with_competitors)
    set_param(params, "with_monitoring_descriptor", with_monitoring_descriptor)
    set_param(params, "with_interruption_info", with_interruption_info)
    set_param(params, "include_cypress", include_cypress)
    set_param(params, "include_runtime", include_runtime)
    set_param(params, "include_archive", include_archive)
    set_param(params, "attributes", attributes)
    set_param(params, "data_source", data_source)
    set_param(params, "operation_incarnation", operation_incarnation)
    set_param(params, "monitoring_descriptor", monitoring_descriptor)

    timeout = get_config(client)["operation_info_commands_timeout"]

    return make_formatted_request(
        "list_jobs",
        params=params,
        format=format,
        client=client,
        timeout=timeout)


def get_job(operation_id: str, job_id: str, format=None, client=None) -> GetJobJobType:
    """Get job of operation.

    :param str operation_id: operation id.
    :param str job_id: job id.
    """
    params = {"operation_id": operation_id, "job_id": job_id}
    timeout = get_config(client)["operation_info_commands_timeout"]
    return make_formatted_request(
        "get_job",
        params=params,
        format=format,
        client=client,
        timeout=timeout)


def run_job_shell(job_id: str, shell_name: Optional[str] = None, timeout=None, command=None, client=None):
    """Runs interactive shell in the job sandbox.

    :param str job_id: job id.
    :param str shell_name: shell name.
    """
    from .job_shell import JobShell

    JobShell(job_id, shell_name=shell_name, interactive=True, timeout=timeout, client=client).run(command=command)


def get_job_stderr(operation_id: str, job_id: str, stderr_type: Optional[str] = None, client=None) -> bytes:
    """Gets stderr of the specified job.

    :param str operation_id: operation id.
    :param str job_id: job id.
    :param str type: stderr type.
    """
    params = {"operation_id": operation_id, "job_id": job_id}
    set_param(params, "type", stderr_type)
    return make_request(
        "get_job_stderr",
        params,
        return_content=False,
        client=client)


def list_job_traces(
    operation_id: str,
    job_id: str,
    per_process : Optional[bool] = None,
    limit : Optional[int] = None,
    format=None,
    client=None
) -> ListJobTracesType:
    """List traces of the specified job."""
    params = {"operation_id": operation_id, "job_id": job_id}
    set_param(params, "per_process", per_process)
    set_param(params, "limit", limit)
    timeout = get_config(client)["operation_info_commands_timeout"]

    return make_formatted_request(
        "list_job_traces",
        params=params,
        format=format,
        client=client,
        timeout=timeout)


def get_job_trace(operation_id: str, job_id: str,
                  trace_id: Optional[str] = None, from_time=None, to_time=None, client=None):
    """Get traces of the specified job."""
    params = {"operation_id": operation_id, "job_id": job_id}
    set_param(params, "trace_id", trace_id)
    set_param(params, "from_time", from_time)
    set_param(params, "to_time", to_time)

    return make_request(
        "get_job_trace",
        params,
        return_content=False,
        use_heavy_proxy=True,
        client=client)


def get_job_input(job_id: str, client=None):
    """Get full input of the specified job.

    :param str job_id: job id.
    """
    return make_request(
        "get_job_input",
        params={"job_id": job_id},
        return_content=False,
        use_heavy_proxy=True,
        client=client)


def get_job_input_paths(job_id: str, client=None):
    """Get input paths of the specified job.

    :param str job_if: job id.
    :return: list of YPaths.
    """
    timeout = get_config(client)["operation_info_commands_timeout"]
    yson_paths = make_formatted_request(
        "get_job_input_paths",
        params={"job_id": job_id},
        format=None,
        timeout=timeout,
        client=client)
    return list(map(YPath, yson_paths))


def abort_job(job_id: str, interrupt_timeout: Optional[int] = None, client=None):
    """Interrupts running job with preserved result.

    :param str job_id: job id.
    :param int interrupt_timeout: wait for interrupt before abort (in ms).
    """
    params = {"job_id": job_id}
    set_param(params, "interrupt_timeout", interrupt_timeout)
    return make_request("abort_job", params, client=client)


def dump_job_proxy_log(job_id: str, operation_id: str, path: str, client=None):
    """Dumps JobProxy logs for a specified job to cypress.

    :param str job_id: job id.
    :param str operation_id: operation id.
    :param str path: ypath to dump logs to.
    """
    params = {
        "job_id": job_id,
        "operation_id": operation_id,
        "path": path
    }
    return make_request("dump_job_proxy_log", params, client=client)


def dump_job_context(job_id: str, path: str, client=None):
    """Dumps job input context to specified path."""
    return make_request("dump_job_context", {"job_id": job_id, "path": path}, client=client)


def get_job_fail_context(operation_id: str, job_id: str, client=None):
    """Get fail context of the specified job.

    :param str operation_id: operation id.
    :param str job_id: job id.
    """
    return make_request(
        "get_job_fail_context",
        params={"operation_id": operation_id, "job_id": job_id},
        return_content=False,
        use_heavy_proxy=True,
        client=client)


def get_job_spec(job_id: str,
                 omit_node_directory: Optional[bool] = None,
                 omit_input_table_specs: Optional[bool] = None,
                 omit_output_table_specs: Optional[bool] = None,
                 client=None) -> JobSpecType:
    """Get spec of the specified job.

    :param str job_id: job id.
    :param bool omit_node_directory: whether node directory should be removed from job spec.
    :param bool omit_input_table_specs: whether input table specs should be removed from job spec.
    :param bool omit_output_table_specs: whether output table specs should be removed from job spec.
    """
    params = {"job_id": job_id}
    set_param(params, "omit_node_directory", omit_node_directory)
    set_param(params, "omit_input_table_specs", omit_input_table_specs)
    set_param(params, "omit_output_table_specs", omit_output_table_specs)
    return make_formatted_request("get_job_spec", params, format=None, client=client)
