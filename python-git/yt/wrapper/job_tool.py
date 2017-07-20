from __future__ import print_function

from yt.common import makedirp
from yt.wrapper.common import parse_bool, DoNotReplaceAction, chunk_iter_stream, MB
from yt.wrapper.job_runner import make_run_script, get_output_descriptor_list
import yt.logger as logger
import yt.yson as yson
import yt.wrapper as yt

import collections
import json
import os
import stat
import subprocess
import sys

DESCRIPTION = """
Tool helps to debug user job code by preparing job environment on local machine.

It downloads all necessary job files, fail context (small portion of job input data)
and prepares run script.
"""

EPILOG = """
Examples:
$ yt-job-tool prepare-job-environment 84702787-e6196e33-3fe03e8-f0f415f8 3c9a3b91-5689208c-3fe0384-e32dff58
...
<job_path>
$ yt-job-tool run-job <job_path>
"""

JOB_PATH_PATTERN = "//sys/operations/{0}/jobs/{1}"

JOB_TYPE_TO_SPEC_TYPE = {
    "partition_reduce": "reducer",
    "partition_map": "mapper",
    "ordered_map": "mapper",
    "reduce_combiner": "reducer",
    "join_reduce": "reducer",
    "sorted_reduce": "reducer",
    "map": "mapper"
}

ORCHID_JOB_PATH_PATTERN = "//sys/scheduler/orchid/scheduler/operations/{0}/running_jobs/{1}"
NODE_ORCHID_JOB_PATH_PATTERN = "//sys/nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}"

OPERATION_ARCHIVE_JOBS_PATH = "//sys/operations_archive/jobs"

JobInfo = collections.namedtuple("JobInfo", ["job_type", "is_running"])

def shellquote(s):
    # https://stackoverflow.com/questions/35817/how-to-escape-os-system-calls-in-python
    return "'" + s.replace("'", "'\\''") + "'"

def make_environment_string(environment):
    return ''.join("export {var}={value}\n".format(var=var, value=shellquote(environment[var]))
                   for var in environment)

def make_run_sh(run_sh_path, operation_id, job_id, sandbox_path, command, environment,
                input, output_path, output_table_count, use_yamr_descriptors):
    output_descriptor_list = get_output_descriptor_list(output_table_count, use_yamr_descriptors)

    # We don't want to redirect stderr.
    output_descriptor_list.remove(2)

    output_descriptors_spec = " ".join(
        "{d}> {output_path}/{d}".format(d=d, output_path=output_path)
        for d in output_descriptor_list)

    script = """\
#!/usr/bin/env bash

cd {sandbox_path}
mkdir -p {output_path}

export YT_JOB_INDEX=0
export YT_START_ROW_INDEX=0
export YT_OPERATION_ID={operation_id}
export YT_JOB_ID={job_id}
export YT_STARTED_BY_JOB_TOOL=1
{environment}

INPUT_DATA="{input}"

({command}) < $INPUT_DATA {output_descriptors_spec}
""".format(
    sandbox_path=sandbox_path,
    operation_id=operation_id,
    job_id=job_id,
    command=command,
    environment=make_environment_string(environment),
    input=input,
    output_path=output_path,
    output_descriptors_spec=output_descriptors_spec)

    with open(run_sh_path, "w") as out:
        out.write(script)
    os.chmod(run_sh_path, 0o744)

def add_hybrid_argument(parser, name, group_required=True, **kwargs):
    group = parser.add_mutually_exclusive_group(required=group_required)
    group.add_argument(name, nargs="?", action=DoNotReplaceAction, **kwargs)
    group.add_argument("--" + name.replace("_", "-"), **kwargs)

def download_file(path, destination_path):
    with open(destination_path, "wb") as f:
        for chunk in chunk_iter_stream(yt.read_file(path), 16 * MB):
            f.write(chunk)

def run_job(job_path):
    if not os.path.exists(job_path):
        raise yt.YtError("Job path {0} does not exist".format(job_path))

    run_script = os.path.join(job_path, "run")
    p = subprocess.Popen([run_script], close_fds=False)
    sys.exit(p.wait())

def download_job_input(operation_id, job_id, job_input_path, mode):
    if mode == "running":
        logger.info("Job is running, using its input context as local input")
        output_path = yt.find_free_subpath(yt.config["remote_temp_files_directory"])
        yt.dump_job_context(job_id, output_path)
        try:
            download_file(output_path, job_input_path)
        finally:
            yt.remove(output_path, force=True)
    elif mode == "failed":
        logger.info("Job is failed, using its fail context as local input")

        cypress_job_path = JOB_PATH_PATTERN.format(operation_id, job_id)
        fail_context_path = yt.ypath_join(cypress_job_path, "fail_context")
        if not yt.exists(fail_context_path):
            raise yt.YtError("Job input data is missing. Neither input context nor fail context exists")

        download_file(fail_context_path, job_input_path)
    elif mode == "full":
        logger.info("Downloading full job input")

        job_input_f = yt.driver.make_request(
            "get_job_input",
            {"operation_id": operation_id, "job_id": job_id},
            return_content=False,
            use_heavy_proxy=True)
        with open(job_input_path, "wb") as out:
            for chunk in chunk_iter_stream(job_input_f, 16 * MB):
                out.write(chunk)

    else:
        raise ValueError("Unknown mode: {0}".format(mode))

    logger.info("Job input is downloaded to %s", job_input_path)

def get_job_info_from_cypress(operation_id, job_id):
    job_is_running = False

    running_job_info = None
    try:
        running_job_info = yt.get(ORCHID_JOB_PATH_PATTERN.format(operation_id, job_id))
    except yt.YtResponseError as err:
        if not err.is_resolve_error():
            raise

    if running_job_info is not None:
        job_info_on_node = None
        try:
            job_info_on_node = yt.get(NODE_ORCHID_JOB_PATH_PATTERN.format(running_job_info["address"], job_id))
        except yt.YtResponseError as err:
            if not err.is_resolve_error():
                raise

        if job_info_on_node is not None:
            phase = job_info_on_node.get("job_phase")
            if phase is not None and phase == "running":
                job_is_running = True

    if job_is_running:
        return JobInfo(running_job_info["job_type"], is_running=True)

    cypress_job_path = JOB_PATH_PATTERN.format(operation_id, job_id)
    if not yt.exists(cypress_job_path):
        raise yt.YtError("Cannot find running or failed job with id {0} (operation id: {1})".format(job_id, operation_id))

    return JobInfo(yt.get_attribute(cypress_job_path, "job_type"), is_running=False)

def get_job_type_from_dyntable(operation_id, job_id):
    job_hash_pair = yt.common.uuid_hash_pair(job_id)
    operation_hash_pair = yt.common.uuid_hash_pair(operation_id)
    key = {
        "operation_id_hi": operation_hash_pair.hi,
        "operation_id_lo": operation_hash_pair.lo,
        "job_id_hi": job_hash_pair.hi,
        "job_id_lo": job_hash_pair.lo
    }
    rows = list(yt.lookup_rows(OPERATION_ARCHIVE_JOBS_PATH, [key], keep_missing_rows=True, column_names=["type"]))
    assert len(rows) == 1
    if rows[0] is None:
        raise yt.YtError("Cannot find job in job archive table (operation-id: {0}; job-id {1})".format(operation_id, job_id))
    return rows[0]["type"]

def get_operation_attributes_from_dyntable(operation_id):
    res = yt.driver.make_request("_get_operation", {"id": operation_id})
    return yson.convert.json_to_yson(json.loads(res))

def ensure_backend_is_supported():
    backend = yt.config.get_backend_type(yt)
    if backend != "http":
        print(
            "ERROR: yt-job-tool can only work with `http` backend, "
            "but is configured to use `{0}' backend".format(backend),
            file=sys.stderr)
        exit(1)

def prepare_job_environment(operation_id, job_id, job_path, run=False, full=False):
    ensure_backend_is_supported()

    if job_path is None:
        job_path = os.path.join(os.getcwd(), "job_" + job_id)

    if full:
        attributes = get_operation_attributes_from_dyntable(operation_id)
    else:
        attributes = yt.get_operation_attributes(operation_id)

    if attributes["operation_type"] in ["remote_copy", "sort", "merge"]:
        raise yt.YtError("Operation {0} is {1} operation and does not run user code"
                         .format(operation_id, attributes["operation_type"]))

    logger.info("Preparing job environment for job %s, operation %s", job_id, operation_id)

    if full:
        job_type = get_job_type_from_dyntable(operation_id, job_id)
        mode = "full"
    else:
        job_info = get_job_info_from_cypress(operation_id, job_id)
        job_type = job_info.job_type
        if job_info.is_running:
            mode = "running"
        else:
            mode = "failed"

    if job_type not in JOB_TYPE_TO_SPEC_TYPE:
        raise yt.YtError("Unknown job type \"{0}\"".format(repr(job_type)))

    op_type = JOB_TYPE_TO_SPEC_TYPE[job_type]

    makedirp(job_path)
    job_input_path = os.path.join(job_path, "input")

    download_job_input(operation_id, job_id, job_input_path, mode)

    # Sandbox files
    sandbox_path = os.path.join(job_path, "sandbox")
    makedirp(sandbox_path)

    file_count = len(attributes["spec"][op_type]["file_paths"])
    for index, file_ in enumerate(attributes["spec"][op_type]["file_paths"]):
        file_name = file_.attributes.get("file_name", yt.get_attribute(file_, "key"))
        logger.info("Downloading job file \"%s\" (%d of %d)", file_name, index + 1, file_count)
        destination_path = os.path.join(sandbox_path, file_name)
        download_file(file_, destination_path)

        if parse_bool(file_.attributes.get("executable", False)):
            os.chmod(destination_path, os.stat(destination_path).st_mode | stat.S_IXUSR)

        logger.info("Done")

    if file_count > 0:
        logger.info("Job files were downloaded to %s", sandbox_path)
    else:
        logger.info("Job has no files to download")

    command_path = os.path.join(job_path, "command")
    job_command = attributes["spec"][op_type]["command"]
    job_environment = attributes["spec"][op_type].get("environment", {})
    with open(command_path, "w") as fout:
        fout.write(job_command)
    logger.info("Command was written to %s", command_path)

    output_table_count = len(attributes["spec"]["output_table_paths"])
    use_yamr_descriptors = parse_bool(attributes["spec"][op_type].get("use_yamr_descriptors", False))

    output_path = os.path.join(job_path, "output")
    run_config = {
        "command_path": command_path,
        "sandbox_path": sandbox_path,
        "input_path": job_input_path,
        "output_path": output_path,
        "output_table_count": output_table_count,
        "use_yamr_descriptors": use_yamr_descriptors,
        "job_id": job_id,
        "operation_id": operation_id,
        "environment": job_environment,
    }
    with open(os.path.join(job_path, "run_config"), "wb") as fout:
        yson.dump(run_config, fout, yson_format="pretty")
    make_run_script(job_path)
    make_run_sh(
        os.path.join(job_path, "run.sh"),
        operation_id=run_config["operation_id"],
        job_id=run_config["job_id"],
        sandbox_path=sandbox_path,
        command=job_command,
        environment=job_environment,
        input=job_input_path,
        output_path=output_path,
        output_table_count=output_table_count,
        use_yamr_descriptors=use_yamr_descriptors)

    if run:
        run_job(job_path)
    else:
        logger.info("Done! Job can be started with \"run\" script in job directory or "
                    "with \"yt-job-tool run-job\" subcommand".format(job_path))
        print(job_path)

def create_job_tool_parser(parser):
    subparsers = parser.add_subparsers(metavar="command", dest="command")
    subparsers.required = True

    prepare_job_env_parser = subparsers.add_parser("prepare-job-environment",
                                                   help="prepare all necessary stuff for job")
    add_hybrid_argument(prepare_job_env_parser, "operation_id")
    add_hybrid_argument(prepare_job_env_parser, "job_id")
    prepare_job_env_parser.add_argument("--job-path",
                                        help="output directory to store job environment. Default: <cwd>/job_<job_id>")
    prepare_job_env_parser.add_argument("--run", help="run job when job environment is prepared",
                                        action="store_true", default=False)
    prepare_job_env_parser.add_argument("--full", help="download full input of a job",
                                        action="store_true", default=False)

    run_job_parser = subparsers.add_parser("run-job", help="runs job binary")
    add_hybrid_argument(run_job_parser, "job_path", help="path to prepared job environment")

