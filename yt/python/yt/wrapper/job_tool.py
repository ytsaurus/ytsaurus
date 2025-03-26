from __future__ import print_function

from yt.common import makedirp, YtError, YtResponseError
from yt.wrapper.cli_helpers import ParseStructuredArgument
from yt.wrapper.common import DoNotReplaceAction, chunk_iter_stream, MB
from yt.wrapper.file_commands import _get_remote_temp_files_directory
from yt.wrapper.job_commands import get_job_input, get_job_fail_context
import yt.logger as logger
import yt.wrapper as yt

import collections
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

OPERATIONS_ARCHIVE_JOBS_PATH = "//sys/operations_archive/jobs"

JobInfo = collections.namedtuple("JobInfo", ["job_type", "is_running", "task_name"])

FULL_INPUT_MODE = "full_input"
INPUT_CONTEXT_MODE = "input_context"


def shellquote(s):
    # https://stackoverflow.com/questions/35817/how-to-escape-os-system-calls-in-python
    return "'" + s.replace("'", "'\\''") + "'"


def make_environment_string(environment):
    return ''.join("export {var}={value}\n".format(var=var, value=shellquote(environment[var]))
                   for var in environment)


def get_output_descriptor_list(output_table_count, use_yamr_descriptors):
    if use_yamr_descriptors:
        return [1, 2] + [i + 3 for i in range(output_table_count)]
    else:
        # descriptor #5 is for job statistics
        return [2, 5] + [3 * i + 1 for i in range(output_table_count)]


def parse_bash_command_line(command_line):
    """Splits command_line into 3 strings: environment_variables, command, arguments.
    """
    environment_variables = ""
    command_with_args = command_line.split(None, 1)

    while len(command_with_args) == 2:
        if "=" in command_with_args[0]:
            environment_variables += command_with_args[0] + " "
            command_with_args = command_with_args[1].split(None, 1)
        else:
            return environment_variables.strip(), command_with_args[0], command_with_args[1]

    return environment_variables.strip(), command_with_args[0], ""


def make_run_sh(job_path, operation_id, job_id, sandbox_path, command, environment,
                input_path, output_path, output_table_count, use_yamr_descriptors):
    output_descriptor_list = get_output_descriptor_list(output_table_count, use_yamr_descriptors)

    command_run_sh_path = os.path.join(job_path, "run.sh")
    gdb_run_sh_path = os.path.join(job_path, "run_gdb.sh")

    # Stderr is treated separately.
    output_descriptor_list.remove(2)

    # Sandbox_suffix is suffix relative to job_path.
    sandbox_suffix = os.path.relpath(sandbox_path, job_path)

    # All other paths that we use are relative to sandbox directory
    # so user can rename job environment directory.
    input_rel_path = os.path.relpath(input_path, sandbox_path)
    output_rel_path = os.path.relpath(output_path, sandbox_path)
    output_descriptors_spec = " ".join(
        "{d}> {output_rel_path}/{d}".format(d=d, output_rel_path=output_rel_path)
        for d in output_descriptor_list)
    output_descriptors_spec += " 2> >(tee {output_rel_path}/2 >&2)".format(output_rel_path=output_rel_path)

    if "BASH_ENV" in environment:
        run_bash_env_command = ". \"$BASH_ENV\""
    else:
        run_bash_env_command = ""

    environment_script = """\
#!/usr/bin/env bash

SANDBOX_DIR="$(dirname $0)/{sandbox_suffix}"
cd "$SANDBOX_DIR"

mkdir -p {output_rel_path}

export YT_JOB_INDEX=0
export YT_START_ROW_INDEX=0
export YT_OPERATION_ID={operation_id}
export YT_JOB_ID={job_id}
export YT_STARTED_BY_JOB_TOOL=1
export SHELL=/bin/bash
{environment}

{run_bash_env_command}
""".format(
        sandbox_suffix=sandbox_suffix,
        operation_id=operation_id,
        job_id=job_id,
        run_bash_env_command=run_bash_env_command,
        environment=make_environment_string(environment),
        output_rel_path=output_rel_path)

    command_script = """\
({command}) < {input_rel_path} {output_descriptors_spec}
""".format(
        command=command,
        input_rel_path=input_rel_path,
        output_descriptors_spec=output_descriptors_spec)

    gdb_environment_variables, gdb_command, gdb_arguments = parse_bash_command_line(command)

    gdb_script = """\
gdb {gdb_command} -ex 'set args {gdb_args} < {input_rel_path} {output_descriptors_spec}' -ex 'set environment {gdb_env}'
""".format(
        gdb_command=gdb_command,
        gdb_args=gdb_arguments,
        gdb_env=gdb_environment_variables,
        input_rel_path=input_rel_path,
        output_descriptors_spec=output_descriptors_spec)

    for run_sh_path, script in [(command_run_sh_path, command_script),
                                (gdb_run_sh_path, gdb_script),
                                ]:
        with open(run_sh_path, "w") as out:
            out.write(environment_script + script)
        os.chmod(run_sh_path, 0o744)


def add_hybrid_argument(parser, name, group_required=True, **kwargs):
    group = parser.add_mutually_exclusive_group(required=group_required)
    group.add_argument(name, nargs="?", action=DoNotReplaceAction, **kwargs)
    group.add_argument("--" + name.replace("_", "-"), **kwargs)


def download_file(path, destination_path, client):
    with open(destination_path, "wb") as f:
        for chunk in chunk_iter_stream(client.read_file(path), 16 * MB):
            f.write(chunk)


def download_table(path, destination_path, client):
    with open(destination_path, "wb") as f:
        for r in client.read_table(path, format=path.attributes["format"], raw=True):
            f.write(r)


def run_job(job_path, env=None):
    if not os.path.exists(job_path):
        raise yt.YtError("Job path {0} does not exist".format(job_path))

    run_script = os.path.join(job_path, "run.sh")
    p = subprocess.Popen([run_script], env=env, close_fds=False)
    sys.exit(p.wait())


def download_job_input(operation_id, job_id, job_input_path, get_context_action, client):
    if get_context_action == "dump_job_context":
        logger.info("Job is running, using its input context as local input")
        output_path = client.find_free_subpath(_get_remote_temp_files_directory(client=None))
        client.dump_job_context(job_id, output_path)
        try:
            download_file(output_path, job_input_path, client)
        finally:
            client.remove(output_path, force=True)
    elif get_context_action in ["get_job_fail_context", "get_job_input"]:
        if get_context_action == "get_job_fail_context":
            logger.info("Job is failed, using its fail context as local input")
            job_input_stream = get_job_fail_context(operation_id, job_id)
        else:
            logger.info("Downloading full job input")
            try:
                job_input_stream = get_job_input(job_id)
            except YtError as err:
                raise YtError("Failed to download job input. To get job fail context, use option --context",
                              inner_errors=[err])
        with open(job_input_path, "wb") as out:
            for chunk in chunk_iter_stream(job_input_stream, 16 * MB):
                out.write(chunk)
    else:
        raise ValueError("Unknown get_context_action: {0}".format(get_context_action))

    logger.info("Job input is downloaded to %s", job_input_path)


def get_job_info(operation_id, job_id, client):
    job_info = client.get_job(operation_id, job_id)
    job_is_running = job_info["state"] == "running"
    return JobInfo(job_info["type"], is_running=job_is_running, task_name=job_info.get("task_name", None))


def ensure_backend_is_supported(client):
    backend = yt.config.get_backend_type(client=client)
    if backend != "http":
        print(
            "ERROR: yt-job-tool can only work with `http` backend, "
            "but is configured to use `{0}' backend".format(backend),
            file=sys.stderr)
        exit(1)


def get_user_job_spec_section(op_spec, job_info):
    if job_info.job_type in ("map", "partition_map", "ordered_map"):
        return op_spec["mapper"]
    elif job_info.job_type in ("partition_reduce", "reduce_combiner", "join_reduce", "sorted_reduce"):
        return op_spec["reducer"]
    elif job_info.job_type == "vanilla":
        return op_spec["tasks"][job_info.task_name]
    else:
        raise yt.YtError("Unknown job type \"{0}\"".format(job_info.job_type))


def prepare_job_environment(operation_id, job_id, job_path, run=False, get_context_mode=INPUT_CONTEXT_MODE):
    def _download_files(user_job_spec_section, sandbox_path, client):
        for index, file_ in enumerate(user_job_spec_section["file_paths"]):
            file_original_attrs = client.get(file_ + "&/@", attributes=["key", "file_name"])
            file_attrs = client.get(file_ + "/@", attributes=["type"])

            file_name = file_.attributes.get("file_name")
            if file_name is None:
                file_name = file_original_attrs.get("file_name")
            if file_name is None:
                file_name = file_original_attrs.get("key")

            file_name_parts = file_name.split("/")
            makedirp(os.path.join(sandbox_path, *file_name_parts[:-1]))
            logger.info("Downloading job file \"%s\" (%d of %d)", file_name, index + 1, file_count)
            destination_path = os.path.join(sandbox_path, *file_name_parts)
            node_type = file_attrs["type"]
            if node_type == "file":
                download_file(file_, destination_path, client)
            elif node_type == "table":
                download_table(file_, destination_path, client)
            else:
                raise yt.YtError("Unknown format of job file node: {0}".format(node_type))

            if file_.attributes.get("executable", False):
                os.chmod(destination_path, os.stat(destination_path).st_mode | stat.S_IXUSR)

            logger.info("Done")

    client = yt.YtClient(config=yt.config.config)

    if get_context_mode not in (INPUT_CONTEXT_MODE, FULL_INPUT_MODE):
        raise YtError(
            "Incorrect get_context_mode {}, expected one of ({}, {})"
            .format(
                repr(get_context_mode),
                repr(INPUT_CONTEXT_MODE),
                repr(FULL_INPUT_MODE)
            ))

    # NB: we should explicitly reset this option to default value since CLI usually set True to it.
    client.config["default_value_of_raw_option"] = None

    ensure_backend_is_supported(client)

    if job_path is None:
        job_path = os.path.join(os.getcwd(), "job_" + job_id)

    operation_info = client.get_operation(operation_id, attributes=["type", "spec"])
    op_type = operation_info["type"]
    op_spec = operation_info["spec"]
    if op_type in ["remote_copy", "sort", "merge"]:
        raise yt.YtError("Operation {0} is {1} operation and does not run user code"
                         .format(operation_id, op_type))

    logger.info("Preparing job environment for job %s, operation %s", job_id, operation_id)

    job_info = get_job_info(operation_id, job_id, client)
    if get_context_mode == FULL_INPUT_MODE:
        get_context_action = "get_job_input"
    elif job_info.is_running:
        get_context_action = "dump_job_context"
    else:
        get_context_action = "get_job_fail_context"

    full_info = client.get_operation(operation_id)

    user_job_spec_section = get_user_job_spec_section(op_spec, job_info)

    makedirp(job_path)
    job_input_path = os.path.join(job_path, "input")

    if job_info.job_type == "vanilla":
        # We create empty input file.
        # Our running script requires it and we don't want to patch this script for vanilla case.
        with open(job_input_path, "w"):
            pass
    else:
        download_job_input(operation_id, job_id, job_input_path, get_context_action, client)

    # Sandbox files
    sandbox_path = os.path.join(job_path, "sandbox")
    makedirp(sandbox_path)

    file_count = len(user_job_spec_section["file_paths"])

    downloaded_with_user_tx = False
    if "user_transaction_id" in full_info:
        try:
            with client.Transaction(transaction_id=full_info["user_transaction_id"]):
                _download_files(user_job_spec_section, sandbox_path, client)
                downloaded_with_user_tx = True
        except YtResponseError as err:
            if not err.is_no_such_transaction():
                raise

    if not downloaded_with_user_tx:
        _download_files(user_job_spec_section, sandbox_path, client)

    if file_count > 0:
        logger.info("Job files were downloaded to %s", sandbox_path)
    else:
        logger.info("Job has no files to download")

    command_path = os.path.join(job_path, "command")
    job_command = user_job_spec_section["command"]
    job_environment = user_job_spec_section.get("environment", {})
    with open(command_path, "w") as fout:
        fout.write(job_command)
    logger.info("Command was written to %s", command_path)

    if job_info.job_type == "vanilla":
        output_table_count = len(user_job_spec_section.get("output_table_paths", []))
    else:
        output_table_count = len(op_spec["output_table_paths"])

    use_yamr_descriptors = user_job_spec_section.get("use_yamr_descriptors", False)

    output_path = os.path.join(job_path, "output")
    make_run_sh(
        job_path,
        operation_id=operation_id,
        job_id=job_id,
        sandbox_path=sandbox_path,
        command=job_command,
        environment=job_environment,
        input_path=job_input_path,
        output_path=output_path,
        output_table_count=output_table_count,
        use_yamr_descriptors=use_yamr_descriptors)

    if run:
        run_job(job_path)
    else:
        logger.info(
            "Done! Job can be started with \"run\" script in job directory ({}) or "
            "with \"yt-job-tool run-job\" subcommand".format(job_path))
        return job_path


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

    get_context_mode_group = prepare_job_env_parser.add_mutually_exclusive_group()
    get_context_mode_group.add_argument("--full-input", "--full", dest="get_context_mode", action="store_const",
                                        help="download input context of a job", const=FULL_INPUT_MODE,
                                        default=FULL_INPUT_MODE)
    get_context_mode_group.add_argument("--context", dest="get_context_mode", action="store_const",
                                        help="download fail context of a job", const=INPUT_CONTEXT_MODE)

    run_job_parser = subparsers.add_parser("run-job", help="runs job binary")
    add_hybrid_argument(run_job_parser, "job_path", help="path to prepared job environment")
    run_job_parser.add_argument("--env", action=ParseStructuredArgument, help="environment to use in script run in YSON format")


def run_prepare_job_environment(**args):
    result = prepare_job_environment(**args)
    if result is not None:
        print(result)


def process_job_tool_arguments(**args):
    commands = {
        "prepare-job-environment": prepare_job_environment,
        "run-job": run_job,
    }
    command = args.pop("command")
    commands[command](**args)
