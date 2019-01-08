from .batch_helpers import batch_apply
from .batch_response import apply_function_to_result
from .config import get_config
from .common import (flatten, imap, round_up_to, iteritems, GB, MB,
                     get_value, unlist, get_started_by,
                     parse_bool, is_prefix, require, YtError, update)
from .cypress_commands import exists, get, remove_with_empty_dirs, get_attribute
from .errors import YtOperationFailedError
from .file_commands import LocalFile, _touch_file_in_cache
from .ypath import TablePath, FilePath
from .py_wrapper import OperationParameters
from .table_commands import is_empty, is_sorted
from .table_helpers import (FileUploader, _prepare_operation_formats, _is_python_function,
                            _prepare_binary, _prepare_source_tables, _prepare_destination_tables,
                            _prepare_table_writer, _prepare_stderr_table)

import yt.logger as logger

from yt.packages.six.moves import zip as izip

import functools
import os
import shutil
from copy import deepcopy

def _check_columns(columns, type):
    if len(columns) == 1 and "," in columns:
        logger.info('Comma found in column name "%s". '
                    'Did you mean to %s by a composite key?',
                    columns[0], type)

def _prepare_reduce_by(reduce_by, client):
    if reduce_by is None:
        if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
            reduce_by = ["key"]
        else:
            raise YtError("reduce_by option is required")
    reduce_by = flatten(reduce_by)
    _check_columns(reduce_by, "reduce")
    return reduce_by

def _prepare_join_by(join_by, required=True):
    if join_by is None:
        if required:
            raise YtError("join_by option is required")
    else:
        join_by = flatten(join_by)
        _check_columns(join_by, "join_reduce")
    return join_by

def _prepare_sort_by(sort_by, client):
    if sort_by is None:
        if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
            sort_by = ["key", "subkey"]
        else:
            raise YtError("sort_by option is required")
    sort_by = flatten(sort_by)
    _check_columns(sort_by, "sort")
    return sort_by

def _set_spec_value(builder, key, value):
    if value is not None:
        builder._spec[key] = value
    return builder

def _is_tables_sorted(table, client):
    def _is_sorted(sort_attributes):
        if not parse_bool(sort_attributes["sorted"]):
            return False
        if "columns" in table.attributes and not is_prefix(sort_attributes["sorted_by"],
                                                           table.attributes["columns"]):
            return False
        return True

    table = TablePath(table, client=client)
    sort_attributes = get(table + "/@", attributes=["sorted", "sorted_by"], client=client)
    return apply_function_to_result(_is_sorted, sort_attributes)

class Finalizer(object):
    """Entity for operation finalizing: checking size of result chunks, deleting of \
    empty output tables and temporary local files.
    """
    def __init__(self, local_files_to_remove, output_tables, spec, client=None):
        self.local_files_to_remove = local_files_to_remove
        self.output_tables = output_tables
        self.client = client
        self.spec = spec

    def __call__(self, state):
        if get_config(self.client)["clear_local_temp_files"]:
            for file_path in self.local_files_to_remove:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)

        if state == "completed":
            for table in imap(lambda table: TablePath(table, client=self.client), self.output_tables):
                self.check_for_merge(table)
        if get_config(self.client)["yamr_mode"]["delete_empty_tables"]:
            for table in imap(lambda table: TablePath(table, client=self.client), self.output_tables):
                if is_empty(table, client=self.client):
                    remove_with_empty_dirs(table, client=self.client)

    def check_for_merge(self, table):
        # TODO: fix in YT-6615
        from .run_operation_commands import run_merge

        if get_config(self.client)["auto_merge_output"]["action"] == "none":
            return

        chunk_count = int(get_attribute(table, "chunk_count", client=self.client))
        if chunk_count < get_config(self.client)["auto_merge_output"]["min_chunk_count"]:
            return

        # We use uncompressed data size to simplify recommended command
        chunk_size = float(get_attribute(table, "compressed_data_size", client=self.client)) / chunk_count
        if chunk_size > get_config(self.client)["auto_merge_output"]["max_chunk_size"]:
            return

        # NB: just get the limit lower than in default scheduler config.
        chunk_count_per_job_limit = 10000

        compression_ratio = get_attribute(table, "compression_ratio", client=self.client)
        data_size_per_job = min(16 * GB, int(500 * MB / float(compression_ratio)))

        data_size = get_attribute(table, "uncompressed_data_size", client=self.client)
        data_size_per_job = min(data_size_per_job, data_size // max(1, chunk_count // chunk_count_per_job_limit))
        data_size_per_job = max(data_size_per_job, chunk_count_per_job_limit)

        mode = "sorted" if is_sorted(table, client=self.client) else "ordered"

        if get_config(self.client)["auto_merge_output"]["action"] == "merge":
            table = TablePath(table, client=self.client)
            table.attributes.clear()
            try:
                spec = {"combine_chunks": True, "data_size_per_job": data_size_per_job}
                if "pool" in self.spec:
                    spec["pool"] = self.spec["pool"]
                run_merge(source_table=table, destination_table=table, mode=mode, spec=spec, client=self.client)
            except YtOperationFailedError:
                logger.warning("Failed to merge table %s", table)
        else:
            logger.info("Chunks of output table {0} are too small. "
                        "This may cause suboptimal system performance. "
                        "If this table is not temporary then consider running the following command:\n"
                        "yt merge --mode {1} --proxy {3} --src {0} --dst {0} "
                        "--spec '{{combine_chunks=true;data_size_per_job={2}}}'"
                        .format(table, mode, data_size_per_job, get_config(self.client)["proxy"]["url"]))

class Toucher(object):
    """Entity for touch operation files in case of retries.
    """
    def __init__(self, uploaded_files, client=None):
        self.uploaded_files = uploaded_files
        self.client = client

    def __call__(self):
        for file in self.uploaded_files:
            _touch_file_in_cache(file, client=self.client)

def spec_option(description=None, nested_spec_builder=None):
    def spec_method_decorator(func):
        @functools.wraps(func)
        def spec_method(self, value):
            return func(self, value)
        spec_method.description = description
        spec_method.__doc__ = description
        spec_method.nested_spec_builder = nested_spec_builder
        spec_method.is_spec_method = True
        return spec_method
    return spec_method_decorator

class JobIOSpecBuilder(object):
    def __init__(self, user_job_spec_builder=None, io_name=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self._spec_patch = {}
        self.user_job_spec_builder = user_job_spec_builder
        self.io_name = io_name

    @spec_option("The configuration of the table reader")
    def table_reader(self, reader):
        return _set_spec_value(self, "table_reader", reader)

    @spec_option("The configuration of the table writer")
    def table_writer(self, writer):
        return _set_spec_value(self, "table_writer", writer)

    @spec_option("The configuration of the stderr file writer")
    def error_file_writer(self, writer):
        return _set_spec_value(self, "error_file_writer", writer)

    @spec_option("The configuration of control attributes")
    def control_attributes(self, attributes):
        return _set_spec_value(self, "control_attributes", attributes)

    @spec_option("The restriction on the number of rows which can be buffered in job proxy")
    def buffer_row_count(self, count):
        return _set_spec_value(self, "buffer_row_count", count)

    @spec_option("The size of the pool for data processing in job proxy")
    def pipe_io_pool_size(self, size):
        return _set_spec_value(self, "pipe_io_pool_size", size)

    def _end_job_io(self):
        assert self.user_job_spec_builder is not None
        user_job_spec_builder = self.user_job_spec_builder
        self.user_job_spec_builder = None
        builder_func = getattr(user_job_spec_builder, self.io_name)
        builder_func(self)
        return user_job_spec_builder

    def _apply_spec_patch(self, spec):
        self._spec_patch = update(spec, self._spec_patch)

    def build(self):
        spec = update(self._spec_patch, deepcopy(self._spec))
        self._spec_patch = {}
        return spec

class PartitionJobIOSpecBuilder(JobIOSpecBuilder):
    def __init__(self, user_job_spec_builder):
        super(PartitionJobIOSpecBuilder, self).__init__(user_job_spec_builder, "partition_job_io")

    def end_partition_job_io(self):
        return self._end_job_io()

class SortJobIOSpecBuilder(JobIOSpecBuilder):
    def __init__(self, user_job_spec_builder):
        super(SortJobIOSpecBuilder, self).__init__(user_job_spec_builder, "sort_job_io")

    def end_sort_job_io(self):
        return self._end_job_io()

class MergeJobIOSpecBuilder(JobIOSpecBuilder):
    def __init__(self, user_job_spec_builder):
        super(MergeJobIOSpecBuilder, self).__init__(user_job_spec_builder, "merge_job_io")

    def end_merge_job_io(self):
        return self._end_job_io()

class ReduceJobIOSpecBuilder(JobIOSpecBuilder):
    def __init__(self, user_job_spec_builder):
        super(ReduceJobIOSpecBuilder, self).__init__(user_job_spec_builder, "reduce_job_io")

    def end_reduce_job_io(self):
        return self._end_job_io()

class MapJobIOSpecBuilder(JobIOSpecBuilder):
    def __init__(self, user_job_spec_builder):
        super(MapJobIOSpecBuilder, self).__init__(user_job_spec_builder, "map_job_io")

    def end_map_job_io(self):
        return self._end_job_io()

class UserJobSpecBuilder(object):
    def __init__(self, spec_builder=None, job_type=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec
        self._spec_patch = {}

        self._spec_builder = spec_builder
        self._job_type = job_type

    @spec_option("The string that will be completed by bash-c call")
    def command(self, binary_or_python_obj):
        return _set_spec_value(self, "command", binary_or_python_obj)

    @spec_option("The list of paths to files and tables in Cypress that will be downloaded to nodes")
    def file_paths(self, paths):
        return _set_spec_value(self, "file_paths", paths)

    @spec_option("The list of paths to Porto layers in Cypress")
    def layer_paths(self, paths):
        return _set_spec_value(self, "layer_paths", paths)

    @spec_option("The format of tabular data")
    def format(self, format):
        return _set_spec_value(self, "format", format)

    @spec_option("The format of tabular data at the input of the operation")
    def input_format(self, format):
        return _set_spec_value(self, "input_format", format)

    @spec_option("The format of tabular data at the output of the operation")
    def output_format(self, format):
        return _set_spec_value(self, "output_format", format)

    @spec_option("The dictionary of environment variables that will be mentioned during operation running")
    def environment(self, environment_dict):
        return _set_spec_value(self, "environment", environment_dict)

    @spec_option("The amount of processing cores at a job which will be taken into consideration during its scheduling")
    def cpu_limit(self, limit):
        return _set_spec_value(self, "cpu_limit", limit)

    @spec_option("The restriction on consumption of memory by job (in bytes)")
    def memory_limit(self, limit):
        return _set_spec_value(self, "memory_limit", limit)

    @spec_option("Memory reserve factor")
    def memory_reserve_factor(self, factor):
        return _set_spec_value(self, "memory_reserve_factor", factor)

    @spec_option("The path in a sandbox which tmpfs will be mounted in")
    def tmpfs_path(self, path):
        return _set_spec_value(self, "tmpfs_path", path)

    @spec_option("The restriction on size tmpfs in a job")
    def tmpfs_size(self, size):
        return _set_spec_value(self, "tmpfs_size", size)

    @spec_option("The size of the stderr which will be saved as a result of job's work")
    def max_stderr_size(self, size):
        return _set_spec_value(self, "max_stderr_size", size)

    @spec_option("The restriction on the amount of customers' statistics which can be written from job")
    def custom_statistics_count_limit(self, limit):
        return _set_spec_value(self, "custom_statistics_count_limit", limit)

    @spec_option("The restriction on job's work time (in milliseconds)")
    def job_time_limit(self, limit):
        return _set_spec_value(self, "job_time_limit", limit)

    @spec_option("Whether to write in the input stream information about indexes of input tables")
    def enable_input_table_index(self, flag=True):
        return _set_spec_value(self, "enable_input_table_index", flag)

    @spec_option("Whether to copy user files to sandbox")
    def copy_files(self, flag=True):
        return _set_spec_value(self, "copy_files", flag)

    @spec_option("Use yamr numeration for file descriptors of output tables in job")
    def use_yamr_descriptors(self, flag=True):
        return _set_spec_value(self, "use_yamr_descriptors", flag)

    @spec_option("Whether to check that job read all of input data")
    def check_input_fully_consumed(self, flag=True):
        return _set_spec_value(self, "check_input_fully_consumed", flag)

    @spec_option("The restriction on the total size of files in a job's sandbox")
    def disk_space_limit(self, limit):
        return _set_spec_value(self, "disk_space_limit", limit)

    @spec_option("The restriction on the total amount of objects in a job's sandbox")
    def inode_limit(self, limit):
        return _set_spec_value(self, "inode_limit", limit)

    @spec_option("The number of ports allocated to the user job")
    def port_count(self, port_count):
        return _set_spec_value(self, "port_count", port_count)

    def environment_variable(self, key, value):
        self._spec.setdefault("environment", {})
        self._spec["environment"][key] = str(value)
        return self

    def add_file_path(self, path):
        self._spec.setdefault("file_paths", [])
        self._spec["file_paths"].append(path)
        return self

    def _deepcopy_spec(self):
        result = {}
        for key in self._spec:
            if key != "command":
                result[key] = deepcopy(self._spec[key])
            else:
                result[key] = self._spec[key]
        return result

    def __deepcopy__(self, memodict=None):
        # Used to not copy user command.
        result = type(self)()

        for attr_name, attr_value in iteritems(self.__dict__):
            if attr_name == "_spec":
                continue
            setattr(result, attr_name, deepcopy(attr_value, memo=memodict))

        result._spec = self._deepcopy_spec()
        return result

    def _end_script(self):
        assert self._spec_builder is not None
        spec_builder = self._spec_builder
        self._spec_builder = None
        builder_func = getattr(spec_builder, self._job_type)
        builder_func(self)
        return spec_builder

    def _prepare_job_files(self, spec, group_by, should_process_key_switch, operation_type, local_files_to_remove,
                           uploaded_files, input_format, output_format, input_table_count, output_table_count, client):
        file_uploader = FileUploader(client=client)
        local_files = []
        files = []
        for file in flatten(spec.get("file_paths", [])):
            if isinstance(file, LocalFile):
                local_files.append(file)
            else:
                files.append(file)

        for file in flatten(spec.get("local_files", [])):
            local_files.append(LocalFile(file))

        local_files = file_uploader(local_files)

        params = OperationParameters(
            input_format=input_format,
            output_format=output_format,
            operation_type=operation_type,
            job_type=self._job_type,
            group_by=group_by,
            should_process_key_switch=should_process_key_switch,
            input_table_count=input_table_count,
            output_table_count=output_table_count,
            use_yamr_descriptors=spec.get("use_yamr_descriptors", False))

        prepare_result = _prepare_binary(spec["command"], file_uploader, params, client=client)

        tmpfs_size = prepare_result.tmpfs_size
        environment = prepare_result.environment
        binary = prepare_result.cmd
        title = prepare_result.title

        if local_files_to_remove is not None:
            local_files_to_remove += prepare_result.local_files_to_remove
        if uploaded_files is not None:
            uploaded_files += file_uploader.uploaded_files

        spec["command"] = binary

        if environment:
            if "environment" not in spec:
                spec["environment"] = {}
            for key, value in iteritems(environment):
                spec["environment"][key] = value

        if title:
            spec["title"] = title

        file_paths = []
        file_paths += flatten(local_files)
        file_paths += flatten(prepare_result.files)
        file_paths += list(imap(lambda path: FilePath(path, client=client), files))
        if file_paths:
            spec["file_paths"] = file_paths
        if "local_files" in spec:
            del spec["local_files"]

        return spec, tmpfs_size, file_uploader.disk_size

    def _prepare_memory_limit(self, spec, client=None):
        memory_limit = get_value(spec.get("memory_limit"), get_config(client)["memory_limit"])
        if memory_limit is not None:
            memory_limit = int(memory_limit)
        if memory_limit is None and get_config(client)["yamr_mode"]["use_yamr_defaults"]:
            memory_limit = 4 * GB

        tmpfs_size = spec.get("tmpfs_size", 0)
        if get_config(client)["pickling"]["add_tmpfs_archive_size_to_memory_limit"] and tmpfs_size > 0:
            if memory_limit is None:
                # Guess that memory limit is 512 MB.
                memory_limit = 512 * MB
            memory_limit += tmpfs_size

        if "memory_limit" not in spec and memory_limit is not None:
            spec["memory_limit"] = memory_limit
        return spec

    def _prepare_ld_library_path(self, spec, client=None):
        if _is_python_function(spec["command"]) and \
                get_config(client)["pickling"]["dynamic_libraries"]["enable_auto_collection"]:
            if "environment" not in spec:
                spec["environment"] = {}
            ld_library_path = spec["environment"].get("LD_LIBRARY_PATH")
            paths = ["./modules/_shared", "./tmpfs/modules/_shared"]
            if ld_library_path is not None:
                paths.insert(0, ld_library_path)
            spec["environment"]["LD_LIBRARY_PATH"] = os.pathsep.join(paths)
        return spec

    def _prepare_tmpfs(self, spec, tmpfs_size, disk_size, client=None):
        mount_sandbox_in_tmpfs = get_config(client)["mount_sandbox_in_tmpfs"]
        if isinstance(mount_sandbox_in_tmpfs, bool):  # COMPAT
            enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs
            additional_tmpfs_size = 0
        else:  # dict
            enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs["enable"]
            additional_tmpfs_size = mount_sandbox_in_tmpfs["additional_tmpfs_size"]

        if enable_mount_sandbox_in_tmpfs:
            for file in spec.get("file_paths", []):
                file_disk_size = None
                if hasattr(file, "attributes") and "disk_size" in file.attributes:
                    file_disk_size = file.attributes["disk_size"]
                else:
                    attributes = get(file + "/@", client=client)
                    if attributes["type"] == "table":
                        raise YtError(
                            'Attributes "disk_size" must be specified for table file "{0}"'.format(str(file)))
                    file_disk_size = attributes["uncompressed_data_size"]
                disk_size += round_up_to(file_disk_size, 4 * 1024)
            tmpfs_size += disk_size
            tmpfs_size += additional_tmpfs_size
            if "tmpfs_size" not in spec and tmpfs_size > 0:
                spec["tmpfs_size"] = tmpfs_size
                spec["copy_files"] = True
                spec["tmpfs_path"] = "."
        else:
            if spec.get("tmpfs_size", 0) > 0 and "tmpfs_path" not in spec:
                spec["tmpfs_path"] = "tmpfs"
        return spec

    def _apply_spec_patch(self, spec):
        self._spec_patch = update(spec, self._spec_patch)

    def build(self, input_tables, output_tables, operation_type, requires_command, requires_format, job_io_spec,
              local_files_to_remove=None, uploaded_files=None, group_by=None, client=None):
        require(self._spec_builder is None, lambda: YtError("The job spec builder is incomplete"))
        spec = update(self._spec_patch, self._deepcopy_spec())
        self._spec_patch = {}

        if job_io_spec is not None:
            enable_key_switch = job_io_spec.get("control_attributes", {}).get("enable_key_switch")
        else:
            enable_key_switch = False

        if "command" not in spec and not requires_command:
            return None
        require(self._spec.get("command") is not None, lambda: YtError("You should specify job command"))

        should_process_key_switch = False
        if requires_format:
            format_ = spec.pop("format", None)
            input_format, output_format = _prepare_operation_formats(
                format_, spec.get("input_format"), spec.get("output_format"), spec["command"],
                input_tables, output_tables, client)
            if getattr(input_format, "control_attributes_mode", None) == "iterator" \
                    and _is_python_function(spec["command"]) \
                    and (enable_key_switch is None or enable_key_switch) \
                    and group_by is not None:
                if "control_attributes" not in job_io_spec:
                    job_io_spec["control_attributes"] = {}
                job_io_spec["control_attributes"]["enable_key_switch"] = True
                should_process_key_switch = True
            spec["input_format"] = input_format.to_yson_type()
            spec["output_format"] = output_format.to_yson_type()
        else:
            input_format, output_format = None, None

        spec = self._prepare_ld_library_path(spec, client)
        spec, tmpfs_size, disk_size = self._prepare_job_files(
            spec, group_by, should_process_key_switch, operation_type, local_files_to_remove, uploaded_files,
            input_format, output_format, len(input_tables), len(output_tables), client)
        spec.setdefault("use_yamr_descriptors",
                        get_config(client)["yamr_mode"]["use_yamr_style_destination_fds"])
        spec.setdefault("check_input_fully_consumed",
                        get_config(client)["yamr_mode"]["check_input_fully_consumed"])
        spec = self._prepare_tmpfs(spec, tmpfs_size, disk_size, client)
        spec = self._prepare_memory_limit(spec, client)
        return spec

class TaskSpecBuilder(UserJobSpecBuilder):
    def __init__(self, name=None, spec_builder=None):
        super(TaskSpecBuilder, self).__init__(spec_builder, job_type=name)

    def job_count(self, job_count):
        self._spec["job_count"] = job_count
        return self

    def end_task(self):
        return self._end_script()

    def _end_script(self):
        spec_builder = self._spec_builder
        self._spec_builder = None
        spec_builder._spec["tasks"][self._job_type] = self
        return spec_builder

class MapperSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(MapperSpecBuilder, self).__init__(spec_builder, job_type="mapper")

    def end_mapper(self):
        return self._end_script()

class ReducerSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(ReducerSpecBuilder, self).__init__(spec_builder, job_type="reducer")

    def end_reducer(self):
        return self._end_script()

class ReduceCombinerSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(ReduceCombinerSpecBuilder, self).__init__(spec_builder, job_type="reduce_combiner")

    def end_reduce_combiner(self):
        return self._end_script()

class SpecBuilder(object):
    def __init__(self, operation_type, user_job_scripts=None, job_io_types=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self.operation_type = operation_type

        self._user_job_scripts = get_value(user_job_scripts, set())
        self._job_io_types = get_value(job_io_types, [])

        self._local_files_to_remove = []
        self._uploaded_files = []
        self._input_table_paths = []
        self._output_table_paths = []

        self._finalize = None
        self._user_spec = {}

        self._prepared_spec = None

        self.run_with_start_op = False

    @spec_option("The name of the pool in which the operation will work")
    def pool(self, pool_name):
        return _set_spec_value(self, "pool", pool_name)

    @spec_option("The weight of the operation")
    def weight(self, weight):
        return _set_spec_value(self, "weight", weight)

    @spec_option("The dictionary with limits on different resources for a given pool (user_slots, cpu, memory)")
    def resource_limits(self, limits):
        return _set_spec_value(self, "resource_limits", limits)

    @spec_option("The minimum share of the resources of the parent pool that must be allocated to this pool")
    def min_share_ratio(self, ratio):
        return _set_spec_value(self, "min_share_ratio", ratio)

    @spec_option("The maximum share of the resources of the parent pool that must be allocated to this pool")
    def max_share_ratio(self, ratio):
        return _set_spec_value(self, "max_share_ratio", ratio)

    @spec_option("The restriction on time of the operation (in milliseconds)")
    def time_limit(self, limit):
        return _set_spec_value(self, "time_limit", limit)

    @spec_option("The title of the operation")
    def title(self, title):
        return _set_spec_value(self, "title", title)

    @spec_option("The list of users who will have the rights to mutating actions on the operation and its jobs")
    def owners(self, owners):
        return _set_spec_value(self, "owners", owners)

    @spec_option("Restriction on an amount of saved stderrs of jobs")
    def max_stderr_count(self, count):
        return _set_spec_value(self, "max_stderr_count", count)

    @spec_option("An amount of failed jobs after which the operation is considered to be failed")
    def max_failed_job_count(self, count):
        return _set_spec_value(self, "max_failed_job_count", count)

    @spec_option("The behavior of the scheduler in case of unavailable input data at the operation startup")
    def unavailable_chunk_strategy(self, strategy):
        return _set_spec_value(self, "unavailable_chunk_strategy", strategy)

    @spec_option("The behavior of the scheduler in case of unavailable input data while running the operation")
    def unavailable_chunk_tactics(self, tactics):
        return _set_spec_value(self, "unavailable_chunk_tactics", tactics)

    @spec_option("If this option is specified, the scheduler will run jobs only on nodes marked with the same tag")
    def scheduling_tag(self, tag):
        return _set_spec_value(self, "scheduling_tag", tag)

    @spec_option("Filter for choosing nodes at job startup")
    def scheduling_tag_filter(self, tag_filter):
        return _set_spec_value(self, "scheduling_tag_filter", tag_filter)

    @spec_option("The maximum amount of data at the input of one job")
    def max_data_size_per_job(self, size):
        return _set_spec_value(self, "max_data_size_per_job", size)

    @spec_option("The dictionary with environment variables that can not be viewed by unauthorized users")
    def secure_vault(self, secure_vault_dict):
        return _set_spec_value(self, "secure_vault", secure_vault_dict)

    @spec_option("The path to the table for writing stderrs of jobs")
    def stderr_table_path(self, path):
        return _set_spec_value(self, "stderr_table_path", path)

    @spec_option("The path to the table for writing core dumps")
    def core_table_path(self, path):
        return _set_spec_value(self, "core_table_path", path)

    @spec_option("The query for pre-filtration of input rows")
    def input_query(self, query):
        return _set_spec_value(self, "input_query", query)

    @spec_option("The account of files with stderr and input context")
    def job_node_account(self, account):
        return _set_spec_value(self, "job_node_account", account)

    @spec_option('Whether to suspend the operation in case of "Account limit exceeded" error')
    def suspend_operation_if_account_limit_exceeded(self, flag=True):
        return _set_spec_value(self, "suspend_operation_if_account_limit_exceeded", flag)

    @spec_option("Whether to allow adaptive job splitting")
    def enable_job_splitting(self, flag=True):
        return _set_spec_value(self, "enable_job_splitting", flag)

    def spec(self, spec):
        self._user_spec = deepcopy(spec)
        return self

    def secure_vault_variable(self, key, value):
        self._spec.setdefault("secure_vault", {})
        self._spec["secure_vault"][key] = str(value)
        return self

    def _prepare_stderr_table(self, spec, client=None):
        if "stderr_table_path" in spec:
            spec["stderr_table_path"] = _prepare_stderr_table(spec["stderr_table_path"], client=client)
        return spec

    def _prepare_tables(self, spec, single_output_table=False, replace_unexisting_by_empty=True, client=None):
        require("input_table_paths" in spec,
                lambda: YtError("You should specify input_table_paths"))

        self._input_table_paths = _prepare_source_tables(
            spec["input_table_paths"],
            replace_unexisting_by_empty=replace_unexisting_by_empty,
            client=client)
        spec["input_table_paths"] = list(imap(lambda table: table.to_yson_type(), self._input_table_paths))

        output_tables_param = "output_table_path" if single_output_table else "output_table_paths"
        require(output_tables_param in spec,
                lambda: YtError("You should specify {0}".format(output_tables_param)))
        self._output_table_paths = _prepare_destination_tables(spec[output_tables_param], client=client)
        if single_output_table:
            spec[output_tables_param] = self._output_table_paths[0].to_yson_type()
        else:
            spec[output_tables_param] = list(imap(lambda table: table.to_yson_type(), self._output_table_paths))

    def _build_user_job_spec(self, spec, job_type, job_io_type, input_tables, output_tables,
                             requires_command=True, requires_format=True, group_by=None, client=None):
        if isinstance(spec[job_type], UserJobSpecBuilder):
            job_spec_builder = spec[job_type]
            spec[job_type] = job_spec_builder.build(group_by=group_by,
                                                    local_files_to_remove=self._local_files_to_remove,
                                                    uploaded_files=self._uploaded_files,
                                                    operation_type=self.operation_type,
                                                    input_tables=input_tables,
                                                    output_tables=output_tables,
                                                    requires_command=requires_command,
                                                    requires_format=requires_format,
                                                    job_io_spec=spec.get(job_io_type),
                                                    client=client)
            if spec[job_type] is None:
                del spec[job_type]
        return spec

    def _build_job_io(self, spec, job_io_type, client=None):
        table_writer = None
        if job_io_type in spec:
            if isinstance(spec.get(job_io_type), JobIOSpecBuilder):
                spec[job_io_type] = spec[job_io_type].build()
            table_writer = spec[job_io_type].get("table_writer")
        table_writer = _prepare_table_writer(table_writer, client=client)
        spec.setdefault(job_io_type, {})
        if table_writer is not None:
            spec[job_io_type]["table_writer"] = table_writer
        return spec

    def _apply_spec_patches(self, spec, spec_patches):
        for user_job_script in self._user_job_scripts:
            if user_job_script in spec_patches:
                spec.setdefault(user_job_script, UserJobSpecBuilder(job_type=user_job_script))

                user_job_spec = None
                if isinstance(spec_patches[user_job_script], UserJobSpecBuilder):
                    user_job_spec = spec_patches[user_job_script]._spec
                else:
                    user_job_spec = spec_patches[user_job_script]

                if isinstance(spec[user_job_script], UserJobSpecBuilder):
                    spec[user_job_script]._apply_spec_patch(user_job_spec)
                else:
                    spec[user_job_script] = update(user_job_spec, spec[user_job_script])

        for job_io_type in self._job_io_types:
            if job_io_type in spec_patches:
                spec.setdefault(job_io_type, JobIOSpecBuilder())

                job_io_spec = None
                if isinstance(spec_patches[job_io_type], JobIOSpecBuilder):
                    job_io_spec = spec_patches[job_io_type]._spec
                else:
                    job_io_spec = spec_patches[job_io_type]

                if isinstance(spec[job_io_type], JobIOSpecBuilder):
                    spec[job_io_type]._apply_spec_patch(job_io_spec)
                else:
                    spec[job_io_type] = update(job_io_spec, spec[job_io_type])

        return update(spec_patches, spec)

    def _apply_spec_overrides(self, spec, client=None):
        return self._apply_spec_patches(spec, deepcopy(get_config(client)["spec_overrides"]))

    def _apply_user_spec(self, spec):
        return self._apply_spec_patches(spec, get_value(self._user_spec, {}))

    def _apply_spec_defaults(self, spec, client=None):
        return update(get_config(client)["spec_defaults"], spec)

    def _prepare_spec(self, spec, client=None):
        spec = self._prepare_stderr_table(spec, client=client)

        for job_io_type in self._job_io_types:
            spec = self._build_job_io(spec, job_io_type=job_io_type, client=client)

        started_by = get_started_by()
        spec = update({"started_by": started_by}, spec)
        if get_config(client)["pool"] is not None:
            spec = update({"pool": get_config(client)["pool"]}, spec)
        if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
            spec = update({"data_size_per_job": 4 * GB}, spec)

        self._prepared_spec = spec

    def get_input_table_paths(self):
        return self._input_table_paths

    def get_output_table_paths(self):
        return self._output_table_paths

    def prepare(self, client=None):
        pass

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)

        spec = self._apply_spec_defaults(self._prepared_spec, client=client)
        return spec

    def supports_user_job_spec(self):
        return False

    def get_finalizer(self, spec, client=None):
        if self.supports_user_job_spec():
            return Finalizer(self._local_files_to_remove, self._output_table_paths, spec, client=client)
        return lambda state: None

    def get_toucher(self, client=None):
        if self.supports_user_job_spec():
            return Toucher(self._uploaded_files, client=client)
        return lambda: None

class ReduceSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(ReduceSpecBuilder, self).__init__(
            operation_type="reduce",
            user_job_scripts=["reducer"],
            job_io_types=["job_io"],
            spec=spec)

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    @spec_option("The approximate amount of data at the input of one job")
    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    @spec_option("The approximate count of jobs")
    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    @spec_option("The set of columns for grouping")
    def reduce_by(self, columns):
        return _set_spec_value(self, "reduce_by", columns)

    @spec_option("The set of columns by which input tables must be sorted")
    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    @spec_option("The set of columns for joining foreign tables")
    def join_by(self, columns):
        return _set_spec_value(self, "join_by", columns)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The list of the output tables")
    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    @spec_option("The list of pivot keys")
    def pivot_keys(self, keys):
        return _set_spec_value(self, "pivot_keys", keys)

    @spec_option(description="Take into account only size of primary tables at the job splitting")
    def consider_only_primary_size(self, flag=True):
        return _set_spec_value(self, "consider_only_primary_size", flag)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        if spec.get("sort_by") is None and spec.get("reduce_by") is not None:
            spec["sort_by"] = spec.get("reduce_by")

        spec["reduce_by"] = _prepare_reduce_by(spec.get("reduce_by"), client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client)

        if spec.get("join_by") is not None:
            spec["join_by"] = _prepare_join_by(spec.get("join_by"), required=False)

        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec
        group_by = spec.get("reduce_by")
        if spec.get("join_by") is not None:
            group_by = spec.get("join_by")

        if "reducer" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_type="reducer",
                                             job_io_type="job_io",
                                             input_tables=self.get_input_table_paths(),
                                             output_tables=self.get_output_table_paths(),
                                             group_by=group_by,
                                             client=client)
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class JoinReduceSpecBuilder(SpecBuilder):
    def __init__(self):
        super(JoinReduceSpecBuilder, self).__init__(
            operation_type="join_reduce",
            user_job_scripts=["reducer"],
            job_io_types=["job_io"])

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    @spec_option("The approximate amount of data at the input of one job")
    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    @spec_option("The approximate count of jobs")
    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    @spec_option("The set of columns for grouping")
    def join_by(self, columns):
        return _set_spec_value(self, "join_by", columns)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The list of the output tables")
    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    @spec_option(description="Take into account only size of primary tables at the job splitting")
    def consider_only_primary_size(self, flag=True):
        return _set_spec_value(self, "consider_only_primary_size", flag)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        spec["join_by"] = _prepare_join_by(spec.get("join_by"))
        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "reducer" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_type="reducer",
                                             job_io_type="job_io",
                                             input_tables=self.get_output_table_paths(),
                                             output_tables=self.get_output_table_paths(),
                                             group_by=spec.get("join_by"),
                                             client=client)
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MapSpecBuilder(SpecBuilder):
    def __init__(self):
        super(MapSpecBuilder, self).__init__(
            operation_type="map",
            user_job_scripts=["mapper"],
            job_io_types=["job_io"])

    @spec_option("The description of mapper script", nested_spec_builder=MapperSpecBuilder)
    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        return MapperSpecBuilder(self)

    @spec_option("The approximate amount of data at the input of one job")
    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    @spec_option("The approximate count of jobs")
    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The list of the output tables")
    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    @spec_option("Enable ordered mode for job splitting")
    def ordered(self, flag=True):
        return _set_spec_value(self, "ordered", flag)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "mapper" in spec:
            spec = self._build_user_job_spec(spec=spec,
                                             job_type="mapper",
                                             job_io_type="job_io",
                                             input_tables=self.get_input_table_paths(),
                                             output_tables=self.get_output_table_paths(),
                                             client=client)
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MapReduceSpecBuilder(SpecBuilder):
    def __init__(self):
        super(MapReduceSpecBuilder, self).__init__(
            operation_type="map_reduce",
            user_job_scripts=["mapper", "reducer", "reduce_combiner"],
            job_io_types=["map_job_io", "sort_job_io", "reduce_job_io"]
        )

    @spec_option("The description of mapper script", nested_spec_builder=MapperSpecBuilder)
    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        return MapperSpecBuilder(self)

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    @spec_option("The set of columns by which input tables must be sorted")
    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    @spec_option("The set of columns for grouping")
    def reduce_by(self, columns):
        return _set_spec_value(self, "reduce_by", columns)

    @spec_option("The approximate count of partitions in sorting")
    def partition_count(self, count):
        return _set_spec_value(self, "partition_count", count)

    @spec_option("The approximate amount of data in each partition")
    def partition_data_size(self, size):
        return _set_spec_value(self, "partition_data_size", size)

    @spec_option("The approximate count of map jobs")
    def map_job_count(self, count):
        return _set_spec_value(self, "map_job_count", count)

    @spec_option("The approximate amount of data at the input of one map job")
    def data_size_per_map_job(self, size):
        return _set_spec_value(self, "data_size_per_map_job", size)

    @spec_option("The percentage of source data which remains after the map phase")
    def map_selectivity_factor(self, factor):
        return _set_spec_value(self, "map_selectivity_factor", factor)

    @spec_option("Number of mapper output tables (not taking one mandatory output table into account)")
    def mapper_output_table_count(self, count):
        return _set_spec_value(self, "mapper_output_table_count", count)

    @spec_option("The account of intermediate data")
    def intermediate_data_account(self, account):
        return _set_spec_value(self, "intermediate_data_account", account)

    @spec_option("The factor of intermediate data replication")
    def intermediate_data_replication_factor(self, factor):
        return _set_spec_value(self, "intermediate_data_replication_factor", factor)

    @spec_option("The compression codec of intermediate data")
    def intermediate_compression_codec(self, codec):
        return _set_spec_value(self, "intermediate_compression_codec", codec)

    @spec_option("The access rights to intermediate data")
    def intermediate_data_acl(self, acl):
        return _set_spec_value(self, "intermediate_data_acl", acl)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The list of the output tables")
    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    @spec_option("Locality timeout for sorting")
    def sort_locality_timeout(self, timeout):
        return _set_spec_value(self, "sort_locality_timeout", timeout)

    @spec_option("The approximate amount of data at the input of one sort job")
    def data_size_per_sort_job(self, size):
        return _set_spec_value(self, "data_size_per_sort_job", size)

    @spec_option("Whether to force a startup of the reduce_combiner")
    def force_reduce_combiners(self, flag=True):
        return _set_spec_value(self, "force_reduce_combiners", flag)

    @spec_option("I/O settings of map jobs", nested_spec_builder=MapJobIOSpecBuilder)
    def map_job_io(self, job_io_spec):
        self._spec["map_job_io"] = job_io_spec
        return self

    def begin_map_job_io(self):
        return MapJobIOSpecBuilder(self)

    @spec_option("I/O settings of sort jobs", nested_spec_builder=SortJobIOSpecBuilder)
    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = job_io_spec
        return self

    def begin_sort_job_io(self):
        return SortJobIOSpecBuilder(self)

    @spec_option("I/O settings of reduce jobs", nested_spec_builder=ReduceJobIOSpecBuilder)
    def reduce_job_io(self, job_io_spec):
        self._spec["reduce_job_io"] = job_io_spec
        return self

    def begin_reduce_job_io(self):
        return ReduceJobIOSpecBuilder(self)

    @spec_option("The description of reduce_combiner script", nested_spec_builder=ReduceCombinerSpecBuilder)
    def reduce_combiner(self, reduce_combiner_script):
        self._spec["reduce_combiner"] = reduce_combiner_script
        return self

    def begin_reduce_combiner(self):
        return ReduceCombinerSpecBuilder(self)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)

        if spec.get("sort_by") is None:
            spec["sort_by"] = spec.get("reduce_by")
        spec["reduce_by"] = _prepare_reduce_by(spec.get("reduce_by"), client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client)

        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        additional_mapper_output_table_count = spec.get("mapper_output_table_count", 0)
        reducer_output_table_count = len(self.get_output_table_paths()) - additional_mapper_output_table_count
        reducer_output_tables = self.get_output_table_paths()[:reducer_output_table_count]
        mapper_output_tables = [None] + self.get_output_table_paths()[reducer_output_table_count:]

        if "mapper" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_type="mapper",
                                             job_io_type="map_job_io",
                                             input_tables=self.get_input_table_paths(),
                                             output_tables=mapper_output_tables,
                                             requires_command=False,
                                             client=client)
        if "reducer" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_type="reducer",
                                             job_io_type="reduce_job_io",
                                             input_tables=[None],
                                             output_tables=reducer_output_tables,
                                             group_by=spec.get("reduce_by"),
                                             client=client)
        if "reduce_combiner" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_type="reduce_combiner",
                                             job_io_type="reduce_job_io",
                                             input_tables=[None],
                                             output_tables=[None],
                                             group_by=spec.get("reduce_by"),
                                             requires_command=False,
                                             client=client)
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MergeSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(MergeSpecBuilder, self).__init__(
            operation_type="merge",
            job_io_types=["job_io"],
            spec=spec)

    @spec_option("The type of merge operation")
    def mode(self, mode):
        return _set_spec_value(self, "mode", mode)

    @spec_option("The set of columns by which output tables must be sorted (in case of mode=sorted)")
    def merge_by(self, columns):
        return _set_spec_value(self, "merge_by", columns)

    @spec_option("The approximate count of jobs")
    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    @spec_option("The approximate amount of data at the input of one job")
    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The path of the output table")
    def output_table_path(self, path):
        return _set_spec_value(self, "output_table_path", path)

    @spec_option("Whether to combine small chunks")
    def combine_chunks(self, flag=True):
        return _set_spec_value(self, "combine_chunks", flag)

    @spec_option("Whether to force reading all data from the input tables")
    def force_transform(self, flag=True):
        return _set_spec_value(self, "force_transform", flag)

    @spec_option("Schema inference mode")
    def schema_inference_mode(self, mode):
        return _set_spec_value(self, "schema_inference_mode", mode)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, single_output_table=True, replace_unexisting_by_empty=False, client=client)

        mode = get_value(spec.get("mode"), "auto")
        if mode == "auto":
            mode = "sorted" if all(batch_apply(_is_tables_sorted, self.get_input_table_paths(),
                                               client=client)) else "ordered"
        spec["mode"] = mode

        self._prepare_spec(spec, client=client)

class SortSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(SortSpecBuilder, self).__init__(
            operation_type="sort",
            job_io_types=["partition_job_io", "sort_job_io", "merge_job_io"],
            spec=spec)

    @spec_option("The set of columns by which input tables must be sorted")
    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    @spec_option("The approximate count of partitions")
    def partition_count(self, count):
        return _set_spec_value(self, "partition_count", count)

    @spec_option("The approximate amount of data in each partition")
    def partition_data_size(self, size):
        return _set_spec_value(self, "partition_data_size", size)

    @spec_option("The approximate amount of data at the input of one partition job")
    def data_size_per_partition_job(self, size):
        return _set_spec_value(self, "data_size_per_partition_job", size)

    @spec_option("The account of intermediate data")
    def intermediate_data_account(self, account):
        return _set_spec_value(self, "intermediate_data_account", account)

    @spec_option("The media type that stores chunks of the intermediate data")
    def intermediate_data_medium(self, medium):
        return _set_spec_value(self, "intermediate_data_medium", medium)

    @spec_option("The compression codec of intermediate data")
    def intermediate_compression_codec(self, codec):
        return _set_spec_value(self, "intermediate_compression_codec", codec)

    @spec_option("The factor of intermediate data replication")
    def intermediate_data_replication_factor(self, factor):
        return _set_spec_value(self, "intermediate_data_replication_factor", factor)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The path of the output table")
    def output_table_path(self, size):
        return _set_spec_value(self, "output_table_path", size)

    @spec_option("The approximate count of partition jobs")
    def partition_job_count(self, count):
        return _set_spec_value(self, "partition_job_count", count)

    @spec_option("Locality timeout")
    def sort_locality_timeout(self, timeout):
        return _set_spec_value(self, "sort_locality_timeout", timeout)

    @spec_option("The approximate amount of data at the input of sorted merge job")
    def data_size_per_sorted_merge_job(self, size):
        return _set_spec_value(self, "data_size_per_sorted_merge_job", size)

    @spec_option("The number of samples per partition (only for dynamic tables)")
    def samples_per_partition(self, count):
        return _set_spec_value(self, "samples_per_partition", count)

    @spec_option("Schema inference mode")
    def schema_inference_mode(self, mode):
        return _set_spec_value(self, "schema_inference_mode", mode)

    def begin_partition_job_io(self):
        return PartitionJobIOSpecBuilder(self)

    @spec_option("I/O settings of partition jobs", nested_spec_builder=PartitionJobIOSpecBuilder)
    def partition_job_io(self, job_io_spec):
        self._spec["partition_job_io"] = job_io_spec
        return self

    def begin_sort_job_io(self):
        return SortJobIOSpecBuilder(self)

    @spec_option("I/O settings of sort jobs", nested_spec_builder=SortJobIOSpecBuilder)
    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = job_io_spec
        return self

    def begin_merge_job_io(self):
        return MergeJobIOSpecBuilder(self)

    @spec_option("I/O settings of merge jobs", nested_spec_builder=MergeJobIOSpecBuilder)
    def merge_job_io(self, job_io_spec):
        self._spec["merge_job_io"] = job_io_spec
        return self

    def _prepare_tables_to_sort(self, spec, client=None):
        self._input_table_paths = _prepare_source_tables(
            spec["input_table_paths"],
            replace_unexisting_by_empty=False,
            client=client)

        exists_results = batch_apply(exists, self._input_table_paths, client=client)
        for table, exists_result in izip(self._input_table_paths, exists_results):
            require(exists_result, lambda: YtError("Table %s should exist" % table))

        spec["input_table_paths"] = list(imap(lambda table: table.to_yson_type(), self._input_table_paths))
        if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not self._input_table_paths:
            return spec

        if "output_table_path" not in spec:
            require(len(self._input_table_paths) == 1,
                    lambda: YtError("You must specify destination sort table in case of multiple source tables"))
            require(not self._input_table_paths[0].has_delimiters(),
                    lambda: YtError("Source table must not have delimiters in case of inplace sort"))
            spec["output_table_path"] = self._input_table_paths[0]

        self._output_table_paths = _prepare_destination_tables(spec["output_table_path"], client=client)
        spec["output_table_path"] = self._output_table_paths[0].to_yson_type()

        return spec

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        require("input_table_paths" in spec, lambda: YtError("You should specify \"input_table_paths\""))

        spec = self._prepare_tables_to_sort(spec, client=client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client=client)
        self._prepare_spec(spec, client=client)

class RemoteCopySpecBuilder(SpecBuilder):
    def __init__(self):
        super(RemoteCopySpecBuilder, self).__init__(operation_type="remote_copy", job_io_types=["job_io"])

    @spec_option("The name of the cluster from which you want to copy the data")
    def cluster_name(self, name):
        return _set_spec_value(self, "cluster_name", name)

    @spec_option("The name of the network you want to use when copying")
    def network_name(self, name):
        return _set_spec_value(self, "network_name", name)

    @spec_option("The configuration of connection to the source cluster")
    def cluster_connection(self, cluster):
        return _set_spec_value(self, "cluster_connection", cluster)

    @spec_option("The list of the input tables")
    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    @spec_option("The path of the output table")
    def output_table_path(self, path):
        return _set_spec_value(self, "output_table_path", path)

    @spec_option("Whether to copy attributes of input table (in case of single input)")
    def copy_attributes(self, flag=True):
        return _set_spec_value(self, "copy_attributes", flag)

    @spec_option("The list of attributes which should be copied")
    def attribute_keys(self, keys):
        return _set_spec_value(self, "attribute_keys", keys)

    @spec_option("Schema inference mode")
    def schema_inference_mode(self, mode):
        return _set_spec_value(self, "schema_inference_mode", mode)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, single_output_table=True, client=client)
        self._prepare_spec(spec, client=client)

class EraseSpecBuilder(SpecBuilder):
    def __init__(self):
        super(EraseSpecBuilder, self).__init__(operation_type="erase", job_io_types=["job_io"])

    @spec_option("The path of the table")
    def table_path(self, paths):
        return _set_spec_value(self, "table_path", paths)

    @spec_option("Whether to combine small chunks")
    def combine_chunks(self, flag=True):
        return _set_spec_value(self, "combine_chunks", flag)

    @spec_option("Schema inference mode")
    def schema_inference_mode(self, mode):
        return _set_spec_value(self, "schema_inference_mode", mode)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)
        require("table_path" in spec, lambda: YtError("You should specify table_path"))

        table_path = _prepare_source_tables(spec["table_path"], client=client)
        spec["table_path"] = unlist(list(imap(lambda table: table.to_yson_type(), table_path)))

        self._input_table_paths = [spec["table_path"]]
        self._prepare_spec(spec, client=client)

class VanillaSpecBuilder(SpecBuilder):
    def __init__(self):
        super(VanillaSpecBuilder, self).__init__(operation_type="vanilla")
        self.run_with_start_op = True
        self._spec["tasks"] = {}

    @spec_option("The description of task", nested_spec_builder=TaskSpecBuilder)
    def tasks(self, tasks):
        for name, task in iteritems(tasks):
            self.task(name, task)
        return self

    def task(self, name, task):
        self._user_job_scripts.add(name)
        self._spec["tasks"][name] = task
        return self

    def begin_task(self, name):
        self._user_job_scripts.add(name)
        return TaskSpecBuilder(name, self)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        for task in self._user_job_scripts:
            spec["tasks"] = self._build_user_job_spec(spec=spec["tasks"],
                                                      job_type=task,
                                                      job_io_type=None,
                                                      input_tables=[],
                                                      output_tables=[],
                                                      client=client,
                                                      requires_format=False)
        return spec

    def supports_user_job_spec(self):
        return True
