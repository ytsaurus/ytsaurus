from .acl_commands import AclBuilder
from .batch_helpers import batch_apply
from .batch_response import apply_function_to_result
from .config import get_config
from .common import (flatten, imap, round_up_to, iteritems, GB, MB,
                     get_value, unlist, get_started_by,
                     parse_bool, is_prefix, require, YtError, update,
                     underscore_case_to_camel_case)
from .cypress_commands import exists, get, remove_with_empty_dirs, get_attribute
from .errors import YtOperationFailedError
from .file_commands import LocalFile, _touch_file_in_cache
from .ypath import TablePath, FilePath
from .py_wrapper import OperationParameters, TempfilesManager, get_local_temp_directory, WrapResult
from .schema import TableSchema
from .spec_builder_helpers import BaseLayerDetector
from .table_commands import is_empty, is_sorted
from .table_helpers import (FileManager, _prepare_operation_formats, _is_python_function,
                            _prepare_python_command, _prepare_source_tables, _prepare_destination_tables,
                            _prepare_table_writer, _prepare_stderr_table)
from .prepare_operation import (TypedJob, run_operation_preparation,
                                SimpleOperationPreparationContext, IntermediateOperationPreparationContext)
from .local_mode import is_local_mode, enable_local_files_usage_in_job
try:
    from yt.python.yt.cpp_wrapper import CppJob
    _CPP_WRAPPER_AVAILABLE = True
except ImportError:
    _CPP_WRAPPER_AVAILABLE = False
from .format import CppUninitializedFormat

import yt.logger as logger

try:
    from yt.packages.six import PY3, text_type
    from yt.packages.six.moves import zip as izip
except ImportError:
    from six import PY3, text_type
    from six.moves import zip as izip

import functools
import os
import shutil
from copy import deepcopy


def _convert_to_bytes(value):
    if not PY3:  # Fast path
        return str(value)

    if isinstance(value, text_type):
        return value.encode("ascii")
    else:
        return value


def _check_columns(columns, type):
    if len(columns) == 1 and "," in columns:
        logger.info('Comma found in column name "%s". '
                    'Did you mean to %s by a composite key?',
                    columns[0], type)


def _prepare_reduce_by(reduce_by, client, required=True):
    if reduce_by is None:
        if get_config(client)["yamr_mode"]["use_yamr_sort_reduce_columns"]:
            reduce_by = ["key"]
        elif required:
            raise YtError("reduce_by option is required")
    else:
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
    def _is_sorted(table_attributes):
        node_type = table_attributes["type"]
        if node_type != "table":
            raise YtError('"{}" must be a table, but current node type is "{}"'.format(table, node_type))

        if not parse_bool(table_attributes["sorted"]):
            return False
        if "columns" in table.attributes and not is_prefix(table_attributes["sorted_by"],
                                                           table.attributes["columns"]):
            return False
        return True

    table = TablePath(table, client=client)
    table_attributes = get(table + "/@", attributes=["type", "sorted", "sorted_by"], client=client)
    return apply_function_to_result(_is_sorted, table_attributes)


def _is_cpp_job(command):
    return _CPP_WRAPPER_AVAILABLE and isinstance(command, CppJob)


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
        def spec_method(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        spec_method.description = description
        spec_method.__doc__ = description
        spec_method.nested_spec_builder = nested_spec_builder
        spec_method.is_spec_method = True
        return spec_method
    return spec_method_decorator


class AclBuilderBase(AclBuilder):
    """Base builder for acl sections in specs.
    """
    def __init__(self, spec_builder, attribute, possible_permissions):
        self._attribute = attribute
        self._spec_builder = spec_builder
        super(AclBuilderBase, self).__init__(possible_permissions)

    def _end_acl(self):
        assert self._spec_builder is not None
        spec_builder = self._spec_builder
        self._spec_builder = None
        setter = getattr(spec_builder, self._attribute)
        setter(self.build())
        return spec_builder


class OperationAclBuilder(AclBuilderBase):
    """Builder for acl section in operation spec.
    """
    def __init__(self, spec_builder):
        super(OperationAclBuilder, self).__init__(spec_builder, attribute="acl", possible_permissions=["read", "manage"])

    def end_acl(self):
        return self._end_acl()


class IntermediateDataAclBuilder(AclBuilderBase):
    """Builder for intermediate_data_acl section in MapReduce operation spec.
    """
    def __init__(self, spec_builder):
        super(IntermediateDataAclBuilder, self).__init__(spec_builder, attribute="intermediate_data_acl")

    def end_intermediate_data_acl(self):
        return self._end_acl()


class JobIOSpecBuilderBase(object):
    """Base builder for job_io section in operation spec.
    """
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
        """Builds final spec."""
        spec = update(self._spec_patch, deepcopy(self._spec))
        self._spec_patch = {}
        return spec


class JobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(JobIOSpecBuilder, self).__init__(user_job_spec_builder, "job_io")

    def end_job_io(self):
        """Ends job_io section."""
        return self._end_job_io()


class PartitionJobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for partition_job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(PartitionJobIOSpecBuilder, self).__init__(user_job_spec_builder, "partition_job_io")

    def end_partition_job_io(self):
        """Ends partition_job_io section."""
        return self._end_job_io()


class SortJobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for sort_job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(SortJobIOSpecBuilder, self).__init__(user_job_spec_builder, "sort_job_io")

    def end_sort_job_io(self):
        """Ends sort_job_io section."""
        return self._end_job_io()


class MergeJobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for merge_job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(MergeJobIOSpecBuilder, self).__init__(user_job_spec_builder, "merge_job_io")

    def end_merge_job_io(self):
        """Ends merge_job_io section."""
        return self._end_job_io()


class ReduceJobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for reduce_job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(ReduceJobIOSpecBuilder, self).__init__(user_job_spec_builder, "reduce_job_io")

    def end_reduce_job_io(self):
        """Ends reduce_job_io section."""
        return self._end_job_io()


class MapJobIOSpecBuilder(JobIOSpecBuilderBase):
    """Builder for map_job_io section in operation spec.
    """
    def __init__(self, user_job_spec_builder):
        super(MapJobIOSpecBuilder, self).__init__(user_job_spec_builder, "map_job_io")

    def end_map_job_io(self):
        """Ends map_job_io section."""
        return self._end_job_io()


class UserJobSpecBuilder(object):
    """Base builder for user_job sections in operation spec.
    """
    def __init__(self, spec_builder=None, job_type=None, job_name=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self._spec_patch = {}

        self._user_spec = {}

        self._spec_builder = spec_builder
        self._job_type = job_type
        self._job_name = get_value(job_name, job_type)

    @spec_option("The string that will be completed by bash-c call")
    def command(self, binary_or_python_obj):
        return _set_spec_value(self, "command", binary_or_python_obj)

    @spec_option("The list of paths to files and tables in Cypress that will be downloaded to nodes")
    def file_paths(self, paths):
        return _set_spec_value(self, "file_paths", paths)

    @spec_option("The list of paths to Porto layers in Cypress")
    def layer_paths(self, paths):
        return _set_spec_value(self, "layer_paths", paths)

    @spec_option("Docker image for root fs layer")
    def docker_image(self, docker_image):
        return _set_spec_value(self, "docker_image", docker_image)

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

    @spec_option("The amount of gpu units at a job which will be mounted to the job container")
    def gpu_limit(self, limit):
        return _set_spec_value(self, "gpu_limit", limit)

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

    @spec_option("Force container cpu limit for user job; this option should not be normally set by user")
    def set_container_cpu_limit(self, set_container_cpu_limit):
        return _set_spec_value(self, "set_container_cpu_limit", set_container_cpu_limit)

    @spec_option("Adds environment variable")
    def environment_variable(self, key, value):
        self._spec.setdefault("environment", {})
        self._spec["environment"][key] = str(value)
        return self

    @spec_option("Adds file to operation")
    def add_file_path(self, path):
        self._spec.setdefault("file_paths", [])
        self._spec["file_paths"].append(path)
        return self

    @spec_option("Patches spec by given dict")
    def spec(self, spec):
        self._user_spec = deepcopy(spec)
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
        builder_func = getattr(spec_builder, self._job_name)
        builder_func(self)
        return spec_builder

    def _prepare_job_files(self, spec, group_by, should_process_key_switch, operation_type, local_files_to_remove,
                           uploaded_files, input_format, output_format, input_tables, output_tables, client):
        file_manager = FileManager(client=client)
        files = []
        for file in flatten(spec.get("file_paths", [])):
            if isinstance(file, LocalFile):
                file_manager.add_files(file)
            else:
                files.append(file)

        if group_by is not None and input_format._encoding is None:
            group_by = [_convert_to_bytes(key) for key in group_by]

        params = OperationParameters(
            input_format=input_format,
            output_format=output_format,
            operation_type=operation_type,
            job_type=self._job_type,
            group_by=group_by,
            should_process_key_switch=should_process_key_switch,
            input_table_count=len(input_tables),
            output_table_count=len(output_tables),
            use_yamr_descriptors=spec.get("use_yamr_descriptors", False))

        is_cpp_job = _is_cpp_job(spec["command"])
        if is_cpp_job:
            state_bytes, spec_patch = spec["command"].prepare_state_and_spec_patch(
                group_by,
                input_tables,
                output_tables,
                client,
            )

            if "input_format" in spec_patch:
                spec["input_format"] = spec_patch["input_format"]
            if "output_format" in spec_patch:
                spec["output_format"] = spec_patch["output_format"]

            temp_directory = get_local_temp_directory(client)
            with TempfilesManager(False, temp_directory) as tempfiles_manager:
                if state_bytes:
                    state_filename = tempfiles_manager.create_tempfile(
                        dir=temp_directory,
                        prefix="jobstate"
                    )
                    with open(state_filename, "wb") as fout:
                        fout.write(state_bytes)
                    file_manager.add_files(LocalFile(state_filename, file_name="jobstate"))
                    params.has_state = True

                for small_file in spec_patch.get("small_files", []):
                    small_file_filename = tempfiles_manager.create_tempfile(
                        dir=temp_directory,
                        prefix=small_file["file_name"],
                    )
                    with open(small_file_filename, "wb") as fout:
                        fout.write(small_file["data"].encode("utf-8"))
                    file_manager.add_files(LocalFile(small_file_filename, file_name=small_file["file_name"]))

        if _is_python_function(spec["command"]) or is_cpp_job:
            local_mode = is_local_mode(client)
            remove_temp_files = get_config(client)["clear_local_temp_files"] and not local_mode
            with TempfilesManager(remove_temp_files, get_local_temp_directory(client)) as tempfiles_manager:
                prepare_result = _prepare_python_command(
                    spec["command"],
                    file_manager,
                    tempfiles_manager,
                    params,
                    local_mode,
                    client=client)
                if enable_local_files_usage_in_job(client):
                    prepare_result.local_files_to_remove += \
                        tempfiles_manager._tempfiles_pool + [tempfiles_manager.tmp_dir]
                local_files = file_manager.upload_files()
        else:
            title = spec["command"].split(' ')[0] if isinstance(spec["command"], str) else None
            prepare_result = WrapResult(
                cmd=spec["command"],
                tmpfs_size=0,
                environment={},
                local_files_to_remove=[],
                title=title)
            local_files = file_manager.upload_files()

        tmpfs_size = prepare_result.tmpfs_size
        environment = prepare_result.environment
        binary = prepare_result.cmd
        title = prepare_result.title

        if local_files_to_remove is not None:
            local_files_to_remove += prepare_result.local_files_to_remove
        if uploaded_files is not None:
            uploaded_files += file_manager.uploaded_files

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
        file_paths += list(imap(lambda path: FilePath(path, client=client), files))
        if file_paths:
            spec["file_paths"] = file_paths
        if "local_files" in spec:
            del spec["local_files"]

        return spec, tmpfs_size, input_tables

    def _prepare_memory_limit(self, spec, client=None):
        memory_limit = get_value(
            spec.get("memory_limit"),
            get_value(
                get_config(client)["memory_limit"],
                get_config(client)["user_job_spec_defaults"].get("memory_limit")
            )
        )
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

        if memory_limit is not None:
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

    def _prepare_tmpfs(self, spec, tmpfs_size, client=None):
        disk_size = 0
        mount_sandbox_in_tmpfs = get_config(client)["mount_sandbox_in_tmpfs"]
        if isinstance(mount_sandbox_in_tmpfs, bool):  # COMPAT
            enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs
            additional_tmpfs_size = 0
        else:  # dict
            enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs["enable"]
            additional_tmpfs_size = mount_sandbox_in_tmpfs["additional_tmpfs_size"]

        if enable_mount_sandbox_in_tmpfs:
            for file in spec.get("file_paths", []):
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

            if "tmpfs_size" in spec:
                tmpfs_size = spec["tmpfs_size"]

            if tmpfs_size > 0:
                spec["tmpfs_size"] = tmpfs_size
                spec["copy_files"] = True
                spec["tmpfs_path"] = "."
        else:
            if spec.get("tmpfs_size", tmpfs_size) > 0 and "tmpfs_path" not in spec:
                spec["tmpfs_path"] = "tmpfs"
                spec["tmpfs_size"] = spec.get("tmpfs_size", tmpfs_size)
        return spec

    def _apply_spec_patch(self, spec):
        self._spec_patch = update(spec, self._spec_patch)

    def _supports_row_index(self, operation_type):
        return (self._job_type != "reducer" and self._job_type != "reduce_combiner") or operation_type != "map_reduce"

    @staticmethod
    def _set_control_attribute(job_io_spec, key, value):
        assert job_io_spec is not None
        if "control_attributes" not in job_io_spec:
            job_io_spec["control_attributes"] = {}
        job_io_spec["control_attributes"][key] = value

    def build(self, operation_preparation_context, operation_type, requires_command,
              requires_format, job_io_spec, has_input_query,
              local_files_to_remove=None, uploaded_files=None, group_by=None, client=None):
        """Builds final spec."""
        require(self._spec_builder is None, lambda: YtError("The job spec builder is incomplete"))

        spec = self._spec_patch
        self._spec_patch = {}
        spec = update(spec, self._deepcopy_spec())

        if job_io_spec is not None:
            enable_key_switch = job_io_spec.get("control_attributes", {}).get("enable_key_switch")
        else:
            enable_key_switch = False

        if "command" not in spec and not requires_command:
            return None
        require(spec.get("command") is not None, lambda: YtError("You should specify job command"))

        command = spec["command"]

        if not requires_format:
            input_format, output_format = None, None
            input_tables = operation_preparation_context.get_input_paths()
            output_tables = operation_preparation_context.get_output_paths()
            should_process_key_switch = False
        elif isinstance(command, TypedJob):
            if "input_format" in spec or "output_format" in spec or "format" in spec:
                raise YtError("Typed job {} must not be used with explicit format specification".format(type(command)))
            if has_input_query:
                raise YtError("Typed job {} is incompatible with \"input_query\" spec field".format(type(command)))
            assert operation_preparation_context is not None

            if self._supports_row_index(operation_type):
                self._set_control_attribute(job_io_spec, "enable_row_index", True)
            self._set_control_attribute(job_io_spec, "enable_key_switch", True)
            spec["enable_input_table_index"] = True
            input_format, output_format, input_tables, output_tables = run_operation_preparation(
                command,
                operation_preparation_context,
                input_control_attributes=job_io_spec["control_attributes"],
            )
            should_process_key_switch = True

        elif _is_cpp_job(command):
            if "input_format" in spec or "output_format" in spec or "format" in spec:
                raise YtError("Cpp job must not be used with explicit format specification")
            # TODO(egor-gutrov): mb check has_input_query?

            if self._supports_row_index(operation_type):
                self._set_control_attribute(job_io_spec, "enable_row_index", True)
                self._set_control_attribute(job_io_spec, "enable_range_index", True)
            should_process_key_switch = True

            input_tables = operation_preparation_context.get_input_paths()
            output_tables = operation_preparation_context.get_output_paths()
            input_format, output_format = CppUninitializedFormat(), CppUninitializedFormat()

        else:
            format_ = spec.pop("format", None)
            input_tables = operation_preparation_context.get_input_paths()
            output_tables = operation_preparation_context.get_output_paths()
            input_format, output_format = _prepare_operation_formats(
                format_, spec.get("input_format"), spec.get("output_format"), command,
                input_tables, output_tables, client)
            should_process_key_switch = (
                group_by is not None and
                getattr(input_format, "control_attributes_mode", None) == "iterator" and
                _is_python_function(spec["command"]) and
                (enable_key_switch is None or enable_key_switch)
            )

        if should_process_key_switch:
            self._set_control_attribute(job_io_spec, "enable_key_switch", True)

        if input_format is not None:
            spec["input_format"] = input_format.to_yson_type()
        if output_format is not None:
            spec["output_format"] = output_format.to_yson_type()

        spec = self._prepare_ld_library_path(spec, client)
        spec, tmpfs_size, input_tables = self._prepare_job_files(
            spec, group_by, should_process_key_switch, operation_type, local_files_to_remove, uploaded_files,
            input_format, output_format, input_tables, output_tables, client)
        spec.setdefault("use_yamr_descriptors",
                        get_config(client)["yamr_mode"]["use_yamr_style_destination_fds"])
        spec.setdefault("check_input_fully_consumed",
                        get_config(client)["yamr_mode"]["check_input_fully_consumed"])
        spec = self._prepare_tmpfs(spec, tmpfs_size, client)
        spec = self._prepare_memory_limit(spec, client)
        spec = BaseLayerDetector.guess_base_layers(spec, client)
        spec = update(spec, self._user_spec)
        spec = update(get_config(client)["user_job_spec_defaults"], spec)
        return spec, input_tables, output_tables


class TaskSpecBuilder(UserJobSpecBuilder):
    """Builder for task section in vanilla operation spec.
    """
    def __init__(self, name=None, spec_builder=None):
        super(TaskSpecBuilder, self).__init__(spec_builder, job_type="vanilla", job_name=name)

    @spec_option("The approximate count of jobs")
    def job_count(self, job_count):
        self._spec["job_count"] = job_count
        return self

    def end_task(self):
        """Ends task section."""
        return self._end_script()

    def _end_script(self):
        spec_builder = self._spec_builder
        self._spec_builder = None
        spec_builder._spec["tasks"][self._job_name] = self
        return spec_builder


class MapperSpecBuilder(UserJobSpecBuilder):
    """Builder for mapper section in operation spec.
    """
    def __init__(self, spec_builder=None):
        super(MapperSpecBuilder, self).__init__(spec_builder, job_type="mapper")

    def end_mapper(self):
        """Ends mapper section."""
        return self._end_script()


class ReducerSpecBuilder(UserJobSpecBuilder):
    """Builder for reducer section in operation spec.
    """
    def __init__(self, spec_builder=None):
        super(ReducerSpecBuilder, self).__init__(spec_builder, job_type="reducer")

    def end_reducer(self):
        """Ends reducer section."""
        return self._end_script()


class ReduceCombinerSpecBuilder(UserJobSpecBuilder):
    """Builder for reducer_combiner section in operation spec.
    """
    def __init__(self, spec_builder=None):
        super(ReduceCombinerSpecBuilder, self).__init__(spec_builder, job_type="reduce_combiner")

    def end_reduce_combiner(self):
        """Ends reduce_combiner section."""
        return self._end_script()


class SpecBuilder(object):
    """Base builder for operation spec.
    """
    def __init__(self, operation_type, user_job_scripts=None, job_io_types=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self.operation_type = operation_type

        self._user_job_scripts = get_value(user_job_scripts, [])
        self._job_io_types = get_value(job_io_types, [])

        self._local_files_to_remove = []
        self._uploaded_files = []
        self._input_table_paths = []
        self._output_table_paths = []

        self._finalize = None
        self._user_spec = {}

        self._prepared_spec = None

    @spec_option("The name of the pool in which the operation will work")
    def pool(self, pool_name):
        return _set_spec_value(self, "pool", pool_name)

    @spec_option("The weight of the operation")
    def weight(self, weight):
        return _set_spec_value(self, "weight", weight)

    @spec_option("The list of the trees in which the job operations will work")
    def pool_trees(self, trees):
        return _set_spec_value(self, "pool_trees", trees)

    @spec_option("The list of the tentative trees in which the operation will try to launch jobs")
    def tentative_pool_trees(self, trees):
        return _set_spec_value(self, "tentative_pool_trees", trees)

    @spec_option("The dictionary with limits on different resources for a given pool (user_slots, cpu, memory)")
    def resource_limits(self, limits):
        return _set_spec_value(self, "resource_limits", limits)

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

    @spec_option("Acl for operation, only read or manage are supported")
    def acl(self, acl):
        self._spec["acl"] = acl
        return self

    def begin_acl(self):
        """Start ACL builder."""
        return OperationAclBuilder(self)

    @spec_option("Restriction on an amount of saved stderrs of jobs")
    def max_stderr_count(self, count):
        return _set_spec_value(self, "max_stderr_count", count)

    @spec_option("Operation alias")
    def alias(self, alias):
        return _set_spec_value(self, "alias", alias)

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

    @spec_option("Description which is visible at the operation page")
    def description(self, description):
        return _set_spec_value(self, "description", description)

    @spec_option("Adds secure value to secure vault")
    def secure_vault_variable(self, key, value):
        self._spec.setdefault("secure_vault", {})
        self._spec["secure_vault"][key] = str(value)
        return self

    @spec_option("Patches spec by given dict")
    def spec(self, spec):
        self._user_spec = deepcopy(spec)
        return self

    def _prepare_stderr_table(self, spec, client=None):
        if "stderr_table_path" in spec:
            spec["stderr_table_path"] = _prepare_stderr_table(spec["stderr_table_path"], client=client)
        return spec

    def _set_tables(self, spec, input_tables, output_tables, single_output_table=False):
        if input_tables is not None:
            self._input_table_paths = input_tables
            spec["input_table_paths"] = list(imap(lambda table: table.to_yson_type(), self._input_table_paths))

        if output_tables is not None:
            output_tables_param = "output_table_path" if single_output_table else "output_table_paths"
            if output_tables_param in spec:
                self._output_table_paths = output_tables
                if single_output_table:
                    spec[output_tables_param] = self._output_table_paths[0].to_yson_type()
                else:
                    spec[output_tables_param] = list(imap(lambda table: table.to_yson_type(), self._output_table_paths))

    def _prepare_tables(self, spec, single_output_table=False, replace_unexisting_by_empty=True, create_on_cluster=False, client=None):
        require("input_table_paths" in spec,
                lambda: YtError("You should specify input_table_paths"))
        input_tables = _prepare_source_tables(
            spec["input_table_paths"],
            replace_unexisting_by_empty=replace_unexisting_by_empty,
            client=client)
        output_tables_param = "output_table_path" if single_output_table else "output_table_paths"
        output_tables = None
        if output_tables_param in spec:
            output_tables = _prepare_destination_tables(spec[output_tables_param], create_on_cluster=create_on_cluster, client=client)
        self._set_tables(spec, input_tables, output_tables, single_output_table=single_output_table)

    def _build_simple_user_job_spec(self, spec, job_type, job_io_type,
                                    requires_command=True, requires_format=True, group_by=None, client=None):
        spec, input_tables, output_tables = self._build_user_job_spec(
            spec,
            job_type=job_type,
            job_io_type=job_io_type,
            requires_command=requires_command,
            requires_format=requires_format,
            group_by=group_by,
            client=client,
        )
        self._set_tables(spec, input_tables, output_tables)
        return spec

    def _build_user_job_spec(self, spec, job_type, job_io_type,
                             requires_command=True, requires_format=True, group_by=None,
                             operation_preparation_context=None,
                             client=None):
        if operation_preparation_context is None:
            operation_preparation_context = SimpleOperationPreparationContext(
                self.get_input_table_paths(),
                self.get_output_table_paths(),
                client=client,
            )
        if isinstance(spec[job_type], UserJobSpecBuilder):
            job_spec_builder = spec[job_type]
            spec[job_type], input_tables, output_tables = job_spec_builder.build(
                operation_preparation_context=operation_preparation_context,
                group_by=group_by,
                local_files_to_remove=self._local_files_to_remove,
                uploaded_files=self._uploaded_files,
                operation_type=self.operation_type,
                requires_command=requires_command,
                requires_format=requires_format,
                job_io_spec=spec.get(job_io_type),
                has_input_query="input_query" in spec,
                client=client,
            )
            if spec[job_type] is None:
                del spec[job_type]
        else:
            input_tables = operation_preparation_context.get_input_paths()
            output_tables = operation_preparation_context.get_output_paths()
        return spec, input_tables, output_tables

    def _build_job_io(self, spec, job_io_type, client=None):
        table_writer = None
        if job_io_type in spec:
            if isinstance(spec.get(job_io_type), JobIOSpecBuilderBase):
                spec[job_io_type] = spec[job_io_type].build()
            table_writer = spec[job_io_type].get("table_writer")
        table_writer = _prepare_table_writer(table_writer, client=client)
        spec.setdefault(job_io_type, {})
        if table_writer is not None:
            spec[job_io_type]["table_writer"] = table_writer
        return spec

    def _apply_spec_patches(self, spec, spec_patches):
        for user_job_script_path in self._user_job_scripts:
            parent_spec_dict = spec
            parent_spec_patches_dict = spec_patches

            skip_patch = False
            for part in user_job_script_path[:-1]:
                # If part is not presented in spec, than regular update will apply this patch successfully later.
                if part not in parent_spec_patches_dict or part not in parent_spec_dict:
                    skip_patch = True
                    break
                parent_spec_patches_dict = parent_spec_patches_dict[part]
                parent_spec_dict = parent_spec_dict[part]

            user_job_script_name = user_job_script_path[-1]
            if skip_patch or user_job_script_name not in parent_spec_patches_dict or \
                    user_job_script_name not in parent_spec_dict:
                continue

            user_job_spec = None
            if isinstance(parent_spec_patches_dict[user_job_script_name], UserJobSpecBuilder):
                user_job_spec = parent_spec_patches_dict[user_job_script_name]._spec
            else:
                user_job_spec = parent_spec_patches_dict[user_job_script_name]

            if isinstance(parent_spec_dict[user_job_script_name], UserJobSpecBuilder):
                parent_spec_dict[user_job_script_name]._apply_spec_patch(user_job_spec)
            else:
                parent_spec_dict[user_job_script_name] = update(user_job_spec, spec[user_job_script_name])

        for job_io_type in self._job_io_types:
            if job_io_type in spec_patches:
                spec.setdefault(job_io_type, JobIOSpecBuilderBase())

                job_io_spec = None
                if isinstance(spec_patches[job_io_type], JobIOSpecBuilderBase):
                    job_io_spec = spec_patches[job_io_type]._spec
                else:
                    job_io_spec = spec_patches[job_io_type]

                if isinstance(spec[job_io_type], JobIOSpecBuilderBase):
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

    def _apply_environment_patch(self, task_spec, client=None):
        config = get_config(client)
        patch = dict(
            YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB=str(int(config["allow_http_requests_to_yt_from_job"]))
        )
        task_spec["environment"] = update(patch, task_spec.get("environment", {}))

    def _prepare_spec(self, spec, client=None):
        spec = self._prepare_stderr_table(spec, client=client)

        for job_io_type in self._job_io_types:
            spec = self._build_job_io(spec, job_io_type=job_io_type, client=client)

        command_length_limit = get_config(client)["started_by_command_length_limit"]
        started_by = get_started_by(command_length_limit=command_length_limit)
        spec = update({"started_by": started_by}, spec)
        if get_config(client)["pool"] is not None:
            spec = update({"pool": get_config(client)["pool"]}, spec)
        if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
            spec = update({"data_size_per_job": 4 * GB}, spec)

        self._prepared_spec = spec

    def get_input_table_paths(self):
        """Returns list of input paths."""
        return self._input_table_paths

    def get_output_table_paths(self):
        """Returns list of output paths."""
        return self._output_table_paths

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        pass

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)

        spec = self._apply_spec_defaults(self._prepared_spec, client=client)
        return spec

    def build(self, client=None):
        """Builds final spec."""
        spec = self._do_build(client)

        operation_title_parts = []
        for user_job_script_path in self._user_job_scripts:
            user_job_title_parts = []
            task_spec = spec
            skip_script = False
            for part in user_job_script_path:
                if part not in task_spec:
                    skip_script = True
                    break
                task_spec = task_spec[part]
                user_job_title_parts.append(part)
            if not skip_script:
                self._apply_environment_patch(task_spec, client)

                user_job_title = ""
                if "title" in task_spec and task_spec["title"]:
                    user_job_title = task_spec["title"]

                for title_part in reversed(user_job_title_parts):
                    if user_job_title:
                        user_job_title = "{}[{}]".format(underscore_case_to_camel_case(title_part), user_job_title)
                    else:
                        user_job_title = underscore_case_to_camel_case(title_part)

                if user_job_title:
                    operation_title_parts.append(user_job_title)

        # For vanilla operation we should also visit all tasks from spec, as
        # they may be absent in _user_job_scripts in case of raw spec.
        # But be careful not to process same user job spec twice!
        for task_key, task_spec in spec.get("tasks", {}).items():
            if ("tasks", task_key) not in self._user_job_scripts:
                self._apply_environment_patch(task_spec, client)

                if "title" in task_spec and task_spec["title"]:
                    operation_title_parts.append(task_spec["title"])

        if "title" not in spec and operation_title_parts:
            operation_title = ", ".join(operation_title_parts)
            spec = update(spec, {"title": operation_title})

        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return False

    def get_finalizer(self, spec, client=None):
        """Returns finalizer instance that should be called after finish of operation."""
        if self.supports_user_job_spec():
            return Finalizer(self._local_files_to_remove, self._output_table_paths, spec, client=client)
        return lambda state: None

    def get_toucher(self, client=None):
        """Returns toucher instance that should be periodically called before operation started."""
        if self.supports_user_job_spec():
            return Toucher(self._uploaded_files, client=client)
        return lambda: None


class ReduceSpecBuilder(SpecBuilder):
    """Builder for spec of reduce operation.
    """
    def __init__(self, spec=None):
        super(ReduceSpecBuilder, self).__init__(
            operation_type="reduce",
            user_job_scripts=[("reducer",)],
            job_io_types=["job_io"],
            spec=spec)

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        """Start building reducer section."""
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

    @spec_option("Standard key guarantee of reduce operation. Can be disabled to deal with skewed keys")
    def enable_key_guarantee(self, flag):
        return _set_spec_value(self, "enable_key_guarantee", flag)

    @spec_option("I/O settings of jobs", nested_spec_builder=JobIOSpecBuilder)
    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        if spec.get("sort_by") is None:
            if spec.get("reduce_by") is not None:
                spec["sort_by"] = spec.get("reduce_by")
            elif spec.get("join_by") is not None:
                spec["sort_by"] = spec.get("join_by")

        reduce_by = _prepare_reduce_by(spec.get("reduce_by"), client, required=False)
        if reduce_by is not None:
            spec["reduce_by"] = reduce_by

        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client)

        if spec.get("join_by") is not None:
            spec["join_by"] = _prepare_join_by(spec.get("join_by"), required=False)

        self._prepare_spec(spec, client=client)

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec
        group_by = spec.get("reduce_by")
        if spec.get("join_by") is not None:
            group_by = spec.get("join_by")

        if "reducer" in spec:
            spec = self._build_simple_user_job_spec(
                spec,
                job_type="reducer",
                job_io_type="job_io",
                group_by=group_by,
                client=client,
            )
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return True


class JoinReduceSpecBuilder(SpecBuilder):
    """Builder for spec of join_reduce operation.
    """
    def __init__(self):
        super(JoinReduceSpecBuilder, self).__init__(
            operation_type="join_reduce",
            user_job_scripts=[("reducer",)],
            job_io_types=["job_io"])

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        """Start building reducer section."""
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
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        spec["join_by"] = _prepare_join_by(spec.get("join_by"))
        self._prepare_spec(spec, client=client)

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "reducer" in spec:
            spec = self._build_simple_user_job_spec(spec,
                                                    job_type="reducer",
                                                    job_io_type="job_io",
                                                    group_by=spec.get("join_by"),
                                                    client=client)
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return True


class MapSpecBuilder(SpecBuilder):
    """Builder for spec of map operation.
    """
    def __init__(self):
        super(MapSpecBuilder, self).__init__(
            operation_type="map",
            user_job_scripts=[("mapper",)],
            job_io_types=["job_io"])

    @spec_option("The description of mapper script", nested_spec_builder=MapperSpecBuilder)
    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        """Start building mapper section."""
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
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        self._prepare_spec(spec, client=client)

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "mapper" in spec:
            spec = self._build_simple_user_job_spec(
                spec=spec,
                job_type="mapper",
                job_io_type="job_io",
                client=client,
            )
        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return True


class MapReduceSpecBuilder(SpecBuilder):
    """Builder for spec of map_reduce operation.
    """
    def __init__(self):
        super(MapReduceSpecBuilder, self).__init__(
            operation_type="map_reduce",
            user_job_scripts=[("mapper",), ("reducer",), ("reduce_combiner",)],
            job_io_types=["map_job_io", "sort_job_io", "reduce_job_io"]
        )

    @spec_option("The description of mapper script", nested_spec_builder=MapperSpecBuilder)
    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        """Start building mapper section."""
        return MapperSpecBuilder(self)

    @spec_option("The description of reducer script", nested_spec_builder=ReducerSpecBuilder)
    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        """Start building reducer section."""
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

    def begin_intermediate_data_acl(self):
        """Start intermediate data ACL builder."""
        return IntermediateDataAclBuilder(self)

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
        self._spec["map_job_io"] = deepcopy(job_io_spec)
        return self

    def begin_map_job_io(self):
        """Start building map_job_io section."""
        return MapJobIOSpecBuilder(self)

    @spec_option("I/O settings of sort jobs", nested_spec_builder=SortJobIOSpecBuilder)
    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = deepcopy(job_io_spec)
        return self

    def begin_sort_job_io(self):
        """Start building sort_job_io section."""
        return SortJobIOSpecBuilder(self)

    @spec_option("I/O settings of reduce jobs", nested_spec_builder=ReduceJobIOSpecBuilder)
    def reduce_job_io(self, job_io_spec):
        self._spec["reduce_job_io"] = deepcopy(job_io_spec)
        return self

    def begin_reduce_job_io(self):
        """Start building reduce_job_io section."""
        return ReduceJobIOSpecBuilder(self)

    @spec_option("The description of reduce_combiner script", nested_spec_builder=ReduceCombinerSpecBuilder)
    def reduce_combiner(self, reduce_combiner_script):
        self._spec["reduce_combiner"] = reduce_combiner_script
        return self

    def begin_reduce_combiner(self):
        """Start building reduce_combiner section."""
        return ReduceCombinerSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)

        if spec.get("sort_by") is None:
            spec["sort_by"] = spec.get("reduce_by")
        spec["reduce_by"] = _prepare_reduce_by(spec.get("reduce_by"), client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client)

        self._prepare_spec(spec, client=client)

    def _do_build_mapper(self, spec, client):
        if isinstance(spec["mapper"], UserJobSpecBuilder):
            mapper_spec = spec["mapper"]._spec
        else:
            mapper_spec = spec["mapper"]

        command = mapper_spec.get("command")
        output_streams = mapper_spec.get("output_streams")

        intermediate_stream_count = 1
        if isinstance(command, TypedJob):
            if command.get_intermediate_stream_count() is not None:
                intermediate_stream_count = command.get_intermediate_stream_count()

            if output_streams is not None:
                raise YtError("Output streams cannot be specified explicitly for a typed map job, use get_intermediate_stream_count and prepare_operation methods instead")

        elif output_streams is not None:
            intermediate_stream_count = len(output_streams)

        additional_mapper_output_table_count = spec.get("mapper_output_table_count", 0)
        mapper_output_tables = [None] * intermediate_stream_count + \
            self.get_output_table_paths()[:additional_mapper_output_table_count]

        operation_preparation_context = SimpleOperationPreparationContext(
            self.get_input_table_paths(),
            mapper_output_tables,
            client=client,
        )

        spec, input_tables, mapper_output_tables = self._build_user_job_spec(
            spec,
            job_type="mapper",
            job_io_type="map_job_io",
            requires_command=False,
            operation_preparation_context=operation_preparation_context,
            client=client,
        )

        output_tables = self.get_output_table_paths()
        output_tables[:additional_mapper_output_table_count] = \
            mapper_output_tables[intermediate_stream_count:]
        self._set_tables(spec, input_tables, output_tables)

        # TODO(egor-gutrov): fill intermediate_stream_schemas for CppJob
        intermediate_streams = []
        if isinstance(command, TypedJob):
            for intermediate_table_path in mapper_output_tables[:intermediate_stream_count]:
                schema = intermediate_table_path.attributes.get("schema")
                if schema is None:
                    schema = TableSchema()
                else:
                    schema = schema.build_schema_sorted_by(spec["sort_by"])
                intermediate_streams.append({
                    "schema": schema,
                })

            # NB: spec["mapper"]["output_streams"] must be empty here due to validation above.
            assert "output_streams" not in spec["mapper"], "output streams must be empty for a typed job"
            spec["mapper"]["output_streams"] = intermediate_streams
        else:
            intermediate_streams = spec["mapper"].get("output_streams", [])

        if intermediate_streams:
            intermediate_stream_schemas = [stream["schema"] for stream in intermediate_streams]
        else:
            intermediate_stream_schemas = [None]

        return spec, intermediate_stream_schemas

    def _do_build_reduce_combiner(self, spec, is_first_task, intermediate_stream_schemas, client):
        if is_first_task:
            intermediate_stream_count = len(self.get_input_table_paths())
            operation_preparation_context = SimpleOperationPreparationContext(
                self.get_input_table_paths(),
                [None] * intermediate_stream_count,
                client=client,
            )
            intermediate_stream_schemas = operation_preparation_context.get_input_schemas()
        else:
            intermediate_stream_count = len(intermediate_stream_schemas)
            operation_preparation_context = IntermediateOperationPreparationContext(
                intermediate_stream_schemas,
                [None] * intermediate_stream_count,
                client=client,
            )
        spec, input_tables, _ = self._build_user_job_spec(
            spec,
            job_type="reduce_combiner",
            job_io_type="sort_job_io",
            group_by=spec.get("reduce_by"),
            requires_command=False,
            operation_preparation_context=operation_preparation_context,
            client=client,
        )
        if is_first_task:
            self._set_tables(spec, input_tables, output_tables=None)
        return spec, intermediate_stream_schemas

    def _do_build_reducer(self, spec, is_first_task, intermediate_stream_schemas, client):
        additional_mapper_output_table_count = spec.get("mapper_output_table_count", 0)
        reducer_output_tables = self.get_output_table_paths()[additional_mapper_output_table_count:]
        if is_first_task:
            operation_preparation_context = SimpleOperationPreparationContext(
                self.get_input_table_paths(),
                reducer_output_tables,
                client=client,
            )
        else:
            operation_preparation_context = IntermediateOperationPreparationContext(
                intermediate_stream_schemas,
                reducer_output_tables,
                client=client,
            )
        spec, input_tables, reducer_output_tables = self._build_user_job_spec(
            spec,
            job_type="reducer",
            job_io_type="reduce_job_io",
            group_by=spec.get("reduce_by"),
            operation_preparation_context=operation_preparation_context,
            client=client,
        )
        if not is_first_task:
            input_tables = None
        output_tables = self.get_output_table_paths()
        output_tables[additional_mapper_output_table_count:] = reducer_output_tables
        self._set_tables(spec, input_tables, output_tables)
        return spec

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        is_first_task = True
        intermediate_stream_schemas = None

        if "mapper" in spec:
            spec, intermediate_stream_schemas = self._do_build_mapper(
                spec,
                client=client)
            is_first_task = False
        else:
            map_job_io_control_attributes = spec.setdefault("map_job_io", {}).setdefault("control_attributes", {})
            map_job_io_control_attributes["enable_table_index"] = True

        if "reduce_combiner" in spec:
            spec, intermediate_stream_schemas = self._do_build_reduce_combiner(
                spec,
                is_first_task=is_first_task,
                intermediate_stream_schemas=intermediate_stream_schemas,
                client=client,
            )
            is_first_task = False

        if "reducer" in spec:
            spec = self._do_build_reducer(
                spec,
                is_first_task=is_first_task,
                intermediate_stream_schemas=intermediate_stream_schemas,
                client=client,
            )

        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return True


class MergeSpecBuilder(SpecBuilder):
    """Builder for spec of merge operation.
    """
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
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
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
    """Builder for spec of sort operation.
    """
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
    def output_table_path(self, path):
        return _set_spec_value(self, "output_table_path", path)

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
        """Start building partition_job_io section."""
        return PartitionJobIOSpecBuilder(self)

    @spec_option("I/O settings of partition jobs", nested_spec_builder=PartitionJobIOSpecBuilder)
    def partition_job_io(self, job_io_spec):
        self._spec["partition_job_io"] = job_io_spec
        return self

    def begin_sort_job_io(self):
        """Start building sort_job_io section."""
        return SortJobIOSpecBuilder(self)

    @spec_option("I/O settings of sort jobs", nested_spec_builder=SortJobIOSpecBuilder)
    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = job_io_spec
        return self

    def begin_merge_job_io(self):
        """Start building merge_job_io section."""
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
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        require("input_table_paths" in spec, lambda: YtError("You should specify \"input_table_paths\""))

        spec = self._prepare_tables_to_sort(spec, client=client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client=client)
        self._prepare_spec(spec, client=client)


class RemoteCopySpecBuilder(SpecBuilder):
    """Builder for spec of remote_copy operation.
    """
    def __init__(self):
        super(RemoteCopySpecBuilder, self).__init__(operation_type="remote_copy", job_io_types=["job_io"])
        self._create_destination_on_cluster = True

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
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def create_destination_on_cluster(self, create_on_cluster):
        """Forces creation of destination object on cluster. Attributes are passed via YRichPath"""
        self._create_destination_on_cluster = create_on_cluster
        return self

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(
            spec,
            single_output_table=True,
            create_on_cluster=self._create_destination_on_cluster,
            client=client
        )
        self._prepare_spec(spec, client=client)


class EraseSpecBuilder(SpecBuilder):
    """Builder for spec of erase operation.
    """
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
        """Start building job_io section."""
        return JobIOSpecBuilder(self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)
        require("table_path" in spec, lambda: YtError("You should specify table_path"))

        table_path = _prepare_source_tables(spec["table_path"], client=client)
        spec["table_path"] = unlist(list(imap(lambda table: table.to_yson_type(), table_path)))

        self._input_table_paths = [spec["table_path"]]
        self._prepare_spec(spec, client=client)


class VanillaSpecBuilder(SpecBuilder):
    """Builder for spec of vanilla operation.
    """
    def __init__(self):
        super(VanillaSpecBuilder, self).__init__(operation_type="vanilla")
        self._spec["tasks"] = {}

    @spec_option("The description of multiple tasks", nested_spec_builder=TaskSpecBuilder)
    def tasks(self, tasks):
        for name, task in iteritems(tasks):
            self.task(name, task)
        return self

    def task(self, name, task):
        """The description of task."""
        self._user_job_scripts.append(("tasks", name))
        self._spec["tasks"][name] = task
        return self

    def begin_task(self, name):
        """Start building task section."""
        self._user_job_scripts.append(("tasks", name))
        return TaskSpecBuilder(name, self)

    def prepare(self, client=None):
        """Prepare spec to be used in operation."""
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_spec(spec, client=client)

    def _do_build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        for task_path in self._user_job_scripts:
            assert len(task_path) == 2
            assert task_path[0] == "tasks"
            task = task_path[-1]
            spec["tasks"], _, _ = self._build_user_job_spec(
                spec=spec["tasks"],
                job_type=task,
                job_io_type=None,
                client=client,
                requires_format=False,
            )

        spec = self._apply_spec_defaults(spec, client=client)
        return spec

    def supports_user_job_spec(self):
        """Whether operation has some user job sections."""
        return True
