from .batch_helpers import batch_apply
from .batch_response import apply_function_to_result
from .config import get_config
from .common import (flatten, imap, round_up_to, iteritems, GB, MB,
                     get_value, unlist, get_started_by, bool_to_string,
                     parse_bool, is_prefix, require, YtError, update)
from .cypress_commands import exists, get, remove_with_empty_dirs, get_attribute
from .errors import YtOperationFailedError
from .ypath import TablePath
from .table_commands import is_empty, is_sorted
from .table_helpers import (FileUploader, _prepare_formats, _is_python_function,
                            _prepare_binary, _prepare_source_tables, _prepare_destination_tables,
                            _prepare_table_writer, _prepare_stderr_table)

import yt.logger as logger

from yt.packages.six.moves import zip as izip

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
        if  chunk_count < get_config(self.client)["auto_merge_output"]["min_chunk_count"]:
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
                spec = {"combine_chunks": bool_to_string(True), "data_size_per_job": data_size_per_job}
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
                        "--spec '{{"
                           "combine_chunks=true;"
                           "data_size_per_job={2}"
                        "}}'".format(table, mode, data_size_per_job, get_config(self.client)["proxy"]["url"]))

class JobIOSpecBuilder(object):
    def __init__(self, user_job_spec_builder=None, io_name=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self._spec_override = {}
        self.user_job_spec_builder = user_job_spec_builder
        self.io_name = io_name

    def table_reader(self, reader):
        return _set_spec_value(self, "table_reader", reader)

    def table_writer(self, writer):
        return _set_spec_value(self, "table_writer", writer)

    def error_file_writer(self, writer):
        return _set_spec_value(self, "error_file_writer", writer)

    def control_attributes(self, attributes):
        return _set_spec_value(self, "control_attributes", attributes)

    def buffer_row_count(self, count):
        return _set_spec_value(self, "buffer_row_count", count)

    def pipe_io_pool_size(self, size):
        return _set_spec_value(self, "pipe_io_pool_size", size)

    def _end_job_io(self):
        assert self.user_job_spec_builder is not None
        user_job_spec_builder = self.user_job_spec_builder
        self.user_job_spec_builder = None
        builder_func = getattr(user_job_spec_builder, self.io_name)
        builder_func(self)
        return user_job_spec_builder

    def _set_spec_override(self, spec):
        self._spec_override = spec

    def build(self):
        spec = update(self._spec_override, deepcopy(self._spec))
        self._spec_override = {}
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
    def __init__(self, spec_builder=None, job_name=None, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec
        self._spec_override = {}

        self._spec_builder = spec_builder
        self._job_name = job_name

    def command(self, binary_or_python_obj):
        return _set_spec_value(self, "command", binary_or_python_obj)

    def file_paths(self, paths):
        return _set_spec_value(self, "file_paths", paths)

    def local_files(self, files):
        return _set_spec_value(self, "local_files", files)

    def format(self, format):
        return _set_spec_value(self, "format", format)

    def input_format(self, format):
        return _set_spec_value(self, "input_format", format)

    def output_format(self, format):
        return _set_spec_value(self, "output_format", format)

    def environment(self, environment_dict):
        return _set_spec_value(self, "environment", environment_dict)

    def cpu_limit(self, limit):
        return _set_spec_value(self, "cpu_limit", limit)

    def memory_limit(self, limit):
        return _set_spec_value(self, "memory_limit", limit)

    def memory_reserve_factor(self, factor):
        return _set_spec_value(self, "memory_reserve_factor", factor)

    def tmpfs_path(self, path):
        return _set_spec_value(self, "tmpfs_path", path)

    def tmpfs_size(self, size):
        return _set_spec_value(self, "tmpfs_size", size)

    def max_stderr_size(self, size):
        return _set_spec_value(self, "max_stderr_size", size)

    def custom_statistics_count_limit(self, limit):
        return _set_spec_value(self, "custom_statistics_count_limit", limit)

    def job_time_limit(self, limit):
        return _set_spec_value(self, "job_time_limit", limit)

    def enable_input_table_index(self, flag=True):
        return _set_spec_value(self, "enable_input_table_index", flag)

    def copy_files(self, flag=True):
        return _set_spec_value(self, "copy_files", flag)

    def use_yamr_descriptors(self, flag=True):
        return _set_spec_value(self, "use_yamr_descriptors", flag)

    def check_input_fully_consumed(self, flag=True):
        return _set_spec_value(self, "check_input_fully_consumed", flag)

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
        # Fix python2.6 bug http://bugs.python.org/issue1515
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

    def _prepare_job_files(self, spec, group_by, local_files_to_remove, input_format, output_format, client):
        file_uploader = FileUploader(client=client)
        files = file_uploader(spec.get("local_files"))

        prepare_result = _prepare_binary(spec["command"], self._job_name, input_format, output_format,
                                         group_by, file_uploader, client=client)

        tmpfs_size = prepare_result.tmpfs_size
        environment = prepare_result.environment
        binary = prepare_result.cmd
        title = prepare_result.title

        if local_files_to_remove is not None:
            local_files_to_remove += prepare_result.local_files_to_remove

        spec["command"] = binary

        if environment:
            if "environment" not in spec:
                spec["environment"] = {}
            for key, value in iteritems(environment):
                spec["environment"][key] = value

        if title:
            spec["title"] = title

        file_paths = []
        file_paths += flatten(files)
        file_paths += flatten(prepare_result.files)
        if "file_paths" in spec:
            file_paths += list(imap(lambda path: TablePath(path, client=client), spec["file_paths"]))
        if file_paths:
            spec["file_paths"] = file_paths

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
            ld_library_path = spec.get("environment", {}).get("LD_LIBRARY_PATH")
            paths = ["./modules/_shared", "./tmpfs/modules/_shared"]
            if ld_library_path is not None:
                paths.insert(0, ld_library_path)
            if "environment" not in spec:
                spec["environment"] = {}
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
                    attributes = get(file + "/@")
                    if attributes["type"] == "table":
                        raise YtError(
                            'Attributes "disk_size" must be specified for table file "{0}"'.format(str(file)))
                    file_disk_size = attributes["uncompressed_data_size"]
                disk_size += round_up_to(file_disk_size, 4 * 1024)
            tmpfs_size += disk_size
            tmpfs_size += additional_tmpfs_size
            if "tmpfs_size" not in spec:
                spec["tmpfs_size"] = tmpfs_size
                spec["copy_files"] = True
                spec["tmpfs_path"] = "."
        else:
            if spec.get("tmpfs_size", 0) > 0 and "tmpfs_path" not in spec:
                spec["tmpfs_path"] = "tmpfs"
        return spec

    def _set_spec_override(self, spec):
        self._spec_override = spec

    def build(self, local_files_to_remove=None, group_by=None, client=None):
        require(self._spec_builder is None, lambda: YtError("The job spec builder is incomplete"))
        require(self._spec.get("command") is not None, lambda: YtError("You should specify job command"))
        spec = update(self._spec_override, self._deepcopy_spec())
        self._spec_override = {}

        format_ = spec.pop("format", None)
        input_format, output_format = _prepare_formats(format_, spec.get("input_format"), spec.get("output_format"),
                                                       spec["command"], client)
        spec["input_format"] = input_format.to_yson_type()
        spec["output_format"] = output_format.to_yson_type()

        spec = self._prepare_ld_library_path(spec, client)
        spec, tmpfs_size, disk_size = self._prepare_job_files(spec, group_by, local_files_to_remove,
                                                              input_format, output_format, client)
        spec.setdefault("use_yamr_descriptors",
                        bool_to_string(get_config(client)["yamr_mode"]["use_yamr_style_destination_fds"]))
        spec.setdefault("check_input_fully_consumed",
                        bool_to_string(get_config(client)["yamr_mode"]["check_input_fully_consumed"]))
        spec = self._prepare_tmpfs(spec, tmpfs_size, disk_size, client)
        spec = self._prepare_memory_limit(spec, client)
        return spec

class MapperSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(MapperSpecBuilder, self).__init__(spec_builder, "mapper")

    def end_mapper(self):
        return self._end_script()

class ReducerSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(ReducerSpecBuilder, self).__init__(spec_builder, "reducer")

    def end_reducer(self):
        return self._end_script()

class ReduceCombinerSpecBuilder(UserJobSpecBuilder):
    def __init__(self, spec_builder=None):
        super(ReduceCombinerSpecBuilder, self).__init__(spec_builder, "reduce_combiner")

    def end_reduce_combiner(self):
        return self._end_script()

class SpecBuilder(object):
    def __init__(self, spec=None):
        self._spec = {}
        if spec:
            self._spec = spec

        self._local_files_to_remove = []
        self._input_table_paths = []
        self._output_table_paths = []

        self.operation_type = None
        self._finalize = None
        self._user_spec = {}

        self._prepared_spec = None

    def pool(self, pool_name):
        return _set_spec_value(self, "pool", pool_name)

    def weight(self, weight):
        return _set_spec_value(self, "weight", weight)

    def time_limit(self, limit):
        return _set_spec_value(self, "time_limit", limit)

    def max_stderr_count(self, count):
        return _set_spec_value(self, "max_stderr_count", count)

    def max_failed_job_count(self, count):
        return _set_spec_value(self, "max_failed_job_count", count)

    def unavailable_chunk_strategy(self, strategy):
        return _set_spec_value(self, "unavailable_chunk_strategy", strategy)

    def unavailable_chunk_tactics(self, tactics):
        return _set_spec_value(self, "unavailable_chunk_tactics", tactics)

    def scheduling_tag(self, tag):
        return _set_spec_value(self, "scheduling_tag", tag)

    def max_data_size_per_job(self, size):
        return _set_spec_value(self, "max_data_size_per_job", size)

    def secure_vault(self, secure_vault_dict):
        return _set_spec_value(self, "secure_vault", secure_vault_dict)

    def stderr_table_path(self, path):
        return _set_spec_value(self, "stderr_table_path", path)

    def spec(self, spec):
        self._user_spec = spec
        return self

    def job_io(self, job_io_spec):
        self._spec["job_io"] = deepcopy(job_io_spec)
        return self

    def begin_job_io(self):
        return JobIOSpecBuilder(self, "job_io")

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
        input_table_paths = _prepare_source_tables(spec["input_table_paths"],
                                                   replace_unexisting_by_empty=replace_unexisting_by_empty,
                                                   client=client)
        spec["input_table_paths"] = list(imap(lambda table: table.to_yson_type(), input_table_paths))

        output_tables_param = "output_table_path" if single_output_table else "output_table_paths"
        require(output_tables_param in spec,
                lambda: YtError("You should specify {0}".format(output_tables_param)))
        output_table_path = _prepare_destination_tables(spec[output_tables_param], client=client)
        spec[output_tables_param] = list(imap(lambda table: table.to_yson_type(), output_table_path))
        if single_output_table:
            spec[output_tables_param] = unlist(spec[output_tables_param])

    def _build_user_job_spec(self, spec, job_name, group_by=None, client=None):
        if isinstance(spec[job_name], UserJobSpecBuilder):
            job_spec_builder = spec[job_name]
            spec[job_name] = job_spec_builder.build(group_by=group_by,
                                                    local_files_to_remove=self._local_files_to_remove,
                                                    client=client)
        return spec

    def _build_job_io(self, spec, job_io_type="job_io", client=None):
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
        for user_job_script in ["mapper", "reducer", "reduce_combiner"]:
            if user_job_script in spec_patches:
                spec.setdefault(user_job_script, UserJobSpecBuilder(job_name=user_job_script))

                user_job_spec = None
                if isinstance(spec_patches[user_job_script], UserJobSpecBuilder):
                    user_job_spec = spec_patches[user_job_script]._spec
                else:
                    user_job_spec = spec_patches[user_job_script]

                if isinstance(spec[user_job_script], UserJobSpecBuilder):
                    spec[user_job_script]._set_spec_override(user_job_spec)
                else:
                    spec[user_job_script] = update(user_job_spec, spec[user_job_script])

        for job_io_type in ["job_io", "partition_job_io", "sort_job_io",
                            "merge_job_io", "map_job_io", "reduce_job_io"]:
            if job_io_type in spec_patches:
                spec.setdefault(job_io_type, JobIOSpecBuilder())

                job_io_spec = None
                if isinstance(spec_patches[job_io_type], JobIOSpecBuilder):
                    job_io_spec = spec_patches[job_io_type]._spec
                else:
                    job_io_spec = spec_patches[job_io_type]

                if isinstance(spec[job_io_type], JobIOSpecBuilder):
                    spec[job_io_type]._set_spec_override(job_io_spec)
                else:
                    spec[job_io_type] = update(job_io_spec, spec[job_io_type])

        return update(spec_patches, spec)

    def _apply_spec_overrides(self, spec, client=None):
        return self._apply_spec_patches(spec, deepcopy(get_config(client)["spec_overrides"]))

    def _apply_user_spec(self, spec):
        return self._apply_spec_patches(spec, get_value(self._user_spec, {}))

    def _prepare_spec(self, spec, client=None):
        spec = self._prepare_stderr_table(spec, client=client)
        spec = self._build_job_io(spec, client=client)

        started_by = get_started_by()
        spec = update(deepcopy(get_config(client)["spec_defaults"]), spec)
        spec = update({"started_by": started_by}, spec)
        if get_config(client)["pool"] is not None:
            spec = update({"pool": get_config(client)["pool"]}, spec)
        if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
            spec = update({"data_size_per_job": 4 * GB}, spec)

        if "input_table_path" in spec:
            self._input_table_paths = [spec["input_table_path"]]
        elif "input_table_paths" in spec:
            self._input_table_paths = spec["input_table_paths"]

        if "output_table_path" in spec:
            self._output_table_paths = [spec["output_table_path"]]
        elif "output_table_paths" in spec:
            self._output_table_paths = spec["output_table_paths"]

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

        return self._prepared_spec

    def supports_user_job_spec(self):
        return False

    def get_finalizer(self, spec, client=None):
        if self.supports_user_job_spec():
            return Finalizer(self._local_files_to_remove, self._output_table_paths, spec, client=client)
        return lambda state: None

class ReduceSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(ReduceSpecBuilder, self).__init__(spec=spec)
        self.operation_type = "reduce"

    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    def reduce_by(self, columns):
        return _set_spec_value(self, "reduce_by", columns)

    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    def join_by(self, columns):
        return _set_spec_value(self, "join_by", columns)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

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
            spec = self._build_user_job_spec(spec, job_name="reducer",
                                             group_by=group_by,
                                             client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class JoinReduceSpecBuilder(SpecBuilder):
    def __init__(self):
        super(JoinReduceSpecBuilder, self).__init__()
        self.operation_type = "join_reduce"

    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    def join_by(self, columns):
        return _set_spec_value(self, "join_by", columns)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

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
            spec = self._build_user_job_spec(spec, job_name="reducer",
                                             group_by=spec.get("join_by"),
                                             client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MapSpecBuilder(SpecBuilder):
    def __init__(self):
        super(MapSpecBuilder, self).__init__()
        self.operation_type = "map"

    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        return MapperSpecBuilder(self)

    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    def ordered(self, flag=True):
        return _set_spec_value(self, "ordered", flag)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, client=client)
        if "ordered" in spec:
            spec["ordered"] = bool_to_string(spec["ordered"])

        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "mapper" in spec:
            spec = self._build_user_job_spec(spec=spec, job_name="mapper", client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MapReduceSpecBuilder(SpecBuilder):
    def __init__(self):
        super(MapReduceSpecBuilder, self).__init__()
        self.operation_type = "map_reduce"

    def mapper(self, mapper_script):
        self._spec["mapper"] = mapper_script
        return self

    def begin_mapper(self):
        return MapperSpecBuilder(self)

    def reducer(self, reducer_script):
        self._spec["reducer"] = reducer_script
        return self

    def begin_reducer(self):
        return ReducerSpecBuilder(self)

    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    def reduce_by(self, columns):
        return _set_spec_value(self, "reduce_by", columns)

    def partition_count(self, count):
        return _set_spec_value(self, "partition_count", count)

    def partition_data_size(self, size):
        return _set_spec_value(self, "partition_data_size", size)

    def map_job_count(self, count):
        return _set_spec_value(self, "map_job_count", count)

    def data_size_per_map_job(self, size):
        return _set_spec_value(self, "data_size_per_map_job", size)

    def map_selectivity_factor(self, factor):
        return _set_spec_value(self, "map_selectivity_factor", factor)

    def intermediate_data_account(self, account):
        return _set_spec_value(self, "intermediate_data_account", account)

    def intermediate_compression_codec(self, codec):
        return _set_spec_value(self, "intermediate_compression_codec", codec)

    def intermediate_data_acl(self, acl):
        return _set_spec_value(self, "intermediate_data_acl", acl)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_paths(self, paths):
        return _set_spec_value(self, "output_table_paths", paths)

    def map_job_io(self, job_io_spec):
        self._spec["map_job_io"] = job_io_spec
        return self

    def begin_map_job_io(self):
        return MapJobIOSpecBuilder(self)

    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = job_io_spec
        return self

    def begin_sort_job_io(self):
        return SortJobIOSpecBuilder(self)

    def reduce_job_io(self, job_io_spec):
        self._spec["reduce_job_io"] = job_io_spec
        return self

    def begin_reduce_job_io(self):
        return ReduceJobIOSpecBuilder(self)

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
        spec = self._build_job_io(spec, job_io_type="map_job_io", client=client)
        spec = self._build_job_io(spec, job_io_type="sort_job_io", client=client)
        spec = self._build_job_io(spec, job_io_type="reduce_job_io", client=client)

        if spec.get("sort_by") is None:
            spec["sort_by"] = spec.get("reduce_by")
        spec["reduce_by"] = _prepare_reduce_by(spec.get("reduce_by"), client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client)

        self._prepare_spec(spec, client=client)

    def build(self, client=None):
        if self._prepared_spec is None:
            self.prepare(client)
        spec = self._prepared_spec

        if "mapper" in spec:
            spec = self._build_user_job_spec(spec, job_name="mapper", client=client)
        if "reducer" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_name="reducer",
                                             group_by=spec.get("reduce_by"),
                                             client=client)
        if "reduce_combiner" in spec:
            spec = self._build_user_job_spec(spec,
                                             job_name="reduce_combiner",
                                             group_by=spec.get("reduce_by"),
                                             client=client)
        return spec

    def supports_user_job_spec(self):
        return True

class MergeSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(MergeSpecBuilder, self).__init__(spec=spec)
        self.operation_type = "merge"

    def mode(self, mode):
        return _set_spec_value(self, "mode", mode)

    def merge_by(self, columns):
        return _set_spec_value(self, "merge_by", columns)

    def job_count(self, count):
        return _set_spec_value(self, "job_count", count)

    def data_size_per_job(self, size):
        return _set_spec_value(self, "data_size_per_job", size)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_path(self, path):
        return _set_spec_value(self, "output_table_path", path)

    def combine_chunks(self, flag=True):
        return _set_spec_value(self, "combine_chunks", flag)

    def force_transform(self, flag=True):
        return _set_spec_value(self, "force_transform", flag)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, single_output_table=True, replace_unexisting_by_empty=False, client=client)
        mode = get_value(spec.get("mode"), "auto")
        if mode == "auto":
            mode = "sorted" if all(batch_apply(_is_tables_sorted, spec.get("input_table_paths"),
                                               client=client)) else "ordered"
        spec["mode"] = mode
        self._prepare_spec(spec, client=client)

class SortSpecBuilder(SpecBuilder):
    def __init__(self, spec=None):
        super(SortSpecBuilder, self).__init__(spec=spec)
        self.operation_type = "sort"

    def sort_by(self, columns):
        return _set_spec_value(self, "sort_by", columns)

    def partition_count(self, count):
        return _set_spec_value(self, "partition_count", count)

    def partition_data_size(self, size):
        return _set_spec_value(self, "partition_data_size", size)

    def data_size_per_partition_job(self, size):
        return _set_spec_value(self, "data_size_per_partition_job", size)

    def intermediate_data_account(self, account):
        return _set_spec_value(self, "intermediate_data_account", account)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_path(self, size):
        return _set_spec_value(self, "output_table_path", size)

    def partition_job_count(self, count):
        return _set_spec_value(self, "partition_job_count", count)

    def begin_partition_job_io(self):
        return PartitionJobIOSpecBuilder(self)

    def partition_job_io(self, job_io_spec):
        self._spec["partition_job_io"] = job_io_spec
        return self

    def begin_sort_job_io(self):
        return SortJobIOSpecBuilder(self)

    def sort_job_io(self, job_io_spec):
        self._spec["sort_job_io"] = job_io_spec
        return self

    def begin_merge_job_io(self):
        return MergeJobIOSpecBuilder(self)

    def merge_job_io(self, job_io_spec):
        self._spec["merge_job_io"] = job_io_spec
        return self

    def _prepare_tables_to_sort(self, spec, client=None):
        input_table_paths = _prepare_source_tables(spec["input_table_paths"],
                                                   replace_unexisting_by_empty=False,
                                                   client=client)

        exists_results = batch_apply(exists, input_table_paths, client=client)
        for table, exists_result in izip(input_table_paths, exists_results):
            require(exists_result, lambda: YtError("Table %s should exist" % table))

        spec["input_table_paths"] = list(imap(lambda table: table.to_yson_type(), input_table_paths))
        if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not input_table_paths:
            return spec

        if "output_table_path" not in spec:
            require(len(input_table_paths) == 1 and not input_table_paths[0].has_delimiters(),
                    lambda: YtError("You must specify destination sort table in case of multiple source tables"))
            spec["output_table_path"] = input_table_paths[0]

        output_table_path = _prepare_destination_tables(spec["output_table_path"], client=client)
        spec["output_table_path"] = unlist(output_table_path)
        return spec

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        require("input_table_paths" in spec, lambda: YtError("You should specify input_table_paths"))

        spec = self._build_job_io(spec, job_io_type="partition_job_io", client=client)
        spec = self._build_job_io(spec, job_io_type="sort_job_io", client=client)
        spec = self._build_job_io(spec, job_io_type="merge_job_io", client=client)

        spec = self._prepare_tables_to_sort(spec, client=client)
        spec["sort_by"] = _prepare_sort_by(spec.get("sort_by"), client=client)
        self._prepare_spec(spec, client=client)

class RemoteCopySpecBuilder(SpecBuilder):
    def __init__(self):
        super(RemoteCopySpecBuilder, self).__init__()
        self.operation_type = "remote_copy"

    def cluster_name(self, name):
        return _set_spec_value(self, "cluster_name", name)

    def network_name(self, name):
        return _set_spec_value(self, "network_name", name)

    def cluster_connection(self, cluster):
        return _set_spec_value(self, "cluster_connection", cluster)

    def input_table_paths(self, paths):
        return _set_spec_value(self, "input_table_paths", paths)

    def output_table_path(self, path):
        return _set_spec_value(self, "output_table_path", path)

    def copy_attributes(self, flag=True):
        return _set_spec_value(self, "copy_attributes", flag)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)

        self._prepare_tables(spec, single_output_table=True, client=client)
        self._prepare_spec(spec, client=client)

class EraseSpecBuilder(SpecBuilder):
    def __init__(self):
        super(EraseSpecBuilder, self).__init__()
        self.operation_type = "erase"

    def table_path(self, paths):
        return _set_spec_value(self, "table_path", paths)

    def combine_chunks(self, flag=True):
        return _set_spec_value(self, "combine_chunks", flag)

    def prepare(self, client=None):
        spec = deepcopy(self._spec)
        spec = self._apply_spec_overrides(spec, client=client)
        spec = self._apply_user_spec(spec)
        require("table_path" in spec, lambda: YtError("You should specify table_path"))

        table_path = _prepare_source_tables(spec["table_path"], client=client)
        spec["table_path"] = unlist(list(imap(lambda table: table.to_yson_type(), table_path)))

        self._input_table_paths = [spec["table_path"]]
        self._prepare_spec(spec, client=client)
