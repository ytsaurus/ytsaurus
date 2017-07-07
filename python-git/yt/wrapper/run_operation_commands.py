"""
Commands for table working and Map-Reduce operations.

.. seealso:: `operations on wiki <https://wiki.yandex-team.ru/yt/userdoc/operations>`_

Python wrapper has some improvements over bare YT operations:

* upload files automatically
* create or erase output table
* delete files after

.. _operation_parameters:

Common operations parameters
----------------------------

* **spec** : (dict) universal method to set operation parameters.

* **job_io** : (dict) spec for job io of all stages of operation, see  \
`IO settings doc <https://wiki.yandex-team.ru/yt/userdoc/table_io>`_.

* **table_writer** : (dict) spec of `"write_table" command \
<https://wiki.yandex-team.ru/yt/userdoc/api#writetable>`_.

* **table_reader** : (dict) spec of `"read_table" command \
<https://wiki.yandex-team.ru/yt/userdoc/api#readtable>`_.

* **format** : (str or descendant of :class:`Format <yt.wrapper.format.Format>`) format of input and output \
data of operation.

* **memory_limit** : (int) memory limit in Mb in *scheduler* for every *job* (512Mb by default).


Operation run under self-pinged transaction, if ``yt.wrapper.config["detached"]`` is `False`.
"""

from . import py_wrapper
from .table_helpers import _prepare_source_tables, _are_default_empty_table, _prepare_table_writer, _remove_tables
from .batch_helpers import create_batch_client, batch_apply
from .batch_response import apply_function_to_result
from .common import flatten, require, unlist, update, parse_bool, is_prefix, get_value, \
                    compose, bool_to_string, get_started_by, MB, GB, \
                    forbidden_inside_job, get_disk_size, round_up_to, set_param, \
                    remove_nones_from_dict
from .retries import Retrier
from .config import get_config
from .cypress_commands import exists, remove, remove_with_empty_dirs, get_attribute, get, \
                              _make_formatted_transactional_request
from .errors import YtError, YtOperationFailedError, YtConcurrentOperationsLimitExceeded
from .exceptions_catcher import KeyboardInterruptsCatcher
from .file_commands import upload_file_to_cache, is_executable
from .format import create_format, YsonFormat, YamrFormat
from .operation_commands import Operation
from .table_commands import create_temp_table, create_table, is_empty, is_sorted, get_sorted_by
from .transaction import Transaction, null_transaction_id
from .ypath import TablePath

from yt.common import to_native_str

import yt.logger as logger
import yt.yson as yson
from yt.yson.parser import YsonParser

from yt.packages.six import text_type, binary_type, PY3
from yt.packages.six.moves import map as imap, zip as izip

import os
import shutil
import sys
import time
import types
from copy import deepcopy

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

@forbidden_inside_job
def run_erase(table, spec=None, sync=True, client=None):
    """Erases table or part of it.

    Erase differs from remove command.
    It only removes content of table (range of records or all table) and doesn't remove Cypress node.

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param dict spec: operation spec.

    .. seealso::  :ref:`operation_parameters`.
    """
    table = TablePath(table, client=client)
    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not exists(table, client=client):
        return
    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: update({"table_path": table}, _),
        lambda _: _apply_spec_overrides(_, client=client),
        lambda _: get_value(_, {})
    )(spec)
    return _make_operation_request("erase", spec, sync, client=client)


@forbidden_inside_job
def run_merge(source_table, destination_table, mode=None,
              sync=True, job_io=None, table_writer=None,
              job_count=None, spec=None, client=None):
    """Merges source tables to destination table.

    :param source_table: tables to merge.
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: path to result table.
    :type destination_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param str mode: one of ["auto", "unordered", "ordered", "sorted"]. "auto" by default. \
    Mode `sorted` keeps sortedness of output tables. \
    Mode `ordered` is about chunk magic, not for ordinary users. \
    In `auto` mode system chooses proper mode depending on the table sortedness.
    :param int job_count: recommendation how many jobs should run.
    :param dict job_io: job io specification.
    :param dict table_writer: standard operation parameter.
    :param dict spec: standard operation parameter.

    .. seealso::  :ref:`operation_parameters`.
    """
    source_table = _prepare_source_tables(source_table, replace_unexisting_by_empty=False, client=client)
    destination_table = unlist(_prepare_destination_tables(destination_table, client=client))

    def is_sorted(table, client):
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

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
        _remove_tables([destination_table], client=client)
        return

    mode = get_value(mode, "auto")
    if mode == "auto":
        mode = "sorted" if all(batch_apply(is_sorted, source_table, client=client)) else "ordered"

    table_writer = _prepare_table_writer(table_writer, client)
    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: _add_job_io_spec("job_io", job_io, table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
        lambda _: update({"mode": mode}, _),
        lambda _: _apply_spec_overrides(_, client=client),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("merge", spec, sync, finalizer=None, client=client)


@forbidden_inside_job
def run_sort(source_table, destination_table=None, sort_by=None,
             sync=True, job_io=None, table_writer=None,
             spec=None, client=None):
    """Sorts source tables to destination table.

    If destination table is not specified, than it equals to source table.

    .. seealso::  :ref:`operation_parameters`.
    """

    sort_by = _prepare_sort_by(sort_by, client)
    source_table = _prepare_source_tables(source_table, replace_unexisting_by_empty=False, client=client)
    exists_results = batch_apply(exists, source_table, client=client)
    for table, exists_result in izip(source_table, exists_results):
        require(exists_result, lambda: YtError("Table %s should exist" % table))

    if destination_table is None:
        if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
            return
        require(len(source_table) == 1 and not source_table[0].has_delimiters(),
                lambda: YtError("You must specify destination sort table "
                                "in case of multiple source tables"))
        destination_table = source_table[0]
    destination_table = unlist(_prepare_destination_tables(destination_table, client=client))

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and not source_table:
        _remove_tables([destination_table], client=client)
        return

    if get_config(client)["yamr_mode"]["run_merge_instead_of_sort_if_input_tables_are_sorted"] \
            and all(sort_by == get_sorted_by(table, [], client=client) for table in source_table):
        return run_merge(source_table, destination_table, "sorted",
                         job_io=job_io, table_writer=table_writer, sync=sync, spec=spec, client=client)

    table_writer = _prepare_table_writer(table_writer, client)
    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: _add_job_io_spec(["partition_job_io", "sort_job_io", "merge_job_io"], job_io, table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": sort_by}, _),
        lambda _: _apply_spec_overrides(_, client=client),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("sort", spec, sync, finalizer=None, client=client)


@forbidden_inside_job
def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None,
                   map_input_format=None, map_output_format=None,
                   reduce_input_format=None, reduce_output_format=None,
                   sync=True, job_io=None, table_writer=None, spec=None,
                   map_files=None, map_local_files=None, map_yt_files=None,
                   reduce_files=None, reduce_local_files=None, reduce_yt_files=None,
                   mapper_memory_limit=None, reducer_memory_limit=None,
                   sort_by=None, reduce_by=None,
                   reduce_combiner=None,
                   reduce_combiner_input_format=None, reduce_combiner_output_format=None,
                   reduce_combiner_files=None, reduce_combiner_local_files=None, reduce_combiner_yt_files=None,
                   reduce_combiner_memory_limit=None,
                   stderr_table=None,
                   client=None):
    """Runs map (optionally), sort, reduce and reduce-combine (optionally) operations.

    :param mapper: python generator, callable object-generator or string (with bash commands).
    :param reducer: python generator, callable object-generator or string (with bash commands).
    :param source_table: input tables or list of tables.
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: output table or list of tables.
    :type destination_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param stderr_table: stderrs table.
    :type stderr_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param format: common format of input, intermediate and output data. More specific formats will override it.
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param map_input_format: input format for map operation.
    :type map_input_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param map_output_format: output format for map operation.
    :type map_output_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param reduce_input_format: input format for reduce operation.
    :type reduce_input_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param reduce_output_format: output format for reduce operation.
    :type reduce_output_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param dict job_io: job io specification.
    :param dict table_writer: standard operation parameter.
    :param dict spec: standard operation parameter.
    :param map_files: Deprecated!
    :param map_local_files: paths to map scripts on local machine.
    :type map_local_files: str or list[str]
    :param map_yt_files: paths to map scripts in Cypress.
    :type map_yt_files: str or list[str]
    :param reduce_files: Deprecated!
    :param reduce_local_files: paths to reduce scripts on local machine.
    :type reduce_combiner_local_files: str or list[str]
    :param reduce_yt_files: paths to reduce scripts in Cypress.
    :type reduce_yt_files: str or list[str]
    :param int mapper_memory_limit: in bytes, map **job** memory limit.
    :param int reducer_memory_limit: in bytes, reduce **job** memory limit.
    :param sort_by: list of columns for sorting by, equals to `reduce_by` by default.
    :type sort_by: str or list[str]
    :param reduce_by: list of columns for grouping by.
    :type reduce_by: str or list[str]
    :param reduce_combiner: python generator, callable object-generator or string (with bash commands).
    :param reduce_combiner_input_format: input format for reduce combiner.
    :type reduce_combiner_input_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param reduce_combiner_output_format: output format for reduce combiner.
    :type reduce_combiner_output_format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param reduce_combiner_files: Deprecated!
    :param reduce_combiner_local_files: paths to reduce combiner scripts on local machine.
    :type reduce_combiner_local_files: str or list[str]
    :param reduce_combiner_yt_files: paths to reduce combiner scripts in Cypress.
    :type reduce_combiner_yt_files: str or list[str]
    :param int reduce_combiner_memory_limit: memory limit in bytes.

    .. seealso::  :ref:`operation_parameters`.
    """

    local_files_to_remove = []

    source_table = _prepare_source_tables(source_table, client=client)
    destination_table = _prepare_destination_tables(destination_table, client=client)
    stderr_table = _prepare_stderr_table(stderr_table, client=client)

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_table):
        _remove_tables(destination_table, client=client)
        return

    if sort_by is None:
        sort_by = reduce_by

    reduce_by = _prepare_reduce_by(reduce_by, client)
    sort_by = _prepare_sort_by(sort_by, client)

    table_writer = _prepare_table_writer(table_writer, client)
    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: update({"stderr_table_path": stderr_table}, _) if stderr_table is not None else _,
        lambda _: _add_job_io_spec(["map_job_io", "reduce_job_io", "sort_job_io"],
                                   job_io, table_writer, _),
        lambda _: _add_input_output_spec(source_table, destination_table, _),
        lambda _: update({"sort_by": sort_by, "reduce_by": reduce_by}, _),
        lambda _: _add_user_command_spec("mapper", mapper,
            format, map_input_format, map_output_format,
            map_files, map_local_files, map_yt_files,
            mapper_memory_limit, None, local_files_to_remove,  _, client=client),
        lambda _: _add_user_command_spec("reducer", reducer,
            format, reduce_input_format, reduce_output_format,
            reduce_files, reduce_local_files, reduce_yt_files,
            reducer_memory_limit, reduce_by, local_files_to_remove, _, client=client),
        lambda _: _add_user_command_spec("reduce_combiner", reduce_combiner,
            format, reduce_combiner_input_format, reduce_combiner_output_format,
            reduce_combiner_files, reduce_combiner_local_files, reduce_combiner_yt_files,
            reduce_combiner_memory_limit, reduce_by, local_files_to_remove, _, client=client),
        lambda _: _apply_spec_overrides(_, client=client),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("map_reduce", spec, sync,
                                   finalizer=Finalizer(local_files_to_remove, destination_table, spec, client=client),
                                   client=client)


@forbidden_inside_job
def _run_operation(binary, source_table, destination_table,
                   files=None, local_files=None, yt_files=None,
                   format=None, input_format=None, output_format=None,
                   sync=True,
                   job_io=None,
                   table_writer=None,
                   job_count=None,
                   memory_limit=None,
                   spec=None,
                   op_name=None,
                   sort_by=None,
                   reduce_by=None,
                   join_by=None,
                   ordered=None,
                   stderr_table=None,
                   client=None):
    """Runs script operation.

    :param binary: python generator, callable object-generator or string (with bash commands).
    :param files: Deprecated!
    :param local_files: paths to scripts on local machine.
    :type local_files: str or list[str]
    :param yt_files: paths to scripts in Cypress.
    :type yt_files: str or list[str]
    :param str op_name: one of ["map", "reduce", ...], "map" by default.
    :param int job_count: recommendation how many jobs should run.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """

    local_files_to_remove = []

    op_name = get_value(op_name, "map")
    source_table = _prepare_source_tables(source_table, client=client)
    destination_table = _prepare_destination_tables(destination_table, client=client)
    stderr_table = _prepare_stderr_table(stderr_table, client=client)

    are_sorted_output = False
    for table in destination_table:
        if table.attributes.get("sorted_by") is not None:
            are_sorted_output = True

    finalize = None

    if op_name == "reduce":
        if sort_by is None:
            sort_by = _prepare_sort_by(reduce_by, client)
        else:
            sort_by = _prepare_sort_by(sort_by, client)
        reduce_by = _prepare_reduce_by(reduce_by, client)
        join_by = _prepare_join_by(join_by, required=False)

        if get_config(client)["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"]:
            are_input_tables_not_properly_sorted = False
            for table in source_table:
                sorted_by = get_sorted_by(table, [], client=client)
                if not sorted_by or not is_prefix(sort_by, sorted_by):
                    are_input_tables_not_properly_sorted = True
                    continue

            if join_by is not None:
                raise YtError("Reduce cannot fallback to map_reduce operation since join_by is specified")

            if are_input_tables_not_properly_sorted and not are_sorted_output:
                if job_count is not None:
                    spec = update({"partition_count": job_count}, spec)
                run_map_reduce(
                    mapper=None,
                    reducer=binary,
                    reduce_files=files,
                    reduce_local_files=local_files,
                    reduce_yt_files=yt_files,
                    source_table=source_table,
                    destination_table=destination_table,
                    format=format,
                    reduce_input_format=input_format,
                    reduce_output_format=output_format,
                    job_io=job_io,
                    table_writer=table_writer,
                    reduce_by=reduce_by,
                    sort_by=sort_by,
                    reducer_memory_limit=memory_limit,
                    sync=sync,
                    spec=spec,
                    client=client)
                return

            if are_input_tables_not_properly_sorted and are_sorted_output:
                if not sync:
                    raise YtError("Replacing yamr-style reduce operation with sort + reduce operations is not supported in sync=False mode.")
                logger.info("Sorting %s", source_table)
                temp_table = create_temp_table(client=client)
                run_sort(source_table, temp_table, sort_by=sort_by, client=client)
                finalize = lambda: remove(temp_table, client=client)
                source_table = [TablePath(temp_table, client=client)]

    if get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and _are_default_empty_table(source_table):
        _remove_tables(destination_table, client=client)
        return

    # Key columns to group rows in job.
    group_by = None
    if op_name == "join_reduce":
        join_by = _prepare_join_by(join_by)
        group_by = join_by
    if op_name == "reduce":
        group_by = reduce_by
        if join_by is not None:
            group_by = join_by

    op_type = None
    if op_name == "map": op_type = "mapper"
    if op_name == "reduce" or op_name == "join_reduce": op_type = "reducer"

    table_writer = _prepare_table_writer(table_writer, client)
    try:
        spec = compose(
            lambda _: _configure_spec(_, client),
            lambda _: update({"stderr_table_path": stderr_table}, _) if stderr_table is not None else _,
            lambda _: _add_job_io_spec("job_io", job_io, table_writer, _),
            lambda _: _add_input_output_spec(source_table, destination_table, _),
            lambda _: update({"reduce_by": reduce_by}, _) if op_name == "reduce" else _,
            # TODO(ignat): add test on sort_by option in reduce
            lambda _: update({"sort_by": sort_by}, _) if (op_name == "reduce" and sort_by is not None) else _,
            lambda _: update({"join_by": join_by}, _) if (op_name == "join_reduce" or (op_name == "reduce" and join_by is not None)) else _,
            lambda _: update({"ordered": bool_to_string(ordered)}, _) \
                if op_name == "map" and ordered is not None else _,
            lambda _: update({"job_count": job_count}, _) if job_count is not None else _,
            lambda _: _add_user_command_spec(op_type, binary,
                format, input_format, output_format,
                files, local_files, yt_files,
                memory_limit, group_by, local_files_to_remove, _, client=client),
            lambda _: _apply_spec_overrides(_, client=client),
            lambda _: get_value(_, {})
        )(spec)

        return _make_operation_request(op_name, spec, sync,
                                       finalizer=Finalizer(local_files_to_remove, destination_table, spec, client=client),
                                       client=client)
    finally:
        if finalize is not None:
            finalize()


def run_map(binary, source_table, destination_table,
            files=None, local_files=None, yt_files=None,
            format=None, input_format=None, output_format=None,
            sync=True,
            job_io=None,
            table_writer=None,
            job_count=None,
            memory_limit=None,
            spec=None,
            ordered=None,
            stderr_table=None,
            client=None):
    """Runs map operation.

    :param bool ordered: force ordered input for mapper.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """
    return _run_operation(op_name="map", **locals())


def run_reduce(binary, source_table, destination_table,
               files=None, local_files=None, yt_files=None,
               format=None, input_format=None, output_format=None,
               sync=True,
               job_io=None,
               table_writer=None,
               job_count=None,
               memory_limit=None,
               spec=None,
               sort_by=None,
               reduce_by=None,
               join_by=None,
               stderr_table=None,
               client=None):
    """Runs reduce operation.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """
    return _run_operation(op_name="reduce", **locals())


def run_join_reduce(binary, source_table, destination_table,
                    files=None, local_files=None, yt_files=None,
                    format=None, input_format=None, output_format=None,
                    sync=True,
                    job_io=None,
                    table_writer=None,
                    job_count=None,
                    memory_limit=None,
                    spec=None,
                    sort_by=None,
                    reduce_by=None,
                    join_by=None,
                    stderr_table=None,
                    client=None):
    """Runs join-reduce operation.

    .. note:: You should specity at least two input table and all except one \
    should have set foreign attibute. You should also specify join_by columns.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """
    return _run_operation(op_name="join_reduce", **locals())


def run_remote_copy(source_table, destination_table,
                    cluster_name=None, network_name=None, cluster_connection=None, copy_attributes=None,
                    spec=None, sync=True, client=None):
    """Copies source table from remote cluster to destination table on current cluster.

    :param source_table: source table to copy (or list of tables).
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: destination table.
    :type destination_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param str cluster_name: cluster name.
    :param str network_name: network name.
    :param dict spec: operation spec.
    :param bool copy_attributes: copy attributes source_table to destination_table.

    .. note:: For atomicity you should specify just one item in `source_table` \
    in case attributes copying.

    .. seealso::  :ref:`operation_parameters`.
    """

    def get_input_name(table):
        return table.to_yson_type()

    source_table = _prepare_source_tables(source_table, client=client)
    destination_table = unlist(_prepare_destination_tables(destination_table, client=client))
    spec = compose(
        lambda _: _configure_spec(_, client),
        lambda _: set_param(_, "network_name", network_name),
        lambda _: set_param(_, "cluster_name", cluster_name),
        lambda _: set_param(_, "cluster_connection", cluster_connection),
        lambda _: set_param(_, "copy_attributes", copy_attributes),
        lambda _: update({"input_table_paths": list(imap(get_input_name, source_table)),
                          "output_table_path": destination_table},
                          _),
        lambda _: _apply_spec_overrides(_, client=client),
        lambda _: get_value(_, {})
    )(spec)

    return _make_operation_request("remote_copy", spec, sync, client=client)


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


def _is_python_function(binary):
    return isinstance(binary, types.FunctionType) or hasattr(binary, "__call__")


def _prepare_formats(format, input_format, output_format, binary, client):
    if format is None:
        format = get_config(client)["tabular_data_format"]
    if format is None and _is_python_function(binary):
        format = YsonFormat(boolean_as_string=False)
    if isinstance(format, str):
        format = create_format(format)
    if isinstance(input_format, str):
        input_format = create_format(input_format)
    if isinstance(output_format, str):
        output_format = create_format(output_format)

    if input_format is None:
        input_format = format
    require(input_format is not None,
            lambda: YtError("You should specify input format"))

    if output_format is None:
        output_format = format
    require(output_format is not None,
            lambda: YtError("You should specify output format"))

    return input_format, output_format


def _prepare_binary(binary, operation_type, input_format, output_format,
                    group_by, file_uploader, client=None):
    if _is_python_function(binary):
        start_time = time.time()
        if isinstance(input_format, YamrFormat) and group_by is not None and set(group_by) != set(["key"]):
            raise YtError("Yamr format does not support reduce by %r", group_by)
        wrap_result = \
            py_wrapper.wrap(function=binary,
                            operation_type=operation_type,
                            input_format=input_format,
                            output_format=output_format,
                            group_by=group_by,
                            uploader=file_uploader,
                            client=client)

        logger.debug("Collecting python modules and uploading to cypress takes %.2lf seconds", time.time() - start_time)
        return wrap_result
    else:
        return py_wrapper.WrapResult(cmd=binary, files=[], tmpfs_size=0, environment={}, local_files_to_remove=[], title=None)


def _prepare_destination_tables(tables, client=None):
    if tables is None:
        if get_config(client)["yamr_mode"]["throw_on_missing_destination"]:
            raise YtError("Destination tables are missing")
        return []
    tables = list(imap(lambda name: TablePath(name, client=client), flatten(tables)))
    batch_client = create_batch_client(raise_errors=True, client=client)
    for table in tables:
        batch_client.create_table(table, ignore_existing=True)
    batch_client.commit_batch()
    return tables

def _prepare_stderr_table(name, client=None):
    if name is None:
        return None

    table = TablePath(name, client=client)
    with Transaction(transaction_id=null_transaction_id, client=client):
        create_table(table, ignore_existing=True, client=client)
    return table


def _add_user_command_spec(op_type, binary, format, input_format, output_format,
                           files, local_files, yt_files,
                           memory_limit, group_by, local_files_to_remove, spec, client=None):
    if binary is None:
        return spec

    if local_files is not None:
        require(files is None, lambda: YtError("You cannot specify files and local_files simultaneously"))
        files = local_files

    file_paths = flatten(get_value(yt_files, []))

    file_uploader = FileUploader(client)
    files = file_uploader(files)
    input_format, output_format = _prepare_formats(format, input_format, output_format, binary=binary, client=client)

    ld_library_path = None
    if _is_python_function(binary) and get_config(client)["pickling"]["dynamic_libraries"]["enable_auto_collection"]:
        ld_library_path = spec.get(op_type, {}).get("environment", {}).get("LD_LIBRARY_PATH")
        paths = ["./modules/_shared", "./tmpfs/modules/_shared"]
        if ld_library_path is not None:
            paths.insert(0, ld_library_path)
        ld_library_path = os.pathsep.join(paths)

    prepare_result = _prepare_binary(binary, op_type, input_format, output_format,
                                     group_by, file_uploader, client=client)

    tmpfs_size = prepare_result.tmpfs_size
    environment = prepare_result.environment
    binary = prepare_result.cmd
    title = prepare_result.title

    if ld_library_path is not None:
        environment["LD_LIBRARY_PATH"] = ld_library_path

    if local_files_to_remove is not None:
        local_files_to_remove += prepare_result.local_files_to_remove

    spec = update(
        {
            op_type: {
                "input_format": input_format.to_yson_type(),
                "output_format": output_format.to_yson_type(),
                "command": binary,
                "file_paths":
                    flatten(files + prepare_result.files + list(imap(lambda path: TablePath(path, client=client), file_paths))),
                "use_yamr_descriptors": bool_to_string(get_config(client)["yamr_mode"]["use_yamr_style_destination_fds"]),
                "check_input_fully_consumed": bool_to_string(get_config(client)["yamr_mode"]["check_input_fully_consumed"])
            }
        },
        spec)

    mount_sandbox_in_tmpfs = get_config(client)["mount_sandbox_in_tmpfs"]
    if isinstance(mount_sandbox_in_tmpfs, bool):  # COMPAT
        enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs
        additional_tmpfs_size = 0
    else:  # dict
        enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs["enable"]
        additional_tmpfs_size = mount_sandbox_in_tmpfs["additional_tmpfs_size"]

    if enable_mount_sandbox_in_tmpfs:
        disk_size = file_uploader.disk_size
        for file in file_paths:
            file_disk_size = None
            if hasattr(file, "attributes") and "disk_size" in file.attributes:
                file_disk_size = file.attributes["disk_size"]
            else:
                attributes = get(file + "/@")
                if attributes["type"] == "table":
                    raise YtError('Attributes "disk_size" must be specified for table file "{0}"'.format(str(file)))
                file_disk_size = attributes["uncompressed_data_size"]
            disk_size += round_up_to(file_disk_size, 4 * 1024)
        tmpfs_size += disk_size
        tmpfs_size += additional_tmpfs_size
        spec = update({op_type: {"tmpfs_size": tmpfs_size, "tmpfs_path": ".", "copy_files": True}}, spec)
    else:
        if tmpfs_size > 0:
            spec = update({op_type: {"tmpfs_size": tmpfs_size, "tmpfs_path": "tmpfs"}}, spec)

    if environment:
        spec = update({op_type: {"environment": environment}}, spec)
    if title:
        spec = update({op_type: {"title": title}}, spec)

    # NB: Configured by common rule now.
    memory_limit = get_value(memory_limit, get_config(client)["memory_limit"])
    if memory_limit is not None:
        memory_limit = int(memory_limit)
    if memory_limit is None and get_config(client)["yamr_mode"]["use_yamr_defaults"]:
        memory_limit = 4 * GB
    if get_config(client)["pickling"]["add_tmpfs_archive_size_to_memory_limit"]:
        if memory_limit is None:
            # Guess that memory limit is 512 MB.
            memory_limit = 512 * MB
        memory_limit += tmpfs_size
    if memory_limit is not None:
        spec = update({op_type: {"memory_limit": memory_limit}}, spec)
    return spec


def _configure_spec(spec, client):
    started_by = get_started_by()
    spec = update(deepcopy(get_config(client)["spec_defaults"]), spec)
    spec = update({"started_by": started_by}, spec)
    if get_config(client)["pool"] is not None:
        spec = update(spec, {"pool": get_config(client)["pool"]})
    if get_config(client)["yamr_mode"]["use_yamr_defaults"]:
        spec = update({"data_size_per_job": 4 * GB}, spec)
    return spec


def _apply_spec_overrides(spec, client):
    return update(deepcopy(get_config(client)["spec_overrides"]), spec)


def _add_input_output_spec(source_table, destination_table, spec):
    def get_input_name(table):
        return table.to_yson_type()
    def get_output_name(table):
        return table.to_yson_type()

    spec = update({"input_table_paths": list(imap(get_input_name, source_table))}, spec)
    if isinstance(destination_table, TablePath):
        spec = update({"output_table_path": get_output_name(destination_table)}, spec)
    else:
        spec = update({"output_table_paths": list(imap(get_output_name, destination_table))}, spec)
    return spec


def _add_job_io_spec(job_types, job_io, table_writer, spec):
    if job_io is not None or table_writer is not None:
        if job_io is None:
            job_io = {}
        if table_writer is not None:
            job_io = update(job_io, {"table_writer": table_writer})
        for job_type in flatten(job_types):
            spec = update({job_type: job_io}, spec)
    return spec

class OperationRequestRetrier(Retrier):
    def __init__(self, command_name, spec, client=None):
        self.command_name = command_name
        self.spec = spec
        self.client = client

        retry_config = {
            "count": get_config(client)["start_operation_retries"]["retry_count"],
            "backoff": get_config(client)["retry_backoff"],
        }
        retry_config = update(deepcopy(get_config(client)["start_operation_retries"]),
                              remove_nones_from_dict(retry_config))
        timeout = get_value(get_config(client)["start_operation_retries"]["retry_timeout"],
                            get_config(client)["start_operation_request_timeout"])

        super(OperationRequestRetrier, self).__init__(retry_config=retry_config,
                                                      timeout=timeout,
                                                      exceptions=(YtConcurrentOperationsLimitExceeded,))

    def action(self):
        return _make_formatted_transactional_request(self.command_name, {"spec": self.spec}, format=None,
                                                     client=self.client)

    def backoff_action(self, iter_number, sleep_backoff):
        logger.warning("Failed to start operation since concurrent operation limit exceeded. "
                       "Sleep for %.2lf seconds before next (%d) retry.",
                       sleep_backoff, iter_number)
        time.sleep(sleep_backoff)

def _make_operation_request(command_name, spec, sync,
                            finalizer=None, client=None):
    def _manage_operation(finalizer):
        retrier = OperationRequestRetrier(command_name=command_name, spec=spec, client=client)
        operation_id = retrier.run()
        operation = Operation(command_name, operation_id, finalize=finalizer, client=client)

        if operation.url:
            logger.info("Operation started: %s", operation.url)
        else:
            logger.info("Operation started: %s", operation.id)

        if sync:
            operation.wait()
        return operation


    if get_config(client)["detached"]:
        return _manage_operation(finalizer)
    else:
        transaction = Transaction(
            attributes={"title": "Python wrapper: envelope transaction of operation"},
            client=client)

        def finish_transaction():
            transaction.__exit__(*sys.exc_info())

        def attached_mode_finalizer(state):
            transaction.__exit__(None, None, None)
            if finalizer is not None:
                finalizer(state)

        transaction.__enter__()
        with KeyboardInterruptsCatcher(finish_transaction, enable=get_config(client)["operation_tracker"]["abort_on_sigint"]):
            return _manage_operation(attached_mode_finalizer)


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


class FileUploader(object):
    def __init__(self, client):
        self.client = client
        self.disk_size = 0

    def __call__(self, files):
        if files is None:
            return []

        file_paths = []
        with Transaction(transaction_id=null_transaction_id, attributes={"title": "Python wrapper: upload operation files"}, client=self.client):
            for file in flatten(files):
                if isinstance(file, (text_type, binary_type)):
                    file_params = {"filename": file}
                else:
                    file_params = deepcopy(file)

                # Hacky way to split string into file path and file path attributes.
                filename = file_params.pop("filename")
                if PY3:
                    filename_bytes = filename.encode("utf-8")
                else:
                    filename_bytes = filename

                stream = BytesIO(filename_bytes)
                parser = YsonParser(
                    stream,
                    encoding="utf-8" if PY3 else None,
                    always_create_attributes=True)

                attributes = {}
                if parser._has_attributes():
                    attributes = parser._parse_attributes()
                    filename = to_native_str(stream.read())

                self.disk_size += get_disk_size(filename)

                path = upload_file_to_cache(filename=filename, client=self.client, **file_params)
                file_paths.append(yson.to_yson_type(path, attributes={
                    "executable": is_executable(filename, client=self.client),
                    "file_name": attributes.get("file_name", os.path.basename(filename)),
                }))
        return file_paths
