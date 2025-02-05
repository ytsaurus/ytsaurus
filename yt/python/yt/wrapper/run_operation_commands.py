"""
Commands for table working and Map-Reduce operations.

.. seealso:: `operations in the docs <https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/overview>`_

Python wrapper has some improvements over bare YT operations:

* upload files automatically
* create or erase output table
* delete files after

.. _operation_parameters:

Common operations parameters
----------------------------

* **spec** : (dict) universal method to set operation parameters.

* **job_io** : (dict) spec for job io of all stages of operation, see  \
`IO settings doc <https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration>`_.

* **table_writer** : (dict) spec of `"write_table" command \
<https://ytsaurus.tech/docs/en/api/commands#write_table>`_.

* **table_reader** : (dict) spec of `"read_table" command \
<https://ytsaurus.tech/docs/en/api/commands#read_table>`_.

* **format** : (str or descendant of :class:`Format <yt.wrapper.format.Format>`) format of input and output \
data of operation.

* **memory_limit** : (int) memory limit in Mb in *scheduler* for every *job* (512Mb by default).


Operation run under self-pinged transaction, if ``yt.wrapper.config["detached"]`` is `False`.
"""

from .table_helpers import _are_default_empty_table, _remove_tables
from .common import is_prefix, forbidden_inside_job
from .retries import Retrier
from .config import get_config, get_command_param
from .cypress_commands import get, remove
from .driver import make_formatted_request
from .errors import YtError, YtConcurrentOperationsLimitExceeded, YtMasterDisconnectedError
from .exceptions_catcher import KeyboardInterruptsCatcher
from .operation_commands import Operation
from .spec_builders import (ReduceSpecBuilder, MergeSpecBuilder, SortSpecBuilder,
                            EraseSpecBuilder, MapReduceSpecBuilder, RemoteCopySpecBuilder,
                            JoinReduceSpecBuilder, MapSpecBuilder)
from .table_commands import create_temp_table, get_sorted_by
from .table_helpers import _prepare_job_io, _prepare_operation_files
from .transaction import Transaction
from .ypath import TablePath
from .driver import get_api_version

import yt.logger as logger
from yt.common import YT_NULL_TRANSACTION_ID as null_transaction_id, _pretty_format_for_logging

import sys
import time


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
    spec_builder = EraseSpecBuilder() \
        .table_path(table) \
        .spec(spec)
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


@forbidden_inside_job
def run_merge(source_table, destination_table, mode=None,
              sync=True, job_io=None, table_writer=None,
              job_count=None, spec=None, merge_by=None,
              client=None):
    """Merges source tables to destination table.

    :param source_table: tables to merge.
    :type source_table: list[str or :class:`TablePath <yt.wrapper.ypath.TablePath>`]
    :param destination_table: path to result table.
    :type destination_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param str mode: one of ["auto", "unordered", "ordered", "sorted"]. "auto" by default.
        Mode `sorted` keeps sortedness of output tables.
        Mode `ordered` is about chunk magic, not for ordinary users.
        In `auto` mode system chooses proper mode depending on the table sortedness.
    :param int job_count: recommendation how many jobs should run.
    :param dict job_io: job io specification.
    :param dict table_writer: standard operation parameter.
    :param dict spec: standard operation parameter.
    :param merge_by: list of columns for merging by (works only for `sorted` mode)
    :type merge_by: list[str]

    .. seealso::  :ref:`operation_parameters`.
    """

    job_io = _prepare_job_io(job_io, table_writer)
    spec_builder = MergeSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_path(destination_table) \
        .mode(mode) \
        .job_io(job_io) \
        .job_count(job_count) \
        .merge_by(merge_by) \
        .spec(spec)
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


@forbidden_inside_job
def run_sort(source_table, destination_table=None, sort_by=None,
             sync=True, job_io=None, table_writer=None,
             spec=None, client=None):
    """Sorts source tables to destination table.

    If destination table is not specified, than it equals to source table.

    .. seealso::  :ref:`operation_parameters`.
    """

    job_io = _prepare_job_io(job_io, table_writer)
    spec_builder = SortSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_path(destination_table) \
        .sort_by(sort_by) \
        .partition_job_io(job_io) \
        .sort_job_io(job_io) \
        .merge_job_io(job_io) \
        .spec(spec)
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


@forbidden_inside_job
def run_map_reduce(mapper, reducer, source_table, destination_table,
                   format=None,
                   map_input_format=None, map_output_format=None,
                   reduce_input_format=None, reduce_output_format=None,
                   sync=True, job_io=None, table_writer=None, spec=None,
                   map_local_files=None, map_yt_files=None,
                   reduce_local_files=None, reduce_yt_files=None,
                   mapper_memory_limit=None, reducer_memory_limit=None,
                   sort_by=None, reduce_by=None,
                   reduce_combiner=None,
                   reduce_combiner_input_format=None, reduce_combiner_output_format=None,
                   reduce_combiner_local_files=None, reduce_combiner_yt_files=None,
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
    :param map_local_files: paths to map scripts on local machine.
    :type map_local_files: str or list[str]
    :param map_yt_files: paths to map scripts in Cypress.
    :type map_yt_files: str or list[str]
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
    :param reduce_combiner_local_files: paths to reduce combiner scripts on local machine.
    :type reduce_combiner_local_files: str or list[str]
    :param reduce_combiner_yt_files: paths to reduce combiner scripts in Cypress.
    :type reduce_combiner_yt_files: str or list[str]
    :param int reduce_combiner_memory_limit: memory limit in bytes.

    .. seealso::  :ref:`operation_parameters`.
    """

    job_io = _prepare_job_io(job_io, table_writer)
    map_file_paths = _prepare_operation_files(map_local_files, map_yt_files)
    reduce_file_paths = _prepare_operation_files(reduce_local_files, reduce_yt_files)
    reduce_combiner_file_paths = _prepare_operation_files(reduce_combiner_local_files, reduce_combiner_yt_files)

    spec_builder = MapReduceSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_paths(destination_table) \
        .stderr_table_path(stderr_table) \
        .map_job_io(job_io) \
        .reduce_job_io(job_io) \
        .sort_job_io(job_io) \
        .sort_by(sort_by) \
        .reduce_by(reduce_by) \
        .spec(spec)  # noqa

    if mapper is not None:
        spec_builder = spec_builder \
            .begin_mapper() \
                .command(mapper) \
                .format(format) \
                .input_format(map_input_format) \
                .output_format(map_output_format) \
                .file_paths(map_file_paths) \
                .memory_limit(mapper_memory_limit) \
            .end_mapper()  # noqa
    if reducer is not None:
        spec_builder = spec_builder \
            .begin_reducer() \
                .command(reducer) \
                .format(format) \
                .input_format(reduce_input_format) \
                .output_format(reduce_output_format) \
                .file_paths(reduce_file_paths) \
                .memory_limit(reducer_memory_limit) \
            .end_reducer()  # noqa
    if reduce_combiner is not None:
        spec_builder = spec_builder \
            .begin_reduce_combiner() \
                .command(reduce_combiner) \
                .format(format) \
                .input_format(reduce_combiner_input_format) \
                .output_format(reduce_combiner_output_format) \
                .file_paths(reduce_combiner_file_paths) \
                .memory_limit(reduce_combiner_memory_limit) \
            .end_reduce_combiner()  # noqa
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


def run_map(binary, source_table, destination_table=None,
            local_files=None, yt_files=None,
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

    job_io = _prepare_job_io(job_io, table_writer)
    file_paths = _prepare_operation_files(local_files, yt_files)

    spec_builder = MapSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_paths(destination_table) \
        .stderr_table_path(stderr_table) \
        .begin_mapper() \
            .command(binary) \
            .file_paths(file_paths) \
            .format(format) \
            .input_format(input_format) \
            .output_format(output_format) \
            .memory_limit(memory_limit) \
        .end_mapper() \
        .ordered(ordered) \
        .job_count(job_count) \
        .job_io(job_io) \
        .spec(spec)  # noqa
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


def run_reduce(binary, source_table, destination_table=None,
               local_files=None, yt_files=None,
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
               enable_key_guarantee=None,
               client=None):
    """Runs reduce operation.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """

    job_io = _prepare_job_io(job_io, table_writer)
    file_paths = _prepare_operation_files(local_files, yt_files)

    spec_builder = ReduceSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_paths(destination_table) \
        .stderr_table_path(stderr_table) \
        .begin_reducer() \
            .command(binary) \
            .file_paths(file_paths) \
            .format(format) \
            .input_format(input_format) \
            .output_format(output_format) \
            .memory_limit(memory_limit) \
        .end_reducer() \
        .sort_by(sort_by) \
        .reduce_by(reduce_by) \
        .join_by(join_by) \
        .job_count(job_count) \
        .job_io(job_io) \
        .enable_key_guarantee(enable_key_guarantee) \
        .spec(spec)  # noqa
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


def run_join_reduce(binary, source_table, destination_table,
                    local_files=None, yt_files=None,
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

    .. note:: You should specify at least two input table and all except one \
    should have set foreign attribute. You should also specify join_by columns.

    .. seealso::  :ref:`operation_parameters` and :func:`run_map_reduce <.run_map_reduce>`.
    """

    job_io = _prepare_job_io(job_io, table_writer)
    file_paths = _prepare_operation_files(local_files, yt_files)

    spec_builder = JoinReduceSpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_paths(destination_table) \
        .stderr_table_path(stderr_table) \
        .begin_reducer() \
            .command(binary) \
            .file_paths(file_paths) \
            .format(format) \
            .input_format(input_format) \
            .output_format(output_format) \
            .memory_limit(memory_limit) \
        .end_reducer() \
        .join_by(join_by) \
        .job_count(job_count) \
        .job_io(job_io) \
        .spec(spec)  # noqa
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


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

    spec_builder = RemoteCopySpecBuilder() \
        .input_table_paths(source_table) \
        .output_table_path(destination_table) \
        .cluster_name(cluster_name) \
        .network_name(network_name) \
        .cluster_connection(cluster_connection) \
        .copy_attributes(copy_attributes) \
        .spec(spec)
    return run_operation(spec_builder, sync=sync, enable_optimizations=True, client=client)


class OperationRequestRetrier(Retrier):
    def __init__(self, operation_type, spec, retry_actions, mutation_id, client=None):
        self.operation_type = operation_type
        self.spec = spec
        self.retry_actions = retry_actions
        self.mutation_id = mutation_id
        self.client = client

        retry_config = get_config(client)["start_operation_retries"]
        timeout = get_config(client)["start_operation_request_timeout"]

        self.request_timeout = timeout

        exceptions = (
            YtConcurrentOperationsLimitExceeded,
            YtMasterDisconnectedError)

        super(OperationRequestRetrier, self).__init__(retry_config=retry_config,
                                                      timeout=timeout,
                                                      exceptions=exceptions)

    def action(self):
        result = make_formatted_request(
            "start_operation" if get_api_version(self.client) == "v4" else "start_op",
            {"operation_type": self.operation_type, "spec": self.spec},
            format=None,
            mutation_id=self.mutation_id,
            timeout=self.request_timeout,
            client=self.client)
        return result["operation_id"] if get_api_version(self.client) == "v4" else result

    def except_action(self, exception, attempt):
        self.exception = exception

    def backoff_action(self, iter_number, sleep_backoff):
        for action in self.retry_actions:
            action()
        error_tail = ""
        if isinstance(self.exception, YtConcurrentOperationsLimitExceeded):
            error_tail = _pretty_format_for_logging(self.exception)
        logger.warning("Failed to start operation since concurrent operation limit exceeded. "
                       "Sleep for %.2lf seconds before next (%d) retry.%s",
                       sleep_backoff, iter_number, error_tail)
        time.sleep(sleep_backoff)


def _make_operation_request(operation_type, spec, sync, mutation_id,
                            finalization_actions=None,
                            retry_actions=None,
                            client=None):
    def _manage_operation(finalization_actions):
        retrier = OperationRequestRetrier(
            operation_type=operation_type,
            spec=spec,
            retry_actions=retry_actions,
            mutation_id=mutation_id,
            client=client)
        operation_id = retrier.run()
        operation = Operation(
            operation_id,
            type=operation_type,
            finalization_actions=finalization_actions,
            client=client)

        if operation.url:
            logger.info("Operation started: %s", operation.url)
        else:
            logger.info("Operation started: %s", operation.id)

        if sync:
            operation.wait()
        return operation

    attached = not get_config(client)["detached"]
    transaction_id = get_command_param("transaction_id", client)
    should_ping_transaction = \
        get_command_param("ping_ancestor_transactions", client) and \
        transaction_id != null_transaction_id

    if attached or should_ping_transaction:
        if attached:
            transaction = Transaction(
                attributes={"title": "Python wrapper: envelope transaction of operation"},
                client=client)
        else:  # should_ping_transaction
            timeout = get("#{0}/@timeout".format(transaction_id), client=client)
            transaction = Transaction(
                transaction_id=get_command_param("transaction_id", client),
                timeout=timeout,
                ping=True,
                client=client)

        def finish_transaction():
            transaction.__exit__(*sys.exc_info())

        def attached_mode_finalizer(state):
            transaction.__exit__(None, None, None)
            if finalization_actions is not None:
                for finalize_function in finalization_actions:
                    finalize_function(state)

        transaction.__enter__()
        with KeyboardInterruptsCatcher(finish_transaction,
                                       enable=get_config(client)["operation_tracker"]["abort_on_sigint"]):
            return _manage_operation(attached_mode_finalizer)
    else:
        return _manage_operation(finalization_actions)


def _run_sort_optimizer(spec, client=None):
    operation_type = "sort"
    is_sorted = all(spec.get("sort_by") == get_sorted_by(table, [], client=client)
                    for table in spec.get("input_table_paths"))
    if get_config(client)["yamr_mode"]["run_merge_instead_of_sort_if_input_tables_are_sorted"] and is_sorted:
        merge_spec_keys = filter(lambda attr: not attr.startswith("_"), dir(MergeSpecBuilder))

        merge_spec = {}
        for key in merge_spec_keys:
            if key in spec:
                merge_spec[key] = spec[key]

        merge_spec["mode"] = "sorted"
        operation_type = "merge"
        builder = MergeSpecBuilder(merge_spec)
        spec = builder.build(client=client)

    return [(operation_type, spec, [])]


def _run_reduce_optimizer(spec, client=None):
    operations_list = [("reduce", spec, [])]

    if get_config(client)["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"]:
        are_sorted_output = False
        for table in spec.get("output_table_paths", []):
            table = TablePath(table)
            if "sorted_by" in table.attributes:
                are_sorted_output = True

        are_input_tables_not_properly_sorted = False
        for table in spec["input_table_paths"]:
            sorted_by = get_sorted_by(table, [], client=client)
            if not sorted_by or not is_prefix(spec.get("sort_by"), sorted_by):
                are_input_tables_not_properly_sorted = True
                break

        if "join_by" in spec:
            raise YtError("Reduce cannot fallback to map_reduce operation since join_by is specified")

        if are_input_tables_not_properly_sorted:
            if are_sorted_output:
                temp_table = create_temp_table(client=client)

                sort_spec = SortSpecBuilder() \
                    .input_table_paths(spec["input_table_paths"]) \
                    .output_table_path(temp_table) \
                    .sort_by(spec["sort_by"]) \
                    .build(client)

                sort_operation_list = _run_sort_optimizer(sort_spec, client)

                spec["input_table_paths"] = [TablePath(temp_table, client=client)]
                operations_list = sort_operation_list + [("reduce", spec, [lambda state: remove(temp_table, client=client)])]
            else:
                if "partition_count" not in spec and "job_count" in spec:
                    spec["partition_count"] = spec["job_count"]
                operations_list = [("map_reduce", spec, [])]

    return operations_list


@forbidden_inside_job
def run_operation(spec_builder, sync=True, run_operation_mutation_id=None, enable_optimizations=False, client=None):
    """Runs operation.

    :param spec_builder: spec builder with parameters of the operation.
    :type spec_builder: :class:`SpecBuilder <yt.wrapper.spec_builders.SpecBuilder>`

    .. seealso::  :ref:`operation_parameters`.
    """

    OPERATION_OPTIMIZERS = {"reduce": _run_reduce_optimizer,
                            "sort": _run_sort_optimizer}

    spec_builder.prepare(client=client)
    destination_tables = spec_builder.get_output_table_paths()

    if enable_optimizations and get_config(client)["yamr_mode"]["treat_unexisting_as_empty"] and \
            _are_default_empty_table(spec_builder.get_input_table_paths()):
        _remove_tables(destination_tables, client=client)
        return

    spec = spec_builder.build(client=client)

    operations_list = [(spec_builder.operation_type, spec, [])]
    if enable_optimizations and spec_builder.operation_type in OPERATION_OPTIMIZERS:
        operations_list = OPERATION_OPTIMIZERS[spec_builder.operation_type](spec, client)

    _, _, finalization_actions = operations_list[-1]

    finalizer = spec_builder.get_finalizer(spec, client=client)
    finalization_actions.append(finalizer)

    retry_actions = [spec_builder.get_toucher(client=client)]

    if len(operations_list) > 1 and not sync:
        raise YtError(
            "Operation has been organized as a chain of several operations. "
            "Using sync=False is not possible in this situation.")

    result = None
    operations_iterator = iter(operations_list)
    try:
        for operation_type, spec, finalization_actions in operations_iterator:
            result = _make_operation_request(operation_type=operation_type,
                                             spec=spec,
                                             finalization_actions=finalization_actions,
                                             retry_actions=retry_actions,
                                             sync=sync,
                                             mutation_id=run_operation_mutation_id,
                                             client=client)

        return result
    except:  # noqa
        for _, _, finalization_actions in operations_iterator:
            for finalize_function in finalization_actions:
                finalize_function(None)
        raise
