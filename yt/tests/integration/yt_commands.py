from __future__ import print_function

from yt.environment.yt_env import set_environment_driver_logging_config
from yt.environment import arcadia_interop

import yt.yson as yson
from yt_driver_bindings import Driver, Request, reopen_logs
from yt.common import YtError, YtResponseError, flatten, update_inplace, update, date_string_to_datetime

from yt.test_helpers import wait, WaitFailed
from yt.test_helpers.job_events import JobEvents, TimeoutError

import __builtin__
import contextlib
import copy as pycopy
import logging
import os
import pytest
import random
import stat
import string
import sys
import tempfile
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from cStringIO import StringIO, OutputType

###########################################################################

root_logger = logging.getLogger()

clusters_drivers = {}
is_multicell = None
path_to_run_tests = None
_zombie_responses = []
_events_on_fs = None
default_api_version = 4

# TODO(levysotsky): Move error codes to separate file in python repo.
SortOrderViolation = 301
UniqueKeyViolation = 306
SchemaViolation = 307
InvalidSchemaValue = 314
ResolveErrorCode = 500
AuthorizationErrorCode = 901
UserJobProducedCoreFiles = 1206
UnrecognizedConfigOption = 1400
FailedToFetchDynamicConfig = 1401
DuplicateMatchingDynamicConfigs = 1402
UnrecognizedDynamicConfigOption = 1403
InvalidDynamicConfig = 1405
NoSuchOperation = 1915
NoSuchJob = 1916
NoSuchAttribute = 1920
TabletNotMounted = 1702

# See transaction_client/public.h
SyncLastCommittedTimestamp   = 0x3fffffffffffff01
AsyncLastCommittedTimestamp  = 0x3fffffffffffff04
MinTimestamp                 = 0x0000000000000001

def authors(*the_authors):
    return pytest.mark.authors(the_authors)

@contextlib.contextmanager
def raises_yt_error(code):
    """
    Context manager that helps to check that code raises YTError.
    When description is int we check that raised error contains this error code.
    When description is string we check that raised error contains description as substring.

    Examples:
        with raises_yt_error(SortOrderViolation):
            ...

        with raises_yt_error("Name of struct field #0 is empty"):
            ...
    """
    if not isinstance(code, (str, int)):
        raise TypeError("code must be str or int, actual type: {}".format(code.__class__))
    try:
        yield
        raise AssertionError("Expected exception to be raised.")
    except YtError as e:
        if isinstance(code, int):
            if not e.contains_code(code):
                raise AssertionError("Raised error doesn't contain error code {}:\n{}".format(
                    code,
                    e,
                ))
        else:
            assert isinstance(code, str)
            if code not in str(e):
                raise AssertionError("Raised error doesn't contain {}:\n{}".format(
                    code,
                    e,
                ))

def print_debug(*args):
    if arcadia_interop.yatest_common is None:
        print(*args, file=sys.stderr)
    else:
        if args:
            root_logger.debug(" ".join(__builtin__.map(str, args)))

def is_debug():
    try:
        from yson_lib import is_debug_build
    except ImportError:
        from yt_yson_bindings.yson_lib import is_debug_build

    return is_debug_build()

def get_driver(cell_index=0, cluster="primary", api_version=default_api_version):
    if cluster not in clusters_drivers:
        return None

    return clusters_drivers[cluster][cell_index][api_version]

def _get_driver(driver):
    if driver is None:
        return get_driver()
    else:
        return driver

def init_drivers(clusters):
    def create_drivers(config):
        drivers = {}
        for api_version in (3, 4):
            config = pycopy.deepcopy(config)
            config["api_version"] = api_version
            drivers[api_version] = Driver(config=config)
        return drivers

    for instance in clusters:
        if instance.master_count > 0:
            prefix = "" if instance.driver_backend == "native" else "rpc_"
            secondary_driver_configs = [instance.configs[prefix + "driver_secondary_" + str(i)]
                                        for i in xrange(instance.secondary_master_cell_count)]
            drivers = create_drivers(instance.configs[prefix + "driver"])

            # Setup driver logging for all instances in the environment as in the primary cluster.
            if instance._cluster_name == "primary":
                # XXX(max42): remove this when Python sync is over.
                try:
                    set_environment_driver_logging_config(instance.driver_logging_config, instance.driver_backend)
                except TypeError:
                    set_environment_driver_logging_config(instance.driver_logging_config)

            secondary_drivers = []
            for secondary_driver_config in secondary_driver_configs:

                secondary_drivers.append(create_drivers(secondary_driver_config))

            clusters_drivers[instance._cluster_name] = [drivers] + secondary_drivers

def wait_assert(check_fn, *args, **kwargs):
    last_exception = []
    last_exc_info = []
    def wrapper():
        try:
            check_fn(*args, **kwargs)
        except AssertionError as e:
            last_exception[:] = [e]
            last_exc_info[:] = [sys.exc_info()]
            print_debug("Assertion failed, retrying.\n{}".format(e))
            return False
        return True
    try:
        wait(wrapper)
    except WaitFailed:
        if not last_exception:
            raise
        tb = "\n".join(traceback.format_tb(last_exc_info[0][2]))
        raise AssertionError("waited assertion failed\n{}{}".format(tb, last_exception[0]))

def wait_drivers():
    for cluster in clusters_drivers.values():
        cell_tag = 0
        def driver_is_ready():
            return get("//@", driver=cluster[cell_tag][default_api_version])

        wait(driver_is_ready, ignore_exceptions=True)

def terminate_drivers():
    for cluster in clusters_drivers:
        for drivers_by_cell_tag in clusters_drivers[cluster]:
            for driver in drivers_by_cell_tag.itervalues():
                driver.terminate()
    clusters_drivers.clear()

def get_branch(dict, path):
    root = dict
    for field in path:
        assert isinstance(field, str), "non-string keys are not allowed in command parameters"
        if field not in root:
            return None
        root = root[field]
    return root

def set_branch(dict, path, value):
    root = dict
    for field in path[:-1]:
        assert isinstance(field, str), "non-string keys are not allowed in command parameters"
        if field not in root:
            root[field] = {}
        root = root[field]
    root[path[-1]] = value

def change(dict, old, new):
    if old in dict:
        set_branch(dict, flatten(new), dict[old])
        del dict[old]

def flat(dict, key):
    if key in dict:
        dict[key] = flatten(dict[key])

def prepare_path(path):
    attributes = {}
    if isinstance(path, yson.YsonString):
        attributes = path.attributes
    result = execute_command("parse_ypath", parameters={"path": path}, verbose=False, parse_yson=True)
    update_inplace(result.attributes, attributes)
    return result

def prepare_paths(paths):
    return [prepare_path(path) for path in flatten(paths)]

def prepare_parameters(parameters):
    change(parameters, "tx", "transaction_id")
    change(parameters, "ping_ancestor_txs", "ping_ancestor_transactions")
    return parameters

def retry(func, retry_timeout=timedelta(seconds=10), retry_interval=timedelta(milliseconds=100)):
    now = datetime.now()
    deadline = now + retry_timeout
    while now < deadline:
        try:
            return func()
        except:
            time.sleep(retry_interval.total_seconds())
            now = datetime.now()
    return func()

def execute_command(command_name, parameters, input_stream=None, output_stream=None,
                    verbose=None, verbose_error=None, ignore_result=False, return_response=False,
                    parse_yson=None, unwrap_v4_result=True):
    global _zombie_responses

    if "verbose" in parameters:
        verbose = parameters["verbose"]
        del parameters["verbose"]
    verbose = verbose is None or verbose

    if "verbose_error" in parameters:
        verbose_error = parameters["verbose_error"]
        del parameters["verbose_error"]
    verbose_error = verbose_error is None or verbose_error

    if "ignore_result" in parameters:
        ignore_result = parameters["ignore_result"]
        del parameters["ignore_result"]
    ignore_result = ignore_result is None or ignore_result

    if "return_response" in parameters:
        return_response = parameters["return_response"]
        del parameters["return_response"]
    return_response = return_response is None or return_response

    driver = _get_driver(parameters.pop("driver", None))

    command_rewrites = {
        "start_tx": "start_transaction",
        "abort_tx": "abort_transaction",
        "commit_tx": "commit_transaction",
        "ping_tx": "ping_transaction",

        "start_op": "start_operation",
        "abort_op": "abort_operation",
        "suspend_op": "suspend_operation",
        "resume_op": "resume_operation",
        "complete_op": "complete_operation",
        "update_op_parameters": "update_operation_parameters",
    }
    if driver.get_config()["api_version"] == 4 and command_name in command_rewrites:
        command_name = command_rewrites[command_name]

    if command_name in ("merge", "erase", "map", "sort", "reduce", "join_reduce", "map_reduce", "remote_copy"):
        parameters["operation_type"] = command_name
        command_name = "start_operation"

    authenticated_user = None
    if "authenticated_user" in parameters:
        authenticated_user = parameters["authenticated_user"]
        del parameters["authenticated_user"]

    if "rewrite_operation_path" not in parameters:
        parameters["rewrite_operation_path"] = False

    if "path" in parameters and command_name != "parse_ypath":
        parameters["path"] = prepare_path(parameters["path"])

    if "paths" in parameters:
        parameters["paths"] = yson.loads(parameters["paths"])
        for index in range(len(parameters["paths"])):
            parameters["paths"][index] = prepare_path(parameters["paths"][index])

    response_parameters = parameters.pop("response_parameters", None)

    yson_format = yson.to_yson_type("yson", attributes={"format": "text"})
    description = driver.get_command_descriptor(command_name)
    if description.input_type() != "null" and parameters.get("input_format") is None:
        parameters["input_format"] = yson_format
    if description.output_type() != "null":
        if parameters.get("output_format") is None:
            parameters["output_format"] = yson_format
        if output_stream is None:
            output_stream = StringIO()

    parameters = prepare_parameters(parameters)

    if verbose:
        def is_text_yson(fmt):
            # XXX(sandello): This is a stupid check.
            return repr(fmt) == "{'attributes': {'format': 'text'}, 'value': 'yson'}"

        pretty_parameters = pycopy.deepcopy(parameters)
        for key in ["input_format", "output_format"]:
            if is_text_yson(pretty_parameters.get(key, None)):
                pretty_parameters.pop(key)

        print_debug(str(datetime.now()), command_name, pretty_parameters)

    response = driver.execute(
        Request(command_name=command_name,
                parameters=parameters,
                input_stream=input_stream,
                output_stream=output_stream,
                user=authenticated_user))

    if ignore_result:
        _zombie_responses.append(response)
        return

    if return_response:
        return response

    response.wait()

    if response_parameters is not None:
        response_params = response.response_parameters()
        print_debug(response_parameters)
        response_parameters.update(response_params)

    if not response.is_ok():
        # TODO(ignat): it build empty error with response.error() as inner error. Fix it!
        error = YtResponseError(response.error())
        if verbose_error:
            print_debug(str(error))
            # NB: we want to see inner errors in teamcity.
        raise error
    if isinstance(output_stream, OutputType):
        result = output_stream.getvalue()
        if verbose:
            print_debug(result)
        if parse_yson:
            result = yson.loads(result)
            if unwrap_v4_result and driver.get_config()["api_version"] == 4 and isinstance(result, dict) and len(result.keys()) == 1:
                result = result.values()[0]
            if driver.get_config()["api_version"] == 3 and command_name == "lock":
                result = {"lock_id": result}
        return result

def execute_command_with_output_format(command_name, kwargs, input_stream=None):
    has_output_format = "output_format" in kwargs
    return_response = "return_response" in kwargs
    if not has_output_format:
        kwargs["output_format"] = yson.loads("<format=text>yson")
    output = kwargs.pop("output_stream", StringIO())
    response = execute_command(command_name, kwargs, input_stream=input_stream, output_stream=output)
    if return_response:
        return response
    if not has_output_format:
        return list(yson.loads(output.getvalue(), yson_type="list_fragment"))
    else:
        return output.getvalue()

###########################################################################

def _assert_true_for_cell(cell_index, predicate):
    assert predicate(get_driver(cell_index))

def assert_true_for_secondary_cells(env, predicate):
    for i in xrange(env.secondary_master_cell_count):
        _assert_true_for_cell(i + 1, predicate)

def assert_true_for_all_cells(env, predicate):
    _assert_true_for_cell(0, predicate)
    assert_true_for_secondary_cells(env, predicate)

def _check_true_for_all_cells(env, predicate):
    for i in xrange(env.secondary_master_cell_count + 1):
        if not predicate(get_driver(i)):
            return False
    return True

def wait_true_for_all_cells(env, predicate):
    wait(lambda: _check_true_for_all_cells(env, predicate))

###########################################################################

def multicell_sleep():
    if is_multicell:
        time.sleep(0.5)

def master_memory_sleep():
    multicell_sleep()
    time.sleep(0.2)
    multicell_sleep()

def dump_job_context(job_id, path, **kwargs):
    kwargs["job_id"] = job_id
    kwargs["path"] = path
    return execute_command("dump_job_context", kwargs)

def get_job_input(job_id, **kwargs):
    kwargs["job_id"] = job_id
    return execute_command("get_job_input", kwargs)

def get_job_input_paths(job_id, **kwargs):
    kwargs["job_id"] = job_id
    return execute_command("get_job_input_paths", kwargs)

def get_table_columnar_statistics(paths, **kwargs):
    kwargs["paths"] = paths
    return execute_command("get_table_columnar_statistics", kwargs, parse_yson=True)

def get_job_stderr(operation_id, job_id, **kwargs):
    kwargs["operation_id"] = operation_id
    kwargs["job_id"] = job_id
    return execute_command("get_job_stderr", kwargs)

def get_job_fail_context(operation_id, job_id, **kwargs):
    kwargs["operation_id"] = operation_id
    kwargs["job_id"] = job_id
    return execute_command("get_job_fail_context", kwargs)

def list_operations(**kwargs):
    return execute_command("list_operations", kwargs, parse_yson=True)

def list_jobs(operation_id, **kwargs):
    kwargs["operation_id"] = operation_id
    return execute_command("list_jobs", kwargs, parse_yson=True)

def strace_job(job_id, **kwargs):
    kwargs["job_id"] = job_id
    return execute_command("strace_job", kwargs, parse_yson=True)

def signal_job(job_id, signal_name, **kwargs):
    kwargs["job_id"] = job_id
    kwargs["signal_name"] = signal_name
    execute_command("signal_job", kwargs)

def abandon_job(job_id, **kwargs):
    kwargs["job_id"] = job_id
    execute_command("abandon_job", kwargs)

def poll_job_shell(job_id, authenticated_user=None, **kwargs):
    kwargs = {"job_id": job_id, "parameters": kwargs}
    if authenticated_user:
        kwargs["authenticated_user"] = authenticated_user
    return execute_command("poll_job_shell", kwargs, parse_yson=True)

def abort_job(job_id, **kwargs):
    kwargs["job_id"] = job_id
    execute_command("abort_job", kwargs)

def interrupt_job(job_id, interrupt_timeout=10000, **kwargs):
    kwargs["job_id"] = job_id
    kwargs["interrupt_timeout"] = interrupt_timeout
    execute_command("abort_job", kwargs)

def retry_while_job_missing(func):
    result = [None]
    def check():
        try:
            result[0] = func()
            return True
        except YtError as err:
            if err.is_no_such_job():
                return False
            raise
    wait(check)
    return result[0]

def lock(path, waitable=False, **kwargs):
    kwargs["path"] = path
    kwargs["waitable"] = waitable
    return execute_command("lock", kwargs, parse_yson=True)

def unlock(path, **kwargs):
    kwargs["path"] = path
    execute_command("unlock", kwargs)

def remove(path, **kwargs):
    kwargs["path"] = path
    execute_command("remove", kwargs)

def get(path, is_raw=False, **kwargs):
    kwargs["path"] = path
    if "default" in kwargs and "verbose_error" not in kwargs:
        kwargs["verbose_error"] = False
    if "return_only_value" not in kwargs:
        kwargs["return_only_value"] = True
    try:
        return execute_command("get", kwargs, parse_yson=not is_raw, unwrap_v4_result=False)
    except YtResponseError as err:
        if err.is_resolve_error() and "default" in kwargs:
            return kwargs["default"]
        raise

def get_job(operation_id, job_id, **kwargs):
    kwargs["operation_id"] = operation_id
    kwargs["job_id"] = job_id
    return execute_command("get_job", kwargs, parse_yson=True)

def set(path, value, is_raw=False, **kwargs):
    if not is_raw:
        value = yson.dumps(value)
    kwargs["path"] = path
    execute_command("set", kwargs, input_stream=StringIO(value))

def create(object_type, path, **kwargs):
    kwargs["type"] = object_type
    kwargs["path"] = path
    return execute_command("create", kwargs, parse_yson=True)

def copy(source_path, destination_path, **kwargs):
    kwargs["source_path"] = source_path
    kwargs["destination_path"] = destination_path
    return execute_command("copy", kwargs, parse_yson=True)

def move(source_path, destination_path, **kwargs):
    kwargs["source_path"] = source_path
    kwargs["destination_path"] = destination_path
    return execute_command("move", kwargs, parse_yson=True)

def link(target_path, link_path, **kwargs):
    kwargs["target_path"] = target_path
    kwargs["link_path"] = link_path
    return execute_command("link", kwargs, parse_yson=True)

def exists(path, **kwargs):
    kwargs["path"] = path
    return execute_command("exists", kwargs, parse_yson=True)

def concatenate(source_paths, destination_path, **kwargs):
    kwargs["source_paths"] = source_paths
    kwargs["destination_path"] = destination_path
    execute_command("concatenate", kwargs)

def externalize(path, cell_tag, **kwargs):
    kwargs["path"] = path
    kwargs["cell_tag"] = cell_tag
    execute_command("externalize", kwargs)

def internalize(path, **kwargs):
    kwargs["path"] = path
    execute_command("internalize", kwargs)

def ls(path, **kwargs):
    kwargs["path"] = path
    return execute_command("list", kwargs, parse_yson=True)

def read_table(path, **kwargs):
    kwargs["path"] = path
    return execute_command_with_output_format("read_table", kwargs)

def read_blob_table(path, **kwargs):
    kwargs["path"] = path
    output = StringIO()
    execute_command("read_blob_table", kwargs, output_stream=output)
    return output.getvalue()

def write_table(path, value=None, is_raw=False, **kwargs):
    if "input_stream" in kwargs:
        input_stream = kwargs.pop("input_stream")
    else:
        assert value is not None
        if not is_raw:
            if not isinstance(value, list):
                value = [value]
            value = yson.dumps(value)
            # remove surrounding [ ]
            value = value[1:-1]
        input_stream = StringIO(value)

    attributes = {}
    if "sorted_by" in kwargs:
        attributes["sorted_by"] = flatten(kwargs["sorted_by"])
    kwargs["path"] = yson.to_yson_type(path, attributes=attributes)
    return execute_command("write_table", kwargs, input_stream=input_stream)

def tx_write_table(*args, **kwargs):
    """
    Write rows to table transactionally.

    If write_table fails with some error it is not guaranteed that table is not locked.
    Locks can linger for some time and prevent from working with this table.

    This function avoids such lingering locks by explicitly creating external transaction
    and aborting it explicitly in case of error.
    """
    parent_tx = kwargs.pop("tx", "0-0-0-0")
    timeout = kwargs.pop("timeout", 60000)

    try:
        tx = start_transaction(timeout=timeout, tx=parent_tx)
    except Exception as e:
        raise AssertionError("Cannot start transaction: {}".format(e))

    try:
        write_table(*args, tx=tx, **kwargs)
    except:
        try:
            abort_transaction(tx)
        except Exception as e:
            raise AssertionError(
                "Cannot abort wrapper transaction: {}".format(e)
            )
        raise

    commit_transaction(tx)

def locate_skynet_share(path, **kwargs):
    kwargs["path"] = path

    output = StringIO()
    execute_command("locate_skynet_share", kwargs, output_stream=output)
    return yson.loads(output.getvalue())

def select_rows(query, **kwargs):
    kwargs["query"] = query
    kwargs["verbose_logging"] = True
    return execute_command_with_output_format("select_rows", kwargs)

def explain_query(query, **kwargs):
    kwargs["query"] = query
    return execute_command_with_output_format("explain_query", kwargs)[0]

def _prepare_rows_stream(data, is_raw=False):
    # remove surrounding [ ]
    if not is_raw:
        data = yson.dumps(data, yson_type="list_fragment")
    return StringIO(data)

def insert_rows(path, data, is_raw=False, **kwargs):
    kwargs["path"] = path
    if not is_raw:
        return execute_command("insert_rows", kwargs, input_stream=_prepare_rows_stream(data))
    else:
        return execute_command("insert_rows", kwargs, input_stream=StringIO(data))

def lock_rows(path, data, **kwargs):
    kwargs["path"] = path
    return execute_command("lock_rows", kwargs, input_stream=_prepare_rows_stream(data))

def delete_rows(path, data, **kwargs):
    kwargs["path"] = path
    return execute_command("delete_rows", kwargs, input_stream=_prepare_rows_stream(data))

def trim_rows(path, tablet_index, trimmed_row_count, **kwargs):
    kwargs["path"] = path
    kwargs["tablet_index"] = tablet_index
    kwargs["trimmed_row_count"] = trimmed_row_count
    return execute_command_with_output_format("trim_rows", kwargs)

def lookup_rows(path, data, **kwargs):
    kwargs["path"] = path
    return execute_command_with_output_format("lookup_rows", kwargs, input_stream=_prepare_rows_stream(data))

def get_in_sync_replicas(path, data, **kwargs):
    kwargs["path"] = path
    return execute_command("get_in_sync_replicas", kwargs, input_stream=_prepare_rows_stream(data), parse_yson=True)

def get_table_pivot_keys(path, **kwargs):
    kwargs["path"] = path
    return execute_command("get_table_pivot_keys", kwargs, parse_yson=True)

def get_tablet_infos(path, tablet_indexes, **kwargs):
    kwargs["path"] = path
    kwargs["tablet_indexes"] = tablet_indexes
    return execute_command("get_tablet_infos", kwargs, parse_yson=True, unwrap_v4_result=False)

def start_transaction(**kwargs):
    return execute_command("start_tx", kwargs, parse_yson=True)

def commit_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    execute_command("commit_tx", kwargs)

def ping_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    execute_command("ping_tx", kwargs)

def abort_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    execute_command("abort_tx", kwargs)

def abort_all_transactions():
    topmost_transactions = ls("//sys/topmost_transactions")
    for i in xrange(len(topmost_transactions) / 100 + 1):
        start = i * 100
        end = min(len(topmost_transactions), (i + 1) * 100)
        if start >= end:
            break
        requests = []
        for j in xrange(start, end):
            requests.append({"command": "abort_transaction", "parameters": {"transaction_id": topmost_transactions[j]}})
        execute_batch(requests)

def generate_timestamp(**kwargs):
    return execute_command("generate_timestamp", kwargs, parse_yson=True)

def mount_table(path, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    return execute_command("mount_table", kwargs)

def unmount_table(path, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    return execute_command("unmount_table", kwargs)

def remount_table(path, **kwargs):
    kwargs["path"] = path
    return execute_command("remount_table", kwargs)

def freeze_table(path, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    return execute_command("freeze_table", kwargs)

def unfreeze_table(path, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    return execute_command("unfreeze_table", kwargs)

def reshard_table(path, arg=None, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    assert arg is not None
    if isinstance(arg, int):
        kwargs["tablet_count"] = arg
    else:
        kwargs["pivot_keys"] = arg
    return execute_command("reshard_table", kwargs)

def reshard_table_automatic(path, **kwargs):
    clear_metadata_caches(kwargs.get("driver"))
    kwargs["path"] = path
    return execute_command("reshard_table_automatic", kwargs, parse_yson=True)

def alter_table(path, **kwargs):
    kwargs["path"] = path
    return execute_command("alter_table", kwargs)

def balance_tablet_cells(bundle, tables=None, **kwargs):
    kwargs["bundle"] = bundle
    if tables is not None:
        kwargs["tables"] = tables
    return execute_command("balance_tablet_cells", kwargs, parse_yson=True)

def write_file(path, data, **kwargs):
    kwargs["path"] = path

    if "input_stream" in kwargs:
        assert data is None
        input_stream = kwargs["input_stream"]
        del kwargs["input_stream"]
    else:
        input_stream = StringIO(data)

    return execute_command("write_file", kwargs, input_stream=input_stream)

def write_local_file(path, file_name, **kwargs):
    with open(file_name, "rt") as f:
        return write_file(path, f.read(), **kwargs)

def read_file(path, **kwargs):
    kwargs["path"] = path
    output = StringIO()
    execute_command("read_file", kwargs, output_stream=output)
    return output.getvalue();

def read_journal(path, **kwargs):
    kwargs["path"] = path
    kwargs["output_format"] = yson.loads("yson")
    output = StringIO()
    execute_command("read_journal", kwargs, output_stream=output)
    return list(yson.loads(output.getvalue(), yson_type="list_fragment"))

def write_journal(path, value, is_raw=False, **kwargs):
    kwargs["path"] = path

    if "input_stream" in kwargs:
        assert value is None
        input_stream = kwargs["input_stream"]
        del kwargs["input_stream"]
    else:
        if not isinstance(value, list):
            value = [value]
        value = yson.dumps(value)
        # remove surrounding [ ]
        value = value[1:-1]
        input_stream = StringIO(value)

    return execute_command("write_journal", kwargs, input_stream=input_stream)

def make_batch_request(command_name, input=None, **kwargs):
    request = dict()
    request["command"] = command_name
    request["parameters"] = kwargs
    if input is not None:
        request["input"] = input
    return request

def execute_batch(requests, **kwargs):
    kwargs["requests"] = requests
    return execute_command("execute_batch", kwargs, parse_yson=True)

def get_batch_error(result):
    if "error" in result:
        return result["error"]
    else:
        return None

def get_batch_output(result):
    if "error" in result:
        raise YtResponseError(result["error"])
    if "output" in result:
        return result["output"]
    return None

def check_permission(user, permission, path, **kwargs):
    kwargs["user"] = user
    kwargs["permission"] = permission
    kwargs["path"] = path
    return execute_command("check_permission", kwargs, parse_yson=True, unwrap_v4_result=False)

def check_permission_by_acl(user, permission, acl, **kwargs):
    kwargs["user"] = user
    kwargs["permission"] = permission
    kwargs["acl"] = acl
    return execute_command("check_permission_by_acl", kwargs, parse_yson=True, unwrap_v4_result=False)

def get_file_from_cache(md5, cache_path, **kwargs):
    kwargs["md5"] = md5
    kwargs["cache_path"] = cache_path
    return execute_command("get_file_from_cache", kwargs, parse_yson=True)

def put_file_to_cache(path, md5, **kwargs):
    kwargs["path"] = path
    kwargs["md5"] = md5
    return execute_command("put_file_to_cache", kwargs, parse_yson=True)

def discover_proxies(type_, **kwargs):
    kwargs["type"] = type_
    return execute_command("discover_proxies", kwargs, parse_yson=True)

###########################################################################

def reset_events_on_fs():
    global _events_on_fs
    _events_on_fs = None

def events_on_fs():
    global _events_on_fs
    if _events_on_fs is None:
        _events_on_fs = JobEvents(create_tmpdir("eventdir"))
    return _events_on_fs

def with_breakpoint(cmd, breakpoint_name="default"):
    if "BREAKPOINT" not in cmd:
        raise ValueError("Command doesn't have BREAKPOINT: {0}".format(cmd))
    result = cmd.replace("BREAKPOINT", events_on_fs().breakpoint_cmd(breakpoint_name), 1)
    if "BREAKPOINT" in result:
        raise ValueError("Command has multiple BREAKPOINT: {0}".format(cmd))
    return result

def wait_breakpoint(*args, **kwargs):
    return events_on_fs().wait_breakpoint(*args, **kwargs)

def release_breakpoint(*args, **kwargs):
    return events_on_fs().release_breakpoint(*args, **kwargs)

###########################################################################

def get_operation_cypress_path(op_id):
    return "//sys/operations/{}/{}".format("%02x" % (long(op_id.split("-")[3], 16) % 256), op_id)

def get_cypress_metrics(operation_id, key, aggr="sum"):
    statistics = get(get_operation_cypress_path(operation_id) + "/@progress/job_statistics")
    return sum(filter(lambda x: x is not None,
                      [get_statistics(statistics, "{0}.$.{1}.map.{2}".format(key, job_state, aggr))
                       for job_state in ("completed", "failed", "aborted")]))

##################################################################

class Operation(object):
    def __init__(self):
        self._tmpdir = ""
        self._poll_frequency = 0.1

    def get_path(self):
        return get_operation_cypress_path(self.id)

    def get_node(self, job_id):
        job_path = "//sys/scheduler/orchid/scheduler/jobs/{0}".format(job_id)
        return get(job_path + "/address", verbose=False)

    def get_job_phase(self, job_id):
        job_phase_path = "//sys/cluster_nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}/job_phase".format(self.get_node(job_id), job_id)
        return get(job_phase_path, verbose=False)

    def ensure_running(self, timeout=10.0):
        print_debug("Waiting for operation %s to become running" % self.id)

        state = self.get_runtime_state(verbose=False)
        while state != "running" and timeout > 0:
            time.sleep(self._poll_frequency)
            timeout -= self._poll_frequency
            state = self.get_state(verbose=False)

        if state != "running":
            raise TimeoutError("Operation didn't become running within timeout")

        wait(lambda: self.get_state() == "running", sleep_backoff=self._poll_frequency, iter=(1 + int(timeout / self._poll_frequency)))

    def get_job_count(self, state, from_orchid=True):
        if from_orchid:
            base_path = self.get_path() + "/controller_orchid/progress/jobs/"
        else:
            base_path = self.get_path() + "/@progress/jobs/"

        try:
            path = base_path + str(state)
            if state == "aborted" or state == "completed":
                path += "/total"
            return get(path, verbose=False)
        except YtError as err:
            if not err.is_resolve_error():
                raise
            return 0

    def get_running_jobs(self):
        jobs_path = self.get_path() + "/controller_orchid/running_jobs"
        return get(jobs_path, verbose=False, default={})

    def get_runtime_state(self, **kwargs):
        return get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(self.id), **kwargs)

    def get_state(self, **kwargs):
        try:
            return get(self.get_path() + "/@state", verbose_error=False, **kwargs)
        except YtResponseError as err:
            if not err.is_resolve_error():
                raise

        return get(self.get_path() + "/@state", **kwargs)

    def wait_for_state(self, state, **kwargs):
        wait(lambda: self.get_state(**kwargs) == state)

    def wait_fresh_snapshot(self, timepoint=None):
        if timepoint is None:
            timepoint = datetime.utcnow()
        snapshot_path = self.get_path() + "/snapshot"
        wait(lambda: exists(snapshot_path) and date_string_to_datetime(get(snapshot_path + "/@creation_time")) > timepoint)

    def get_alerts(self):
        try:
            return get(self.get_path() + "/@alerts")
        except YtResponseError as err:
            if err.is_resolve_error():
                return {}
            raise

    def read_stderr(self, job_id):
        return read_file(self.get_path() + "/jobs/{}/stderr".format(job_id))

    def get_error(self):
        state = self.get_state(verbose=False)
        if state == "failed":
            error = get(self.get_path() + "/@result/error", verbose=False)
            jobs_path = self.get_path() + "/jobs"
            jobs = get(jobs_path, verbose=False)
            job_errors = []
            for job in jobs:
                job_error_path = jobs_path + "/{0}/@error".format(job)
                job_stderr_path = jobs_path + "/{0}/stderr".format(job)
                if exists(job_error_path, verbose=False):
                    job_error = get(job_error_path, verbose=False)
                    message = job_error["message"]
                    if "stderr" in jobs[job]:
                        message = message + "\n" + read_file(job_stderr_path, verbose=False)
                    job_errors.append(YtError(message=message,
                                              code=job_error.get("code", 1),
                                              attributes=job_error.get("attributes"),
                                              inner_errors=job_error.get("inner_errors")))
            inner_errors = error.get("inner_errors", [])
            if len(job_errors) > 0:
                inner_errors.append(YtError(message="Some of the jobs have failed", inner_errors=job_errors))

            return YtError(message=error["message"],
                           code=error.get("code"),
                           attributes=error["attributes"],
                           inner_errors=inner_errors)
        if state == "aborted":
            return YtError(message="Operation {0} aborted".format(self.id))

    def build_progress(self):
        try:
            progress = get(self.get_path() + "/@brief_progress/jobs", verbose=False, verbose_error=False)
        except YtError:
            return "(brief progress is not available yet)"

        result = {}
        for job_type in progress:
            if isinstance(progress[job_type], dict):
                result[job_type] = progress[job_type]["total"]
            else:
                result[job_type] = progress[job_type]
        return "({0})".format(", ".join("{0}={1}".format(key, result[key]) for key in result))

    def track(self, raise_on_failed=True, raise_on_aborted=True):
        counter = 0
        while True:
            state = self.get_state(verbose=False)
            message = "Operation {0} {1}".format(self.id, state)
            if counter % 30 == 0 or state in ["failed", "aborted", "completed"]:
                print_debug(message)
                if state == "running":
                    print_debug(self.build_progress())
                    print_debug()
                else:
                    print_debug()
            if state == "failed" and raise_on_failed:
                raise self.get_error()
            if state == "aborted" and raise_on_aborted:
                raise self.get_error()
            if state in ["failed", "aborted", "completed"]:
                break
            counter += 1
            time.sleep(self._poll_frequency)

    def abort(self, wait_until_finished=False, **kwargs):
        abort_op(self.id, **kwargs)
        if wait_until_finished:
            self.wait_for_state("aborted")

    def complete(self, wait_until_finished=False, **kwargs):
        complete_op(self.id, **kwargs)
        if wait_until_finished:
            self.wait_for_state("completed")

    def suspend(self, **kwargs):
        suspend_op(self.id, **kwargs)

    def resume(self, **kwargs):
        resume_op(self.id, **kwargs)

def create_tmpdir(prefix):
    basedir = os.path.join(path_to_run_tests, "tmp")
    try:
        if not os.path.exists(basedir):
            os.mkdir(basedir)
    except OSError:
        sys.excepthook(*sys.exc_info())

    tmpdir = tempfile.mkdtemp(
        prefix="{0}_{1}_".format(prefix, os.getpid()),
        dir=basedir)
    # Give full access to tmpdir, it must be accessible from user jobs
    # to implement waitable jobs.
    os.chmod(tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return tmpdir


def start_op(op_type, **kwargs):
    op_name = None
    if op_type == "map":
        op_name = "mapper"
    if op_type == "reduce" or op_type == "join_reduce":
        op_name = "reducer"

    input_name = None
    if op_type != "erase" and op_type != "vanilla":
        kwargs["in_"] = prepare_paths(kwargs["in_"])
        input_name = "input_table_paths"

    output_name = None
    if "out" in kwargs:
        if op_type in ["map", "reduce", "join_reduce", "map_reduce"]:
            kwargs["out"] = prepare_paths(kwargs["out"])
            output_name = "output_table_paths"
        else:
            kwargs["out"] = prepare_path(kwargs["out"])
            output_name = "output_table_path"

    if "file" in kwargs:
        kwargs["file"] = prepare_paths(kwargs["file"])

    for opt in ["sort_by", "reduce_by", "join_by"]:
        flat(kwargs, opt)

    operation = Operation()

    change(kwargs, "table_path", ["spec", "table_path"])
    change(kwargs, "in_", ["spec", input_name])
    change(kwargs, "out", ["spec", output_name])
    change(kwargs, "command", ["spec", op_name, "command"])
    change(kwargs, "file", ["spec", op_name, "file_paths"])
    change(kwargs, "sort_by", ["spec", "sort_by"])
    change(kwargs, "reduce_by", ["spec", "reduce_by"])
    change(kwargs, "join_by", ["spec", "join_by"])
    change(kwargs, "mapper_file", ["spec", "mapper", "file_paths"])
    change(kwargs, "reduce_combiner_file", ["spec", "reduce_combiner", "file_paths"])
    change(kwargs, "reducer_file", ["spec", "reducer", "file_paths"])
    change(kwargs, "mapper_command", ["spec", "mapper", "command"])
    change(kwargs, "reduce_combiner_command", ["spec", "reduce_combiner", "command"])
    change(kwargs, "reducer_command", ["spec", "reducer", "command"])

    track = kwargs.get("track", True)
    if "track" in kwargs:
        del kwargs["track"]

    kwargs["operation_type"] = op_type

    operation.id = execute_command("start_op", kwargs, parse_yson=True)

    if track:
        operation.track()

    return operation


def operation_nodes_exist():
    for entry in ls("//sys/operations"):
        if len(entry) != 2:
            return True
        if ls("//sys/operations/" + entry):
            return True
    return False


def clean_operations():
    cleaner_path = "//sys/scheduler/config/operations_cleaner"
    try:
        set(cleaner_path + "/enable", True, recursive=True)
        wait(lambda: not operation_nodes_exist())
    finally:
        set(cleaner_path + "/enable", False)


def resolve_operation_id_or_alias(command):
    def resolved_command(op_id_or_alias, **kwargs):
        if op_id_or_alias.startswith("*"):
            kwargs["operation_alias"] = op_id_or_alias
        else:
            kwargs["operation_id"] = op_id_or_alias
        return command(**kwargs)

    return resolved_command

@resolve_operation_id_or_alias
def abort_op(**kwargs):
    execute_command("abort_op", kwargs)

@resolve_operation_id_or_alias
def suspend_op(**kwargs):
    execute_command("suspend_op", kwargs)

@resolve_operation_id_or_alias
def complete_op(**kwargs):
    execute_command("complete_op", kwargs)

@resolve_operation_id_or_alias
def resume_op(**kwargs):
    execute_command("resume_op", kwargs)

@resolve_operation_id_or_alias
def get_operation(is_raw=False, **kwargs):
    result = execute_command("get_operation", kwargs)
    return result if is_raw else yson.loads(result)

@resolve_operation_id_or_alias
def update_op_parameters(**kwargs):
    execute_command("update_op_parameters", kwargs)

def map(**kwargs):
    change(kwargs, "ordered", ["spec", "ordered"])
    return start_op("map", **kwargs)

def merge(**kwargs):
    flat(kwargs, "merge_by")
    for opt in ["combine_chunks", "merge_by", "mode"]:
        change(kwargs, opt, ["spec", opt])
    return start_op("merge", **kwargs)

def reduce(**kwargs):
    return start_op("reduce", **kwargs)

def join_reduce(**kwargs):
    return start_op("join_reduce", **kwargs)

def map_reduce(**kwargs):
    return start_op("map_reduce", **kwargs)

def vanilla(**kwargs):
    return start_op("vanilla", **kwargs)

def erase(path, **kwargs):
    kwargs["table_path"] = path
    change(kwargs, "combine_chunks", ["spec", "combine_chunks"])
    return start_op("erase", **kwargs)

def sort(**kwargs):
    return start_op("sort", **kwargs)

def remote_copy(**kwargs):
    return start_op("remote_copy", **kwargs)

def build_snapshot(*args, **kwargs):
    return get_driver().build_snapshot(*args, **kwargs)

def build_master_snapshots(*args, **kwargs):
    return get_driver().build_master_snapshots(*args, **kwargs)

def get_version():
    return execute_command("get_version", {}, parse_yson=True)

def gc_collect(driver=None):
    _get_driver(driver=driver).gc_collect()

def clear_metadata_caches(driver=None):
    _get_driver(driver=driver).clear_metadata_caches()


def create_account(name, parent_name=None, empty=False, **kwargs):
    sync = kwargs.pop('sync_creation', True)
    kwargs["type"] = "account"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    if parent_name is not None:
        kwargs["attributes"]["parent_name"] = parent_name
    if not empty and "resource_limits" not in kwargs["attributes"]:
        kwargs["attributes"]["resource_limits"] = {
            "disk_space_per_medium": {"default" : 2 ** 30},
            "chunk_count": 100000,
            "node_count": 1000,
            "tablet_count": 0,
            "tablet_static_memory": 0,
            "master_memory": 100000
        }
    execute_command("create", kwargs)
    if sync:
        wait(lambda: get("//sys/accounts/{0}/@life_stage".format(name)) == 'creation_committed')

def remove_account(name, **kwargs):
    gc_collect(kwargs.get("driver"))
    sync = kwargs.pop('sync_deletion', True)
    account_path = "//sys/accounts/" + name
    remove(account_path, **kwargs)
    if sync:
        wait(lambda: not exists(account_path))

def create_pool_tree(name, wait_for_orchid=True, **kwargs):
    kwargs["type"] = "scheduler_pool_tree"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs, parse_yson=True)
    if wait_for_orchid:
        wait(lambda: exists(scheduler_orchid_pool_tree_path(name)))

def remove_pool_tree(name, wait_for_orchid=True, **kwargs):
    remove("//sys/pool_trees/" + name, **kwargs)
    if wait_for_orchid:
        wait(lambda: not exists(scheduler_orchid_pool_tree_path(name)))

def create_pool(name, pool_tree="default", parent_name=None, **kwargs):
    kwargs["type"] = "scheduler_pool"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    kwargs["attributes"]["pool_tree"] = pool_tree
    if parent_name:
        kwargs["attributes"]["parent_name"] = parent_name
    execute_command("create", kwargs, parse_yson=True)

def create_user(name, **kwargs):
    kwargs["type"] = "user"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)
    wait(lambda: get("//sys/users/{0}/@life_stage".format(name)) == "creation_committed")

def make_random_string(length=10):
    return "".join(random.choice(string.letters) for _ in xrange(length))

def create_test_tables(row_count=1, **kwargs):
    create("table", "//tmp/t_in", **kwargs)
    write_table("//tmp/t_in", [{"x": str(i)} for i in xrange(row_count)])
    create("table", "//tmp/t_out", **kwargs)

def run_test_vanilla(command, spec=None, job_count=1, track=False, task_patch=None, **kwargs):
    spec = spec or {}
    spec["tasks"] = {
        "task": update({"job_count": job_count, "command": command}, task_patch)
    }
    return vanilla(spec=spec, track=track, **kwargs)

def run_sleeping_vanilla(**kwargs):
    return run_test_vanilla("sleep 1000", **kwargs)

def enable_op_detailed_logs(op):
    update_op_parameters(op.id, parameters={
        "scheduling_options_per_pool_tree": {
            "default": {
                "enable_detailed_logs": True,
            }
        }
    })

def remove_user(name, **kwargs):
    remove("//sys/users/" + name, **kwargs)

def create_group(name, **kwargs):
    kwargs["type"] = "group"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)
    wait(lambda: get("//sys/groups/{0}/@life_stage".format(name)) == "creation_committed")

def remove_group(name, **kwargs):
    remove("//sys/groups/" + name, **kwargs)

def add_member(member, group, **kwargs):
    kwargs["member"] = member
    kwargs["group"] = group
    execute_command("add_member", kwargs)

def remove_member(member, group, **kwargs):
    kwargs["member"] = member
    kwargs["group"] = group
    execute_command("remove_member", kwargs)

def create_network_project(name, **kwargs):
    kwargs["type"] = "network_project"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)

def remove_network_project(name, **kwargs):
    remove("//sys/network_projects/" + name, **kwargs)

def create_tablet_cell(**kwargs):
    kwargs["type"] = "tablet_cell"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    return execute_command("create", kwargs, parse_yson=True)

def create_tablet_cell_bundle(name, initialize_options=True, **kwargs):
    kwargs["type"] = "tablet_cell_bundle"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    if initialize_options:
        if "options" not in kwargs["attributes"]:
            kwargs["attributes"]["options"] = dict()
        for option in ("changelog_account", "snapshot_account"):
            if option not in kwargs["attributes"]["options"]:
                kwargs["attributes"]["options"][option] = "sys"
    execute_command("create", kwargs)
    wait(lambda: get("//sys/tablet_cell_bundles/{0}/@life_stage".format(name)) == "creation_committed")

def remove_tablet_cell_bundle(name, driver=None):
    remove("//sys/tablet_cell_bundles/" + name, driver=driver)

def remove_tablet_cell(id, driver=None):
    remove("//sys/tablet_cells/" + id, driver=driver, force=True)

def sync_remove_tablet_cells(cells, driver=None):
    cells = __builtin__.set(cells)
    for id in cells:
        remove_tablet_cell(id, driver=driver)
    wait(lambda: len(cells.intersection(__builtin__.set(get("//sys/tablet_cells", driver=driver)))) == 0)

def remove_tablet_action(id, driver=None):
    remove("#" + id, driver=driver)

def create_table_replica(table_path, cluster_name, replica_path, **kwargs):
    kwargs["type"] = "table_replica"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["table_path"] = table_path
    kwargs["attributes"]["cluster_name"] = cluster_name
    kwargs["attributes"]["replica_path"] = replica_path
    return execute_command("create", kwargs, parse_yson=True)

def remove_table_replica(replica_id):
    remove("#{0}".format(replica_id))

def alter_table_replica(replica_id, **kwargs):
    kwargs["replica_id"] = replica_id
    execute_command("alter_table_replica", kwargs)

def create_data_center(name, **kwargs):
    kwargs["type"] = "data_center"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)

def remove_data_center(name, **kwargs):
    remove("//sys/data_centers/" + name, **kwargs)

def create_rack(name, **kwargs):
    kwargs["type"] = "rack"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)

def remove_rack(name, **kwargs):
    remove("//sys/racks/" + name, **kwargs)

def create_medium(name, **kwargs):
    kwargs["type"] = "medium"
    if "attributes" not in kwargs:
        kwargs["attributes"] = dict()
    kwargs["attributes"]["name"] = name
    execute_command("create", kwargs)

#########################################
# Helpers:

def get_transactions(driver=None):
    gc_collect(driver=driver)
    return ls("//sys/transactions", driver=driver)

def get_topmost_transactions(driver=None):
    gc_collect(driver=driver)
    return ls("//sys/topmost_transactions", driver=driver)

def get_chunks(driver=None):
    gc_collect(driver=driver)
    return ls("//sys/chunks", driver=driver)

def get_accounts(driver=None):
    return ls("//sys/accounts", driver=driver)

def get_users(driver=None):
    return ls("//sys/users", driver=driver)

def get_groups(driver=None):
    return ls("//sys/groups", driver=driver)

def get_tablet_cells(driver=None):
    return ls("//sys/tablet_cells", driver=driver)

def get_tablet_actions(driver=None):
    return ls("//sys/tablet_actions", driver=driver)

def get_data_centers(driver=None):
    return ls("//sys/data_centers", driver=driver)

def get_racks(driver=None):
    return ls("//sys/racks", driver=driver)

def get_nodes(driver=None):
    return ls("//sys/cluster_nodes", driver=driver)

def get_media(driver=None):
    return ls("//sys/media", driver=driver)

def get_chunk_owner_disk_space(path, *args, **kwargs):
    disk_space = get("{0}/@resource_usage/disk_space_per_medium".format(path), *args, **kwargs)
    return disk_space.get("default", 0)

def get_recursive_disk_space(path):
    disk_space = get("{0}/@recursive_resource_usage/disk_space_per_medium".format(path))
    return disk_space.get("default", 0)

def get_account_disk_space(account):
    disk_space = get("//sys/accounts/{0}/@resource_usage/disk_space_per_medium".format(account))
    return disk_space.get("default", 0)

def get_account_committed_disk_space(account):
    disk_space = get("//sys/accounts/{0}/@committed_resource_usage/disk_space_per_medium".format(account))
    return disk_space.get("default", 0)

def get_account_disk_space_limit(account, medium="default"):
    disk_space = get("//sys/accounts/{0}/@resource_limits/disk_space_per_medium".format(account))
    return disk_space.get(medium, 0)

def set_account_disk_space_limit(account, limit, medium="default"):
    set("//sys/accounts/{0}/@resource_limits/disk_space_per_medium/{1}".format(account, medium), limit)

def get_chunk_replication_factor(chunk_id):
    return get("#{0}/@media/default/replication_factor".format(chunk_id))

def make_ace(action, subjects, permissions, inheritance_mode="object_and_descendants", columns=None):
    def _to_list(x):
        if isinstance(x, str):
            return [x]
        else:
            return x

    ace = {
        "action": action,
        "subjects": _to_list(subjects),
        "permissions": _to_list(permissions),
        "inheritance_mode": inheritance_mode
    }
    if columns is not None:
        ace["columns"] = _to_list(columns)
    return ace

#########################################

def make_column(name, type_v3, **attributes):
    result = {
        "name": name,
        "type_v3": type_v3,
    }
    for k in attributes:
        result[k] = attributes[k]
    return result

def make_sorted_column(name, type_v3, **attributes):
    return make_column(name, type_v3, sort_order="ascending", **attributes)

def make_schema(columns, **attributes):
    schema = yson.YsonList()
    for column_schema in columns:
        column_schema = column_schema.copy()
        schema.append(column_schema)
    for attr, value in attributes.items():
        schema.attributes[attr] = value
    return schema

def normalize_schema(schema):
    """Remove 'type_v2' / 'type_v3' field from schema, useful for schema comparison."""
    result = pycopy.deepcopy(schema)
    for column in result:
        column.pop("type_v2", None)
        column.pop("type_v3", None)
    return result

def normalize_schema_v3(schema):
    """Remove "type" / "required" / "type_v2" fields from schema, useful for schema comparison."""
    result = pycopy.deepcopy(schema)
    for column in result:
        for f in ["type", "required", "type_v2"]:
            if f in column:
                del column[f]
    return result

def optional_type(element_type):
    return {
        "type_name": "optional",
        "item": element_type,
    }

def make_struct_members(fields):
    result = []
    for name, type in fields:
        result.append({
            "name": name,
            "type": type,
        })
    return result

def make_tuple_elements(elements):
    result = []
    for type in elements:
        result.append({
            "type": type,
        })
    return result

def struct_type(fields):
    """
    Create yson description of struct type.
    fields is a list of (name, type) pairs.
    """
    result = {
        "type_name": "struct",
        "members": make_struct_members(fields),
    }
    return result

def list_type(element_type):
    return {
        "type_name": "list",
        "item": element_type,
    }

def tuple_type(elements):
    return {
        "type_name": "tuple",
        "elements": make_tuple_elements(elements),
    }

def variant_struct_type(fields):
    result = {
        "type_name": "variant",
        "members": make_struct_members(fields),
    }
    return result

def variant_tuple_type(elements):
    return {
        "type_name": "variant",
        "elements": make_tuple_elements(elements)
    }

def dict_type(key_type, value_type):
    return {
        "type_name": "dict",
        "key": key_type,
        "value": value_type,
    }

def tagged_type(tag, element_type):
    return {
        "type_name": "tagged",
        "tag": tag,
        "item": element_type,
    }

##################################################################

def get_guid_from_parts(lo, hi):
    assert 0 <= lo < 2 ** 64
    assert 0 <= hi < 2 ** 64
    ints = [0, 0, 0, 0]
    ints[0] = hi & 0xFFFFFFFF
    ints[1] = hi >> 32
    ints[2] = lo & 0xFFFFFFFF
    ints[3] = lo >> 32
    return "{3:x}-{2:x}-{1:x}-{0:x}".format(*ints)

def generate_uuid(generator=None):
    def get_int():
        return hex(random.randint(0, 2**32 - 1))[2:].rstrip("L")
    return "-".join([get_int() for _ in xrange(4)])

##################################################################

def get_statistics(statistics, complex_key):
    result = statistics
    for part in complex_key.split("."):
        if part:
            if part not in result:
                return None
            result = result[part]
    return result

##################################################################

def check_all_stderrs(op, expected_content, expected_count, substring=False):
    jobs_path = op.get_path() + "/jobs"
    assert get(jobs_path + "/@count") == expected_count
    for job_id in ls(jobs_path):
        stderr_path = "{0}/{1}/stderr".format(jobs_path, job_id)
        content = read_file(stderr_path)
        assert get(stderr_path + "/@uncompressed_data_size") == len(content)
        if substring:
            assert expected_content in content
        else:
            assert content == expected_content

##################################################################

def set_banned_flag(value, nodes=None):
    if value:
        flag = True
        expected_state = "offline"
    else:
        flag = False
        expected_state = "online"

    if not nodes:
        nodes = ls("//sys/cluster_nodes")

    for address in nodes:
        set("//sys/cluster_nodes/{0}/@banned".format(address), flag)

    def check():
        for address in nodes:
            if get("//sys/cluster_nodes/{0}/@state".format(address)) != expected_state:
                return False
        return True
    wait(check)

##################################################################

class PrepareTables(object):
    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")

##################################################################

def set_node_banned(address, flag, driver=None):
    set("//sys/cluster_nodes/%s/@banned" % address, flag, driver=driver)
    ban, state = ("banned", "offline") if flag else ("unbanned", "online")
    print_debug("Waiting for node %s to become %s..." % (address, ban))
    wait(lambda: get("//sys/cluster_nodes/%s/@state" % address, driver=driver) == state)

def set_node_decommissioned(address, flag, driver=None):
    set("//sys/cluster_nodes/%s/@decommissioned" % address, flag, driver=driver)
    print_debug("Node %s is %s" % (address, "decommissioned" if flag else "not decommissioned"))

def wait_for_nodes(driver=None):
    print_debug("Waiting for nodes to become online...")
    wait(lambda: all(n.attributes["state"] == "online"
                     for n in ls("//sys/cluster_nodes", attributes=["state"], driver=driver)))

def wait_for_chunk_replicator(driver=None):
    print_debug("Waiting for chunk replicator to become enabled...")
    wait(lambda: get("//sys/@chunk_replicator_enabled", driver=driver))

def get_cluster_drivers(primary_driver=None):
    if primary_driver is None:
        return [drivers_by_cell_tag[default_api_version] for drivers_by_cell_tag in clusters_drivers["primary"]]
    for drivers in clusters_drivers.values():
        if drivers[0][default_api_version] == primary_driver:
            return [drivers_by_cell_tag[default_api_version] for drivers_by_cell_tag in drivers]
    raise "Failed to get cluster drivers"

def wait_for_cells(cell_ids=None, driver=None):
    print_debug("Waiting for tablet cells to become healthy...")

    def get_cells(driver):
        cells = ls("//sys/tablet_cells", attributes=["health", "id", "peers"], driver=driver)
        if cell_ids is None:
            return cells
        return [cell for cell in cells if cell.attributes["id"] in cell_ids]

    def check_orchid():
        cells = get_cells(driver=driver)
        for cell in cells:
            peer = cell.attributes["peers"][0]
            if "address" not in peer:
                return False
            node = peer["address"]
            try:
                if not exists("//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}".format(node, cell.attributes["id"]), driver=driver):
                    return False
            except YtResponseError:
                return False
        return True
    wait(check_orchid)

    def check_cells(driver):
        cells = get_cells(driver=driver)
        for cell in cells:
            if cell.attributes["health"] != "good":
                return False
        return True
    for driver in get_cluster_drivers(driver):
        wait(lambda: check_cells(driver=driver))

def sync_create_cells(cell_count, tablet_cell_bundle="default", driver=None):
    cell_ids = []
    for _ in xrange(cell_count):
        cell_id = create_tablet_cell(attributes={
            "tablet_cell_bundle": tablet_cell_bundle
        }, driver=driver)
        cell_ids.append(cell_id)
    wait_for_cells(cell_ids, driver=driver)
    return cell_ids

def wait_until_sealed(path, driver=None):
    wait(lambda: get(path + "/@sealed", driver=driver))

def truncate_journal(path, row_count, **kwargs):
    kwargs["path"] = path
    kwargs["row_count"] = row_count
    return execute_command("truncate_journal", kwargs)

def wait_for_tablet_state(path, state, **kwargs):
    print_debug("Waiting for tablets to become %s..." % (state))
    driver = kwargs.pop("driver", None)
    if kwargs.get("first_tablet_index", None) == None and kwargs.get("last_tablet_index", None) == None:
        wait(lambda: get(path + "/@tablet_state", driver=driver) == state)
    else:
        tablet_count = get(path + "/@tablet_count", driver=driver)
        first_tablet_index = kwargs.get("first_tablet_index", 0)
        last_tablet_index = kwargs.get("last_tablet_index", tablet_count - 1)
        wait(lambda: all(x["state"] == state for x in
             get(path + "/@tablets", driver=driver)[first_tablet_index:last_tablet_index + 1]))
        wait(lambda: get(path + "/@tablet_state") != "transient")

def sync_mount_table(path, freeze=False, **kwargs):
    mount_table(path, freeze=freeze, **kwargs)
    wait_for_tablet_state(path, "frozen" if freeze else "mounted", **kwargs)

def sync_unmount_table(path, **kwargs):
    unmount_table(path, **kwargs)
    wait_for_tablet_state(path, "unmounted", **kwargs)

def sync_freeze_table(path, **kwargs):
    freeze_table(path, **kwargs)
    wait_for_tablet_state(path, "frozen", **kwargs)

def sync_unfreeze_table(path, **kwargs):
    unfreeze_table(path, **kwargs)
    wait_for_tablet_state(path, "mounted", **kwargs)

def sync_reshard_table(path, *args, **kwargs):
    reshard_table(path, *args, **kwargs)
    wait(lambda: get(path + "/@tablet_state") != "transient")

def _wait_for_tablet_actions(action_ids):
    wait(lambda: all(
        get("//sys/tablet_actions/{}/@state".format(action_id)) in ("completed", "failed")
        for action_id in action_ids))

def sync_reshard_table_automatic(path, **kwargs):
    kwargs["keep_actions"] = True
    rsp = reshard_table_automatic(path, **kwargs)
    _wait_for_tablet_actions(rsp)

def sync_balance_tablet_cells(bundle, tables=None, **kwargs):
    kwargs["keep_actions"] = True
    rsp = balance_tablet_cells(bundle, tables, **kwargs)
    _wait_for_tablet_actions(rsp)

def sync_flush_table(path, driver=None):
    sync_freeze_table(path, driver=driver)
    sync_unfreeze_table(path, driver=driver)

def sync_compact_table(path, driver=None):
    chunk_ids = __builtin__.set(get(path + "/@chunk_ids", driver=driver))
    sync_unmount_table(path, driver=driver)
    set(path + "/@forced_compaction_revision", 1, driver=driver)
    sync_mount_table(path, driver=driver)

    print_debug("Waiting for tablets to become compacted...")
    wait(lambda: len(chunk_ids.intersection(__builtin__.set(get(path + "/@chunk_ids", driver=driver)))) == 0)

    print_debug("Waiting for tablets to become stable...")
    def check_stable():
        chunk_ids1 = get(path + "/@chunk_ids", driver=driver)
        time.sleep(3.0)
        chunk_ids2 = get(path + "/@chunk_ids", driver=driver)
        return chunk_ids1 == chunk_ids2
    wait(lambda: check_stable())

def sync_enable_table_replica(replica_id, driver=None):
    alter_table_replica(replica_id, enabled=True, driver=driver)

    print_debug("Waiting for replica to become enabled...")
    wait(lambda: get("#{0}/@state".format(replica_id), driver=driver) == "enabled")

def sync_disable_table_replica(replica_id, driver=None):
    alter_table_replica(replica_id, enabled=False, driver=driver)

    print_debug("Waiting for replica to become disabled...")
    wait(lambda: get("#{0}/@state".format(replica_id), driver=driver) == "disabled")

def get_tablet_leader_address(tablet_id):
    cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
    peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
    leader_peer = list(x for x in peers if x["state"] == "leading")[0]
    return leader_peer["address"]

def get_tablet_follower_addresses(tablet_id):
    cell_id = get("//sys/tablets/" + tablet_id + "/@cell_id")
    peers = get("//sys/tablet_cells/" + cell_id + "/@peers")
    follower_peers = list(x for x in peers if x["state"] == "following")
    return [peer["address"] for peer in follower_peers]

def sync_alter_table_replica_mode(replica_id, mode, driver=None):
    alter_table_replica(replica_id, mode=mode, driver = driver)

    print_debug("Waiting for replica mode to become {}...".format(mode))
    tablets = get("#{}/@tablets".format(replica_id), driver=driver)
    tablet_ids = [x["tablet_id"] for x in tablets]
    cell_ids = [get("#{}/@cell_id".format(tablet_id), driver=driver) for tablet_id in tablet_ids]
    addresses = [get_tablet_leader_address(tablet_id) for tablet_id in tablet_ids]
    def check():
        for tablet_id, cell_id, address in zip(tablet_ids, cell_ids, addresses):
            actual_mode = get("//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}/replicas/{}/mode".format(address, cell_id, tablet_id, replica_id), driver=driver)
            if actual_mode != mode:
                return False
        return True
    wait(check)

def create_dynamic_table(path, **attributes):
    if "dynamic" not in attributes:
        attributes.update({"dynamic": True})

    is_sorted = "schema" in attributes and any(column.get("sort_order") == "ascending" for column in attributes["schema"])
    if is_sorted and "enable_dynamic_store_read" not in attributes:
        attributes.update({"enable_dynamic_store_read": True})

    create("table", path, attributes=attributes)

def sync_control_chunk_replicator(enabled):
    print_debug("Setting chunk replicator state to", enabled)
    set("//sys/@config/chunk_manager/enable_chunk_replicator", enabled, recursive=True)
    wait(lambda: all(get("//sys/@chunk_replicator_enabled", driver=drivers_by_cell_tag[default_api_version]) == enabled
                     for drivers_by_cell_tag in clusters_drivers["primary"]))

def get_singular_chunk_id(path, **kwargs):
    chunk_ids = get(path + "/@chunk_ids", **kwargs)
    assert len(chunk_ids) == 1
    return chunk_ids[0]

def get_first_chunk_id(path, **kwargs):
    return get(path + "/@chunk_ids/0", **kwargs)

def get_job_count_profiling():
    start_time = datetime.now()

    job_count = {"state": defaultdict(int), "abort_reason": defaultdict(int)}

    try:
        profiling_response = get("//sys/scheduler/orchid/profiling/scheduler/job_count", verbose=False)
    except YtError:
        return job_count

    profiling_info = {}
    for value in reversed(profiling_response):
        key = tuple(sorted(value["tags"].items()))
        if key not in profiling_info:
            profiling_info[key] = value["value"]

    # Enable it for debugging.
    # print "profiling_info:", profiling_info
    for key, value in profiling_info.iteritems():
        state = dict(key)["state"]
        job_count["state"][state] += value

    for key, value in profiling_info.iteritems():
        state = dict(key)["state"]
        if state != "aborted":
            continue
        abort_reason = dict(key)["abort_reason"]
        job_count["abort_reason"][abort_reason] += value

    duration = (datetime.now() - start_time).total_seconds()

    # Enable it for debugging.
    print_debug("job_counters (take {} seconds to calculate): {}".format(duration, job_count))

    return job_count

def scheduler_orchid_path():
    return "//sys/scheduler/orchid"

def scheduler_orchid_pool_tree_path(tree):
    return scheduler_orchid_path() + "/scheduler/scheduling_info_per_pool_tree/{}/fair_share_info".format(tree)

def scheduler_orchid_default_pool_tree_path():
    return scheduler_orchid_pool_tree_path("default")

def get_applied_node_dynamic_config(node):
    return get("//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/config".format(node))

# Implements config.update(new_config) for dynamic nodes config and waits for config apply
# assuming the only nodes config filter is `%true`.
def update_nodes_dynamic_config(new_config):
    current_config = get("//sys/cluster_nodes/@config")
    current_config["%true"].update(new_config)
    set("//sys/cluster_nodes/@config", current_config)

    for node in ls("//sys/cluster_nodes"):
        wait(lambda: get_applied_node_dynamic_config(node) == current_config["%true"])
