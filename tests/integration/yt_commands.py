import yt.yson as yson
from yt.bindings.driver import Driver, Request, make_request
from yt.common import YtError, flatten, update

import pytest

import sys

import time
from datetime import datetime

from cStringIO import StringIO

only_linux = pytest.mark.skipif("not sys.platform.startswith(\"linux\")")

driver = None

def get_driver():
    return driver

def init_driver(config):
    global driver
    driver = Driver(config=config)

def set_branch(dict, path, value):
    root = dict
    for field in path[:-1]:
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
    result = yson.loads(command("parse_ypath", arguments={"path": path}, verbose=False))
    update(result.attributes, attributes)
    return result

def prepare_paths(paths):
    return [prepare_path(path) for path in flatten(paths)]

def prepare_args(arguments):
    change(arguments, "tx", "transaction_id")
    change(arguments, "attr", "attributes")
    change(arguments, "ping_ancestor_txs", "ping_ancestor_transactions")
    if "opt" in arguments:
        for option in flatten(arguments["opt"]):
            key, value = option.split("=", 1)
            set_branch(arguments, key.strip("/").split("/"), yson.loads(value))
        del arguments["opt"]

    return arguments

def command(command_name, arguments, input_stream=None, output_stream=None, verbose=None):
    if "verbose" in arguments:
        verbose = arguments["verbose"]
        del arguments["verbose"]
    verbose = verbose is None or verbose

    if "driver" in arguments:
        driver = arguments["driver"]
        del arguments["driver"]
    else:
        driver = get_driver()

    user = None
    if "user" in arguments:
        user = arguments["user"]
        del arguments["user"]

    if "path" in arguments and command_name != "parse_ypath":
        arguments["path"] = prepare_path(arguments["path"])

    arguments = prepare_args(arguments)

    if verbose:
        print >>sys.stderr, str(datetime.now()), command_name, arguments
    result = make_request(
        driver,
        Request(command_name=command_name,
                arguments=arguments,
                input_stream=input_stream,
                output_stream=output_stream,
                user=user))
    if verbose and result:
        print >>sys.stderr, result
        print >>sys.stderr
    return result

###########################################################################

def lock(path, waitable=False, **kwargs):
    kwargs["path"] = path
    kwargs["waitable"] = waitable
    return command('lock', kwargs).replace('"', '').strip('\n')

def remove(path, **kwargs):
    kwargs["path"] = path
    return command('remove', kwargs)

def get(path, is_raw=False, **kwargs):
    def has_arg(name):
        return name in kwargs and kwargs[name] is not None

    kwargs["path"] = path
    result = command('get', kwargs)
    return result if is_raw else yson.loads(result)

def set(path, value, is_raw=False, **kwargs):
    if not is_raw:
        value = yson.dumps(value)
    kwargs["path"] = path
    return command('set', kwargs, input_stream=StringIO(value))

def create(object_type, path, **kwargs):
    kwargs["type"] = object_type
    kwargs["path"] = path
    return command('create', kwargs)

def copy(source_path, destination_path, **kwargs):
    kwargs["source_path"] = source_path
    kwargs["destination_path"] = destination_path
    return command('copy', kwargs)

def move(source_path, destination_path, **kwargs):
    kwargs["source_path"] = source_path
    kwargs["destination_path"] = destination_path
    return command('move', kwargs)

def link(target_path, link_path, **kwargs):
    kwargs["target_path"] = target_path
    kwargs["link_path"] = link_path
    return command('link', kwargs)

def exists(path, **kwargs):
    kwargs["path"] = path
    res = command('exists', kwargs)
    return yson.loads(res) == 'true'

def ls(path, **kwargs):
    kwargs["path"] = path
    return yson.loads(command('list', kwargs))

def execute_command_with_output_format(command_name, kwargs):
    has_output_format = "output_format" in kwargs
    if not has_output_format:
        kwargs["output_format"] = yson.loads("<format=text>yson")
    output = StringIO()
    command(command_name, kwargs, output_stream=output)
    if not has_output_format:
        return list(yson.loads(output.getvalue(), yson_type="list_fragment"))
    else:
        return output.getvalue()

def read(path, **kwargs):
    kwargs["path"] = path
    return execute_command_with_output_format("read_table", kwargs)

def write(path, value, is_raw=False, **kwargs):
    if not is_raw:
        if not isinstance(value, list):
            value = [value]
        value = yson.dumps(value)
        # remove surrounding [ ]
        value = value[1:-1]

    attributes = {}
    if "sorted_by" in kwargs:
        attributes={"sorted_by": flatten(kwargs["sorted_by"])}
    kwargs["path"] = yson.to_yson_type(path, attributes=attributes)
    return command("write_table", kwargs, input_stream=StringIO(value))

def select(query, **kwargs):
    kwargs["query"] = query
    return execute_command_with_output_format("select", kwargs)

def start_transaction(**kwargs):
    out = command('start_tx', kwargs)
    return out.replace('"', '').strip('\n')

def commit_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    return command('commit_tx', kwargs)

def ping_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    return command('ping_tx', kwargs)

def abort_transaction(tx, **kwargs):
    kwargs["transaction_id"] = tx
    return command('abort_tx', kwargs)

def mount_table(path, **kwargs):
    kwargs["path"] = path
    return command('mount_table', kwargs)

def unmount_table(path, **kwargs):
    kwargs["path"] = path
    return command('unmount_table', kwargs)

def remount_table(path, **kwargs):
    kwargs["path"] = path
    return command('remount_table', kwargs)

def reshard_table(path, pivot_keys, **kwargs):
    kwargs["path"] = path
    kwargs["pivot_keys"] = pivot_keys
    return command('reshard_table', kwargs)

def upload(path, data, **kwargs):
    kwargs["path"] = path
    return command('write_file', kwargs, input_stream=StringIO(data))

def upload_file(path, file_name, **kwargs):
    with open(file_name, 'rt') as f:
        return upload(path, f.read(), **kwargs)

def download(path, **kwargs):
    kwargs["path"] = path
    output = StringIO()
    command('read_file', kwargs, output_stream=output)
    return output.getvalue();

def read_journal(path, **kwargs):
    kwargs["path"] = path
    kwargs["output_format"] = yson.loads("yson")
    output = StringIO()
    command("read_journal", kwargs, output_stream=output)
    return list(yson.loads(output.getvalue(), yson_type="list_fragment"))

def write_journal(path, value, is_raw=False, **kwargs):
    if not isinstance(value, list):
        value = [value]
    value = yson.dumps(value)
    # remove surrounding [ ]
    value = value[1:-1]
    kwargs["path"] = path
    return command('write_journal', kwargs, input_stream=StringIO(value))

def track_op(op_id):
    counter = 0
    while True:
        state = get("//sys/operations/%s/@state" % op_id, verbose=False)
        message = "Operation %s %s" % (op_id, state)
        if counter % 30 == 0 or state in ["failed", "aborted", "completed"]:
            print >>sys.stderr, message
        if state == "failed":
            error = get("//sys/operations/%s/@result/error" % op_id, verbose=False, is_raw=True)
            jobs = get("//sys/operations/%s/jobs" % op_id, verbose=False)
            for job in jobs:
                if exists("//sys/operations/%s/jobs/%s/@error" % (op_id, job), verbose=False):
                    error = error + "\n\n" + get("//sys/operations/%s/jobs/%s/@error" % (op_id, job), verbose=False, is_raw=True)
                    if "stderr" in jobs[job]:
                        error = error + "\n" + download("//sys/operations/%s/jobs/%s/stderr" % (op_id, job), verbose=False)
            raise YtError(error)
        if state == "aborted":
            raise YtError(message)
        if state == "completed":
            break
        counter += 1
        time.sleep(0.1)

def start_op(op_type, **kwargs):
    op_name = None
    if op_type == "map":
        op_name = "mapper"
    if op_type == "reduce":
        op_name = "reducer"

    input_name = None
    if op_type != "erase":
        kwargs["in_"] = prepare_paths(kwargs["in_"])
        input_name = "input_table_paths"

    output_name = None
    if op_type in ["map", "reduce", "map_reduce"]:
        kwargs["out"] = prepare_paths(kwargs["out"])
        output_name = "output_table_paths"
    elif "out" in kwargs:
        kwargs["out"] = prepare_path(kwargs["out"])
        output_name = "output_table_path"

    if "file" in kwargs:
        kwargs["file"] = prepare_paths(kwargs["file"])

    for opt in ["sort_by", "reduce_by"]:
        flat(kwargs, opt)

    change(kwargs, "table_path", ["spec", "table_path"])
    change(kwargs, "in_", ["spec", input_name])
    change(kwargs, "out", ["spec", output_name])
    change(kwargs, "command", ["spec", op_name, "command"])
    change(kwargs, "file", ["spec", op_name, "file_paths"])
    change(kwargs, "sort_by", ["spec","sort_by"])
    change(kwargs, "reduce_by", ["spec","reduce_by"])
    change(kwargs, "mapper_file", ["spec", "mapper", "file_paths"])
    change(kwargs, "reduce_combiner_file", ["spec", "reduce_combiner", "file_paths"])
    change(kwargs, "reducer_file", ["spec", "reducer", "file_paths"])
    change(kwargs, "mapper_command", ["spec", "mapper", "command"])
    change(kwargs, "reduce_combiner_command", ["spec", "reduce_combiner", "command"])
    change(kwargs, "reducer_command", ["spec", "reducer", "command"])

    track = not kwargs.get("dont_track", False)
    if "dont_track" in kwargs:
        del kwargs["dont_track"]

    op_id = command(op_type, kwargs).strip().replace('"', '')

    if track:
        track_op(op_id)

    return op_id

def map(**kwargs):
    return start_op('map', **kwargs)

def merge(**kwargs):
    flat(kwargs, "merge_by")
    for opt in ["combine_chunks", "merge_by", "mode"]:
        change(kwargs, opt, ["spec", opt])
    return start_op('merge', **kwargs)

def reduce(**kwargs):
    return start_op('reduce', **kwargs)

def map_reduce(**kwargs):
    return start_op('map_reduce', **kwargs)

def erase(path, **kwargs):
    kwargs["table_path"] = path
    change(kwargs, "combine_chunks", ["spec", "combine_chunks"])
    return start_op('erase', **kwargs)

def sort(**kwargs):
    return start_op('sort', **kwargs)

def remote_copy(**kwargs):
    return start_op('remote_copy', **kwargs)

def abort_op(op, **kwargs):
    kwargs["operation_id"] = op
    command('abort_op', kwargs)

def build_snapshot(*args, **kwargs):
    get_driver().build_snapshot(*args, **kwargs)

def gc_collect():
    get_driver().gc_collect()

def create_account(name, **kwargs):
    kwargs["type"] = "account"
    kwargs["attributes"] = {'name': name}
    command('create', kwargs)

def remove_account(name, **kwargs):
    remove('//sys/accounts/' + name, **kwargs)
    gc_collect()

def create_user(name, **kwargs):
    kwargs["type"] = "user"
    kwargs["attributes"] = {'name': name}
    command('create', kwargs)

def remove_user(name, **kwargs):
    remove('//sys/users/' + name, **kwargs)
    gc_collect()

def create_group(name, **kwargs):
    kwargs["type"] = "group"
    kwargs["attributes"] = {'name': name}
    command('create', kwargs)

def remove_group(name, **kwargs):
    remove('//sys/groups/' + name, **kwargs)
    gc_collect()

def add_member(member, group, **kwargs):
    kwargs["member"] = member
    kwargs["group"] = group
    command('add_member', kwargs)

def remove_member(member, group, **kwargs):
    kwargs["member"] = member
    kwargs["group"] = group
    command('remove_member', kwargs)

def create_tablet_cell(size):
    return yson.loads(command('create', {'type': 'tablet_cell', 'attributes': {'size': size}}))

def remove_tablet_cell(id):
    remove('//sys/tablet_cells/' + id)
    gc_collect()

#########################################
# Helpers:

def get_transactions():
    gc_collect()
    return ls('//sys/transactions')

def get_topmost_transactions():
    gc_collect()
    return ls('//sys/topmost_transactions')

def get_chunks():
    gc_collect()
    return ls('//sys/chunks')

def get_accounts():
    gc_collect()
    return ls('//sys/accounts')

def get_users():
    gc_collect()
    return ls('//sys/users')

def get_groups():
    gc_collect()
    return ls('//sys/groups')

#########################################
