import yt.yson

import subprocess
import threading

###########################################################################

class YTError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

###########################################################################


debug_info = {}
timeout = 60
shared_output = None

def communicate_with_process(process, data):
    global shared_output
    shared_output = process.communicate(data)

def send_data(process, data=None):
    t = threading.Thread(target = communicate_with_process, args=[process, data])
    t.start()
    t.join(timeout)
    if t.is_alive():
        message = 'FAIL: "{0}" --- did not finish after {1} seconds '.format(debug_info[process.pid], timeout)
        print message
        assert False, message

    stdout, stderr = shared_output

    if process.returncode != 0:
        print '!process exited with returncode:', process.returncode
        # add '!' before each line in stderr output
        print '\n'.join('!' + s for s in stderr.strip('\n').split('\n'))
        print 
        raise YTError(stderr)
    print stdout
    return stdout.strip('\n')

def command(name, *args, **kwargs):
    process = run_command(name, *args, **kwargs)
    return send_data(process)

def convert_to_yt_args(*args, **kwargs):
    all_args = list(args)
    for k, v in kwargs.items():
        # workaround to deal with 'in' as keyword
        if k == 'in_': k = 'in'

        if isinstance(v, list):
            for elem in v:
                all_args.extend(['--' + k, elem])
        else:
            all_args.extend(['--' + k, v])

    return all_args

def quote(s):
    return "'" + s + "'"

def run_command(name, *args, **kwargs):
    all_args = [name] + convert_to_yt_args(*args, **kwargs)
    debug_string =  'yt ' + ' '.join(quote(s) for s in all_args)
    print debug_string

    process = subprocess.Popen(['yt'] + all_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE)

    debug_info[process.pid] = debug_string
    return process    

###########################################################################

def lock(path, **kwargs):
    return command('lock', path, **kwargs)

def get_str(path, **kwargs):
    return command('get', path, **kwargs)

def remove(path, *args, **kwargs):
    return command('remove', path, *args, **kwargs)

def set_str(path, value, **kwargs):
    return command('set', path, value, **kwargs)

def ls_str(path, **kwargs):
    return command('list', path, **kwargs)

def create(object_type, path, *args, **kwargs):
    return command('create', object_type, path, *args, **kwargs)

def copy(source_path, destination_path, **kwargs):
    return command('copy', source_path, destination_path, **kwargs)

def move(source_path, destination_path, **kwargs):
    return command('move', source_path, destination_path, **kwargs)

def link(source_path, link_path, **kwargs):
    return command('link', source_path, link_path, **kwargs)

def exists(path, **kwargs):
    res = command('exists', path, **kwargs)
    return yson2py(res) == 'true'

def read_str(path, **kwargs):
    return command('read', path, **kwargs)

def write_str(path, value, **kwargs):
    return command('write', path, value, **kwargs)

def start_transaction(**kwargs):
    out = command('start_tx', **kwargs)
    return out.replace('"', '').strip('\n')

def commit_transaction(tx, **kwargs):
    return command('commit_tx', tx, **kwargs)

def ping_transaction(tx, *args, **kwargs):
    return command('ping_tx', tx, *args, **kwargs)

def abort_transaction(tx, **kwargs):
    return command('abort_tx', tx, **kwargs)

def upload(path, data, **kwargs): 
    process =  run_command('upload', path, **kwargs)
    return send_data(process, data)

def upload_file(path, file_name, **kwargs):
    with open(file_name, 'rt') as f:
        return upload(path, f.read(), **kwargs)

def download(path, **kwargs):
    return command('download', path, **kwargs)

def start_op(op_type, *args, **kwargs):
    out = command(op_type, *args, **kwargs)
    return out.replace('"', '').strip('\n')

def map(*args, **kwargs):
    return start_op('map', *args, **kwargs)

def merge(*args, **kwargs):
    return start_op('merge', *args, **kwargs)

def reduce(*args, **kwargs):
    return start_op('reduce', *args, **kwargs)

def map_reduce(*args, **kwargs):
    return start_op('map_reduce', *args, **kwargs)

def erase(path, *args, **kwargs):
    return start_op('erase', path, *args, **kwargs)

def sort(*args, **kwargs):
    return start_op('sort', *args, **kwargs)

def track_op(op, **kwargs):
    command('track_op', op, **kwargs)

def abort_op(op, **kwargs):
    command('abort_op', op, **kwargs)

def build_snapshot(*args, **kwargs):
    command('build_snapshot', *args, **kwargs)

def gc_collect():
    command('gc_collect')

def create_account(name):
    command('create', 'account', opt=['/attributes/name=' + name])

def remove_account(name):
    remove('//sys/accounts/' + name)
    gc_collect()

def create_user(name):
    command('create', 'user', opt=['/attributes/name=' + name])

def remove_user(name):
    remove('//sys/users/' + name)
    gc_collect()

def create_group(name):
    command('create', 'group', opt=['/attributes/name=' + name])

def remove_group(name):
    remove('//sys/groups/' + name)
    gc_collect()

def add_member(member, group):
    command('add_member', member, group)

def remove_member(member, group):
    command('remove_member', member, group)

#########################################

def get(path, **kwargs):
    return yson2py(get_str(path, **kwargs))

def ls(path, **kwargs):
    return yson2py(ls_str(path, **kwargs))

def set(path, value, **kwargs):
    return set_str(path, py2yson(value), **kwargs)

def read(path, **kwargs):
    return table2py(read_str(path, **kwargs))

def write(path, value, **kwargs):
    output = py2yson(value)
    if isinstance(value, list):
        output = output[1:-1] # remove surrounding [ ]
    return write_str(path, output, **kwargs)

#########################################
# Helpers:

def table2py(yson):
    return yt.yson.yson_parser.parse_list_fragment(yson)

def yson2py(yson):
    return yt.yson.yson_parser.parse_string(yson)

def py2yson(py):
    return yt.yson.yson.dumps(py, indent='')

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
