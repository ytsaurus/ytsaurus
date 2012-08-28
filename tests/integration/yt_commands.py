import yson as yson_lib

import os
import sys
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

def command(name, *args, **kw):
    process = run_command(name, *args, **kw)
    return send_data(process)

def convert_to_yt_args(*args, **kw):
    all_args = list(args)
    for k, v in kw.items():
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

def run_command(name, *args, **kw):
    all_args = [name] + convert_to_yt_args(*args, **kw)
    debug_string =  'yt ' + ' '.join(quote(s) for s in all_args)
    print debug_string

    process = subprocess.Popen(['yt'] + all_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE)

    debug_info[process.pid] = debug_string
    return process    




###########################################################################

def lock(path, **kw):
    return command('lock', path, **kw)

def get_str(path, **kw):
    return command('get', path, **kw)

def remove(path, **kw):
    return command('remove', path, **kw)

def set_str(path, value, **kw):
    return command('set', path, value, **kw)

def ls_str(path, **kw):
    return command('list', path, **kw)

def create(object_type, path, **kw):
    return command('create', object_type, path, **kw)

def copy(source_path, destination_path, **kw):
    return command('copy', source_path, destination_path, **kw)

def read_str(path, **kw):
    return command('read', path, **kw)

def write_str(path, value, **kw):
    return command('write', path, value, **kw)

def start_transaction(**kw):
    raw_tx = command('start_tx', **kw)
    tx_id = raw_tx.replace('"', '').strip('\n')
    return tx_id

def commit_transaction(tx, **kw):
    return command('commit_tx', tx, **kw)

def renew_transaction(tx, **kw):
    return command('renew_tx', tx, **kw)

def abort_transaction(tx, **kw):
    return command('abort_tx', tx, **kw)

def upload(path, data, **kw): 
    process =  run_command('upload', path, **kw)
    return send_data(process, data)

def upload_file(path, file_name, **kw):
    with open(file_name, 'rt') as f:
        return upload(path, f.read(), **kw)

def download(path, **kw):
    return command('download', path, **kw)

def map(*args, **kw):
    return command('map', *args, **kw)

def merge(*args, **kw):
    return command('merge', *args, **kw)

def reduce(*args, **kw):
    return command('reduce', *args, **kw)

def map_reduce(*args, **kw):
    return command('map_reduce', *args, **kw)

def track_op(op, **kw):
    return command('track_op', op, **kw)

def erase(path, *args, **kw):
    return command('erase', path, *args, **kw)

def sort(**kw):
    return command('sort', **kw)

#########################################

def get(path, **kw):
    return yson2py(get_str(path, **kw))

def ls(path, **kw):
    return yson2py(ls_str(path, **kw))

def set(path, value, **kw):
    return set_str(path, py2yson(value), **kw)

def read(path, **kw):
    return table2py(read_str(path, **kw))

def write(path, value, **kw):
    output = py2yson(value)
    if isinstance(value, list):
        output = output[1:-1] # remove surrounding [ ]
    return write_str(path, output, **kw)

#########################################

# Helpers:

def table2py(yson):
    return yson_lib.yson_parser.parse_list_fragment(yson)

def yson2py(yson):
    return yson_lib.yson_parser.parse_string(yson)

def py2yson(py):
    return yson_lib.yson.dumps(py, indent='')

def get_transactions(**kw):
    yson_map = get_str('//sys/transactions', **kw)
    return yson2py(yson_map).keys()

#########################################
