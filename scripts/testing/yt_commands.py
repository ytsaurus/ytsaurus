import subprocess

import sys
#TODO:get rid of it
sys.path.append('../yson')

import yson_parser
import yson

import os

###########################################################################

YT = "yt"

###########################################################################

class YTError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

###########################################################################

def send_data(process, data=None):
    stdout, stderr = process.communicate(data)
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
    print 'yt ' + ' '.join(quote(s) for s in all_args)

    process = subprocess.Popen([YT] + all_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE)
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

def read_str(path, **kw):
    return command('read', path, **kw)

def write_str(path, value, **kw):
    return command('write', path, value, **kw)

def start_transaction(**kw):
    raw_tx = command('start_tx', **kw)
    tx_id = raw_tx.replace('"', '').strip('\n')
    return tx_id

def commit_transaction(**kw):
    return command('commit_tx', **kw)

def renew_transaction(*args, **kw):
    return command('renew_tx', *args, **kw)

def abort_transaction(**kw):
    return command('abort_tx', **kw)

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

def track_op(**kw):
    if "op" in kw:
        kw["op"] = kw["op"].strip("\"\'")
    return command('track_op', **kw)

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
    return yson_parser.parse_list_fragment(yson)

def yson2py(yson):
    return yson_parser.parse_string(yson)

def py2yson(py):
    return yson.dumps(py, indent='')


def get_transactions(**kw):
    yson_map = get_str('//sys/transactions', **kw)
    return yson2py(yson_map).keys()

#########################################
