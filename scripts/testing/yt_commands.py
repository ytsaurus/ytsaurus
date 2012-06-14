import subprocess

import yson_parser
import yson

YT = "yt"

###########################################################################

class YTError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

###########################################################################

def command(name, *args, **kw):
    process = run_command(name, *args, **kw)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print 'XXX:', stderr
        raise YTError(stderr)
    print stdout
    return stdout.strip('\n')

def convert_to_yt_args(*args, **kw):
    all_args = list(args)
    for k, v in kw.items():
        # workaround to deal with in as keyword
        if k == 'input': k = 'in'
        all_args.extend(['--' + k, v])
    return all_args

def run_command(name, *args, **kw):
    all_args = [name] + convert_to_yt_args(*args, **kw)
    print ' '.join(all_args)

    process = subprocess.Popen([YT] + all_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process    


###########################################################################

def lock(path, **kw): return command('lock', path, **kw)
def get(path, **kw): return command('get', path, **kw)
def remove(path, **kw): return command('remove', path, **kw)

#TODO(panin): think of better name
def set(path, value, **kw): return command('set', path, value, **kw)

def ls(path, **kw): return command('list', path, **kw)

def create(object_type, path, **kw): return command('create', object_type, path, **kw)
def read(path, **kw): return command('read', path, **kw)
def write(path, value, **kw): return command('write', path, value, **kw)

def start_transaction(**kw):
    raw_tx = command('start_tx', **kw)
    tx_id = raw_tx.replace('"', '').strip('\n')
    return tx_id

def commit_transaction(**kw): return command('commit_tx', **kw)
def renew_transaction(**kw): return command('renew_tx', **kw)
def abort_transaction(**kw): return command('abort_tx', **kw)

def upload(path, **kw): return command('upload', path, **kw)

def map(**kw): return command('map', **kw)

#########################################

#helpers:

def table2py(yson):
    return yson_parser.parse_list_fragment(yson)

def yson2py(yson):
    return yson_parser.parse_string(yson)

#TODO(panin): maybe rename to read_py?
def read_table(path, **kw):
    return table2py(read(path, **kw))

def get_py(path, **kw):
    return yson2py(get(path, **kw))

def get_transactions(**kw):
    yson_map = get('//sys/transactions', **kw)
    return yson2py(yson_map).keys()

#########################################

#testing helpers:

def assertItemsEqual(a, b):
    assert sorted(a) == sorted(b)
