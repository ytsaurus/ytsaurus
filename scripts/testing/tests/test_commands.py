from yt_env_setup import YTEnvSetup
from yt_env_util import command, exec_cmd

def start_transaction():
    tx_id = exec_cmd(['start']).replace('"', '').strip('\n')
    return tx_id

def commit_transaction(tx_id):
    exec_cmd(['commit', '--tx', tx_id])

def abort_transaction(tx_id):
    exec_cmd(['abort', '--tx', tx_id])

def expect_error(stdout, stderr, exitcode):
    assert exitcode != 0
    print stderr
    return stderr

def expect_ok(stdout, stderr, exitcode):
    assert exitcode == 0
    assert stderr == ''
    print stdout
    return stdout

def lock(path, **kw):
    return command('lock', path, **kw)

def get(path, **kw):
    return command('get', path, **kw)

def set(path, value, **kw):
    return command('set', path, value, **kw)


class TestTxCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 0

    def test_simple(self):
        tx_id = start_transaction()
        
        #TODO(panin): check that transaction is on the master
        #print exec_cmd(['get', '/sys/transactions/'])
        
        commit_transaction(tx_id)


class TestLockCommands(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_HOLDERS = 0


    def test_invalid_cases(self):
        pass
        # # outside of transaction
        # expect_error(lock('/'))

        # # at non-existsing node
        # tx_id = start_transaction()
        # exec_cmd(['get', '/sys/transactions/'])
        
        # expect_error(lock('/non_existent', tx = tx_id))
        
        # exec_cmd(['get', '/sys/transactions/'])

        # exec_cmd(get('/', tx = tx_id))

        # # error while parsing mode
        # expect_error(lock('/', mode = 'invalid', tx = tx_id))

        # #taking None lock is forbidden
        # expect_error(lock('/', mode = 'None', tx = tx_id))
       
        # abort_transaction(tx_id)

    def test_display_lock(self):
        tx_id = start_transaction()
        
        expect_ok(*set('/list', '[1; 2; 3]', tx = tx_id))


        abort_transaction(tx_id)

