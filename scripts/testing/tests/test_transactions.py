import pytest

from yt_env_setup import YTEnvSetup
from yt_env_util import *

@pytest.mark.xfail(run = False, reason = 'Pipes, obviously, suck.')
class TestTransactions(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 0
    SETUP_TIMEOUT = 3

    def test_abort(self):
        yt = launch_yt()

        execute_cmd('{do=set; path = "//root"; value={a=100;c=10}}', yt)

        execute_cmd('{do=start_transaction}', yt)

        execute_cmd('{do=set; path = "//root/b"; value=200}', yt)
        assert do_get("//root", yt) == '{"a"=100;"b"=200;"c"=10}'

        execute_cmd('{do=set; path = "//root/a"; value=0}', yt)
        assert do_get("//root", yt) == '{"a"=0;"b"=200;"c"=10}'

        execute_cmd('{do=remove; path = "//root/c"}', yt)
        assert do_get("//root", yt) == '{"a"=0;"b"=200}'

        execute_cmd('{do=abort_transaction}', yt)
        assert do_get("//root", yt) == '{"a"=100;"c"=10}'

        execute_cmd('{do = remove; path = "//root"}', yt)

    def test_isolation(self):
        yt1 = launch_yt()
        yt2 = launch_yt()

        execute_cmd('{do=set; path = "//root"; value={a=3}}', yt2)

        execute_cmd('{do=start_transaction}', yt1)

        execute_cmd('{do=set; path = "//root/a"; value = 42}', yt1)

        assert do_get("//root/a@lock_mode", yt1) == '"exclusive"'
        assert do_get("//root/a@lock_mode", yt2) == '"none"'

        # TODO: check error message
        execute_error_cmd('{do=set; path = "//root/a"; value = 100}', yt2)

        assert do_get("//root", yt1) == '{"a"=42}'
        assert do_get("//root", yt2) == '{"a"=3}'

        execute_cmd('{do=commit_transaction}', yt1)

        assert do_get("//root/a@lock_mode", yt1) == '"none"'

        assert do_get("//root", yt1) == '{"a"=42}'
        assert do_get("//root", yt2) == '{"a"=42}'

        execute_cmd('{do = remove; path = "//root"}', yt1)


#  TODO: remake using decorator
@pytest.mark.xfail(run = False, reason = 'Pipes, obviously, suck.')
class TestTransactions2(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    SETUP_TIMEOUT = 7


    def test_merge_tables(self):
        #raw_input('press any key')

        yt1 = launch_yt()
        yt2 = launch_yt()


        # raw_input('press any key')

        execute_cmd('{do=start_transaction}', yt1)

        execute_cmd('{do=create; path = "//table"; type = table}', yt1)

        write('{do=write; path = "//table"; value = [{a=111; b = 222}]}', yt1)

        #yt2 = launch_yt()
        # execute_cmd(yt2, '{do=start_transaction}')
        # yt1.stdin.write('{do=write; path = "//table"; value = [{a=111; b = 222}]}\n')
        # print yt1.communicate('{do=commit_transaction}\n')[0]

        # execute_cmd(yt2, '{do=create; path = "//table"; type = table}')
        #execute_cmd('{do=write; path = "//table"; value = [{x=333; y = 444}]}', yt1)

        #yt1.stdin.write('{do=read; path = "//table"; stream ="> tmpfile.txt"}\n')
        #write('{do=commit_transaction}', yt1)

        output =  yt1.communicate('{do=get; path = "/"}')
        print 'output get', output
        # execute_cmd(yt1, '{do=commit_transaction}')



