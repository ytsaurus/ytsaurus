import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

#TODO(panin): refactor
def get_stderr(op_id):
    jobs_path = '//sys/operations/' + op_id + '/jobs'
    job_id = yson2py(ls(jobs_path))[0]
    return download(jobs_path + '/"' + job_id + '"/stderr')

class TestSchedulerCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

    def test_map_empty_table(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        map(input='//tmp/t1', out='//tmp/t2', mapper='cat')

        assert read_table('//tmp/t2') == []

    def test_map_one_chunk(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{a=b}')
        map(input='//tmp/t1', out='//tmp/t2', mapper='cat')

        assert read_table('//tmp/t2') == [{'a' : 'b'}]

    def test_map_input_equal_to_output(self):
        create('table', '//tmp/t1')
        write('//tmp/t1', '{foo=bar}')

        map(input='//tmp/t1', out='//tmp/t1', mapper='cat')

        assert read_table('//tmp/t1') == [{'foo': 'bar'}, {'foo': 'bar'}]

    def test_map_stderr_ok(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{foo=bar}')

        mapper = "cat > /dev/null; echo stderr 1>&2"

        op_id = map(dont_track=None, input='//tmp/t1', out='//tmp/t2', mapper=mapper)
        track_op(op=op_id)
        assert get_stderr(op_id) == 'stderr'

    def test_map_stderr_failed(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{foo=bar}')

        mapper = "cat > /dev/null; echo stderr 1>&2; exit(125)"

        map(input='//tmp/t1', out='//tmp/t2', mapper=mapper)
        assert get_stderr(op_id) == 'stderr'


    def test_map_many_output_tables(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write('//tmp/t_in', '{a=b}')

        mapper = \
"""
echo {v = 1} >&1
echo {v = 2} >&4
echo {v = 3} >&7
cat  /dev/stdin > /dev/null

"""
        upload('//tmp/mapper.sh', mapper)

        map(input='//tmp/t_in', 
            out=output_tables,
            mapper='"bash mapper.sh"',
            file='//tmp/mapper.sh')
