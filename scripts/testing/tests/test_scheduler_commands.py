import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

#TODO(panin): refactor
def check_all_stderrs(op_id, expected):
    jobs_path = '//sys/operations/' + op_id + '/jobs'
    for job_id in yson2py(ls(jobs_path)):
        download(jobs_path + '/"' + job_id + '"/stderr')

class TestSchedulerMapCommands(YTEnvSetup):
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

    # check that stderr is captured for successfull job
    def test_map_stderr_ok(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{foo=bar}')

        mapper = "cat > /dev/null; echo stderr 1>&2"

        op_id = map('--dont_track', input='//tmp/t1', out='//tmp/t2', mapper=mapper)
        track_op(op=op_id)
        check_all_stderrs(op_id, 'stderr')

    # check that stderr is captured for failed jobs
    def test_map_stderr_failed(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{foo=bar}')

        mapper = "cat > /dev/null; echo stderr 1>&2; exit 125"

        op_id = map('--dont_track', input='//tmp/t1', out='//tmp/t2', mapper=mapper)
        track_op(op=op_id)
        check_all_stderrs(op_id, 'stderr')

    def test_map_job_count(self):
        create('table', '//tmp/t1')
        for i in xrange(5):
            write('//tmp/t1', '{foo=bar}')

        mapper = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create('table', table_name)
            map(input='//tmp/t1',
                out=table_name,
                mapper=mapper,
                opt='/spec/job_count=%d' % job_count)
            assert read_table(table_name) == [{'hello': 'world'} for i in xrange(expected_num_records)]

        check('//tmp/t2', 3, 3)
        check('//tmp/t3', 10, 5) # number of jobs can't be more that number of chunks

    def test_map_with_user_files(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', '{foo=bar}')

        file1 = '//tmp/some_file.txt' 
        file2 = '//tmp/renamed_file.txt' 

        upload(file1, '{value=42};\n')
        upload(file2, '{a=b};\n')

        # check attributes @file_name
        set(file2 + '/@file_name', 'my_file.txt')
        mapper = "cat > /dev/null; cat some_file.txt; cat my_file.txt"

        map(input='//tmp/t1',
            out='//tmp/t2',
            mapper=mapper,
            file=[file1, file2])

        assert read_table('//tmp/t2') == [{'value': 42}, {'a': 'b'}]


    def test_map_many_output_tables(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write('//tmp/t_in', '{a=b}')

        mapper = \
"""
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7

"""
        upload('//tmp/mapper.sh', mapper)

        map(input='//tmp/t_in',
            out=output_tables,
            mapper='bash mapper.sh',
            file='//tmp/mapper.sh')

        assert read_table(output_tables[0]) == [{'v': 0}]
        assert read_table(output_tables[1]) == [{'v': 1}]
        assert read_table(output_tables[2]) == [{'v': 2}]


class TestSchedulerMergeCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_HOLDERS = 5
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        t1 = '//tmp/t1'
        create('table', t1)
        v1 = [{'key' + str(i) : 'value' + str(i)} for i in xrange(3)]
        for v in v1:
            write_py(t1, v)

        t2 = '//tmp/t2'
        create('table', t2)
        v2 = [{'another_key' + str(i) : 'another_value' + str(i)} for i in xrange(4)]
        for v in v2:
            write_py(t2, v)

        self.t1 = t1
        self.t2 = t2
        
        self.v1 = v1
        self.v2 = v2
        

    # usual cases
    def test_merge_unordered(self):
        self._prepare_tables()

        merge(input=[self.t1, self.t2], 
              out='//tmp/t_out')
        
        assertItemsEqual(read_table('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == '7'

    def test_merge_unordered_combine(self):
        self._prepare_tables()

        merge('--combine_chunks',
              input=[self.t1, self.t2],
              out='//tmp/t_out')

        assertItemsEqual(read_table('//tmp/t_out'), self.v1 + self.v2)
        assert get('//tmp/t_out/@chunk_count') == '1'







