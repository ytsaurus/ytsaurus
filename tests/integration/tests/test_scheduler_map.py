import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

#TODO(panin): refactor
def check_all_stderrs(op_id, expected):
    jobs_path = '//sys/operations/' + op_id + '/jobs'
    for job_id in ls(jobs_path):
        download(jobs_path + '/' + job_id + '/stderr')

class TestSchedulerMapCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    START_SCHEDULER = True

    def test_empty_table(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        map(in_='//tmp/t1', out='//tmp/t2', command='cat')

        assert read('//tmp/t2') == []

    def test_one_chunk(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write_str('//tmp/t1', '{a=b}')
        map(in_='//tmp/t1', out='//tmp/t2', command='cat')

        assert read('//tmp/t2') == [{'a' : 'b'}]

    def test_in_equal_to_out(self):
        create('table', '//tmp/t1')
        write_str('//tmp/t1', '{foo=bar}')

        map(in_='//tmp/t1', out='//tmp/t1', command='cat')

        assert read('//tmp/t1') == [{'foo': 'bar'}, {'foo': 'bar'}]

    # check that stderr is captured for successfull job
    def test_stderr_ok(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write_str('//tmp/t1', '{foo=bar}')

        command = '''cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index='"'$YT_JOB_INDEX'"'};'''

        op_id = map('--dont_track', in_='//tmp/t1', out='//tmp/t2', command=command)
        track_op(op_id)

        a = read('//tmp/t2')
	assert len(a) == 2
	assert a[0].has_key('operation')
	assert a[0]['operation'] != ''
	assert a[1].has_key('job_index')
	assert a[1]['job_index'] != ''
	
        check_all_stderrs(op_id, 'stderr')

    # check that stderr is captured for failed jobs
    def test_stderr_failed(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write_str('//tmp/t1', '{foo=bar}')

        command = "cat > /dev/null; echo stderr 1>&2; exit 125"

        op_id = map('--dont_track', in_='//tmp/t1', out='//tmp/t2', command=command)

        # if all jobs failed then operation is also failed
        with pytest.raises(YTError): track_op(op_id)
        
        check_all_stderrs(op_id, 'stderr')

    def test_invalid_output_record(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write_str('//tmp/t1', '{key=foo;value=ninja}')

        command = "awk '($1==\"foo\"){print \"bar\"}'"

        with pytest.raises(YTError):
            map(
                in_='//tmp/t1',
                out='//tmp/t2',
                opt='/spec/mapper/format=yamr',
                command=command)

    def test_job_count(self):
        create('table', '//tmp/t1')
        for i in xrange(5):
            write_str('//tmp/t1', '{foo=bar}')

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create('table', table_name)
            map(in_='//tmp/t1',
                out=table_name,
                command=command,
                opt=['/spec/job_count=%d' % job_count,
                     '/spec/min_data_size_per_job=1'])
            assert read(table_name) == [{'hello': 'world'} for i in xrange(expected_num_records)]

        check('//tmp/t2', 3, 3)
        check('//tmp/t3', 10, 5) # number of jobs can't be more that number of chunks

    def test_with_user_files(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write_str('//tmp/t1', '{foo=bar}')

        file1 = '//tmp/some_file.txt' 
        file2 = '//tmp/renamed_file.txt' 

        upload(file1, '{value=42};\n')
        upload(file2, '{a=b};\n')

        # check attributes @file_name
        set(file2 + '/@file_name', 'my_file.txt')
        command= "cat > /dev/null; cat some_file.txt; cat my_file.txt"

        map(in_='//tmp/t1',
            out='//tmp/t2',
            command=command,
            file=[file1, file2])

        assert read('//tmp/t2') == [{'value': 42}, {'a': 'b'}]


    def test_many_output_tables(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write_str('//tmp/t_in', '{a=b}')

        mapper = \
"""
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7

"""
        upload('//tmp/mapper.sh', mapper)

        map(in_='//tmp/t_in',
            out=output_tables,
            command='bash mapper.sh',
            file='//tmp/mapper.sh')

        assert read(output_tables[0]) == [{'v': 0}]
        assert read(output_tables[1]) == [{'v': 1}]
        assert read(output_tables[2]) == [{'v': 2}]

    def test_tskv_input_format(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{foo=bar}')

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt='/spec/mapper/input_format=<line_prefix=tskv>dsv')

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    def test_tskv_output_format(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{foo=bar}')

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "tskv" + "\\t" + "hello=world"

"""
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt=[ \
                '/spec/mapper/input_format=<format=text>yson',
                '/spec/mapper/output_format=<line_prefix=tskv>dsv'])

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    def test_yamr_output_format(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{foo=bar}')

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "key\\tsubkey\\tvalue"

"""
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt=[ \
                '/spec/mapper/input_format=<format=text>yson',
                '/spec/mapper/output_format=<has_subkey=true>yamr'])

        assert read('//tmp/t_out') == [{'key': 'key', 'subkey': 'subkey', 'value': 'value'}]

    def test_yamr_input_format(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{value=value;subkey=subkey;key=key;a=another}')

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt='/spec/mapper/input_format=<has_subkey=true>yamr')

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    def test_executable_mapper(self):
        create('table', '//tmp/t_in')
        write_str('//tmp/t_in', '{foo=bar}')

        mapper =  \
"""
#!/bin/sh
cat > /dev/null; echo {hello=world}
"""
        upload('//tmp/mapper.sh', mapper)
        set('//tmp/mapper.sh/@executable', "true")

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="./mapper.sh",
            file='//tmp/mapper.sh')

        assert read('//tmp/t_out') == [{'hello': 'world'}]
    
    def test_abort_op(self):
        create('table', '//tmp/t')
        write_str('//tmp/t', '{foo=bar}')

        op_id = map('--dont_track',
            in_='//tmp/t', 
            out='//tmp/t',
            command="sleep 2")

        path = '//sys/operations/%s/@state' % op_id
        # check running
        abort_op(op_id)
        assert get(path) == 'aborted'



