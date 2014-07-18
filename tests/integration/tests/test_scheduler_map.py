import pytest
import sys
import time
import __builtin__

from yt_env_setup import YTEnvSetup
from yt_commands import *


##################################################################

class TestEventLog(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        'scheduler' : {
            'enable_accounting': 'true',
            'event_log' : {
                'flush_period' : 5000
            }
        }
    }

    def test_scheduler_event_log(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', [{"a": "b"}])
        map(in_='//tmp/t1', out='//tmp/t2', command="cat; bash -c 'for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done' >/dev/null")

        # wait for scheduler to dump the event log
        time.sleep(6)
        res = read('//sys/scheduler/event_log')
        event_types = __builtin__.set()
        for item in res:
            event_types.add(item['event_type'])
            if item['event_type'] == 'job_completed':
                stats = item['statistics']
                for key in ['cpu', 'block_io']:
                    assert key in stats
                # our job should burn enough cpu
                assert int(stats['cpu']['user_time']) > 0
        assert {"operation_started"}.issubset(event_types)


class TestSchedulerMapCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def test_empty_table(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        map(in_='//tmp/t1', out='//tmp/t2', command='cat')

        assert read('//tmp/t2') == []

    @only_linux
    def test_one_chunk(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', {"a": "b"})
        op_id = map(dont_track=True,
            in_='//tmp/t1', out='//tmp/t2', command=r'cat; echo "{v1=\"$V1\"};{v2=\"$V2\"}"',
            opt=['/spec/mapper/environment={V1="Some data";V2="$(SandboxPath)/mytmp"}',
                 '/spec/title=MyTitle'])

        get('//sys/operations/%s/@spec' % op_id)
        track_op(op_id)

        res = read('//tmp/t2')
        assert len(res) == 3
        assert res[0] == {'a' : 'b'}
        assert res[1] == {'v1' : 'Some data'}
        assert res[2].has_key('v2')
        assert res[2]['v2'].endswith("/mytmp")
        assert res[2]['v2'].startswith("/")

    def test_big_input(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')

        count = 1000 * 1000
        original_data = [{'index': i} for i in xrange(count)]
        write('//tmp/t1', original_data)

        command = 'cat'
        map(in_='//tmp/t1', out='//tmp/t2', command=command)

        new_data = read('//tmp/t2', verbose=False)
        assert sorted(row.items() for row in new_data) == [[('index', i)] for i in xrange(count)]

    def test_two_inputs_at_the_same_time(self):
        create('table', '//tmp/t_input')
        create('table', '//tmp/t_output1')
        create('table', '//tmp/t_output2')

        count = 1000
        original_data = [{'index': i} for i in xrange(count)]
        write('//tmp/t_input', original_data)

        file = '//tmp/some_file.txt'
        create('file', file)
        upload(file, '{value=42};\n')

        command = 'bash -c "cat <&0 & sleep 0.1; cat some_file.txt >&4; wait;"'
        map(in_='//tmp/t_input',
            out=['//tmp/t_output1', '//tmp/t_output2'],
            command=command,
            file=[file],
            verbose=True)

        assert read('//tmp/t_output2') == [{'value': 42}]
        assert sorted([row.items() for row in read('//tmp/t_output1')]) == [[('index', i)] for i in xrange(count)]

    def test_first_after_second(self):
        create('table', '//tmp/t_input')
        create('table', '//tmp/t_output1')
        create('table', '//tmp/t_output2')

        count = 10000
        original_data = [{'index': i} for i in xrange(count)]
        write('//tmp/t_input', original_data)

        file1 = '//tmp/some_file.txt'
        create('file', file1)
        upload(file1, '}}}}};\n')

        command = 'cat some_file.txt >&4; cat >&4; echo "{value=42}"'
        op_id = map(dont_track=True,
                    in_='//tmp/t_input',
                    out=['//tmp/t_output1', '//tmp/t_output2'],
                    command=command,
                    file=[file1],
                    verbose=True)
        with pytest.raises(YtError):
            track_op(op_id)

    @only_linux
    def test_in_equal_to_out(self):
        create('table', '//tmp/t1')
        write('//tmp/t1', {"foo": "bar"})

        map(in_='//tmp/t1', out='<append=true>//tmp/t1', command='cat')

        assert read('//tmp/t1') == [{'foo': 'bar'}, {'foo': 'bar'}]

    #TODO(panin): refactor
    def _check_all_stderrs(self, op_id, expected_content, expected_count):
        jobs_path = '//sys/operations/' + op_id + '/jobs'
        assert get(jobs_path + '/@count') == expected_count
        for job_id in ls(jobs_path):
            assert download(jobs_path + '/' + job_id + '/stderr') == expected_content

    # check that stderr is captured for successfull job
    @only_linux
    def test_stderr_ok(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', {"foo": "bar"})

        command = '''cat > /dev/null; echo stderr 1>&2; echo {operation='"'$YT_OPERATION_ID'"'}';'; echo {job_index=$YT_JOB_INDEX};'''

        op_id = map(dont_track=True, in_='//tmp/t1', out='//tmp/t2', command=command)
        track_op(op_id)

        assert read('//tmp/t2') == [{'operation' : op_id}, {'job_index' : 0}]
        self._check_all_stderrs(op_id, 'stderr\n', 1)

    # check that stderr is captured for failed jobs
    @only_linux
    def test_stderr_failed(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', {"foo": "bar"})

        command = '''echo "{x=y}{v=};{a=b}"; while echo xxx 2>/dev/null; do false; done; echo stderr 1>&2; cat > /dev/null;'''

        op_id = map(dont_track=True, in_='//tmp/t1', out='//tmp/t2', command=command)
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            track_op(op_id)

        self._check_all_stderrs(op_id, 'stderr\n', 10)

    # check max_stderr_count
    @only_linux
    def test_stderr_limit(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', {"foo": "bar"})

        command = '''cat > /dev/null; echo stderr 1>&2; exit 125'''

        op_id = map(dont_track=True, in_='//tmp/t1', out='//tmp/t2', command=command, opt=['/spec/max_failed_job_count=5'])
        # if all jobs failed then operation is also failed
        with pytest.raises(YtError):
            track_op(op_id)

        self._check_all_stderrs(op_id, 'stderr\n', 5)

    @pytest.mark.skipif("not sys.platform.startswith(\"linux\")")
    def test_stderr_of_failed_jobs(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', [{"foo": "bar"} for i in xrange(110)])

        command = '''cat > /dev/null;
                     echo stderr 1>&2;
                     if [ "$YT_START_ROW_INDEX" = "109" ]; then
                         sleep 5;
                         exit 125;
                     else
                         exit 0;
                     fi;'''

        op_id = map(dont_track=True, in_='//tmp/t1', out='//tmp/t2', command=command, opt=['/spec/max_failed_job_count=1', '/spec/job_count=110'])
        with pytest.raises(YtError):
            track_op(op_id)

        # The default number of stderr is 100. We check that we have 101-st stderr of failed job,
        # that is last one.
        self._check_all_stderrs(op_id, 'stderr\n', 101)

    def test_invalid_output_record(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        write('//tmp/t1', {"key": "foo", "value": "ninja"})

        command = "awk '($1==\"foo\"){print \"bar\"}'"

        with pytest.raises(YtError):
            map(
                in_='//tmp/t1',
                out='//tmp/t2',
                opt='/spec/mapper/format=yamr',
                command=command)

    @only_linux
    def test_sorted_output(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        for i in xrange(2):
            write('<append=true>//tmp/t1', {"key": "foo", "value": "ninja"})

        command = '''cat >/dev/null; k1="$YT_JOB_INDEX"0; k2="$YT_JOB_INDEX"1; echo "{key=$k1; value=one}; {key=$k2; value=two}"'''

        map(in_='//tmp/t1',
            out='<sorted_by=[key];append=true>//tmp/t2',
            command=command,
            opt=['/spec/job_count=2'])

        assert get('//tmp/t2/@sorted') == 'true'
        assert get('//tmp/t2/@sorted_by') == ['key']
        assert read('//tmp/t2') == [{'key':0 , 'value':'one'}, {'key':1, 'value':'two'}, {'key':10, 'value':'one'}, {'key':11, 'value':'two'}]

    def test_sorted_output_overlap(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        for i in xrange(2):
            write('<append=true>//tmp/t1', {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo "{key=1; value=one}; {key=2; value=two}"'

        with pytest.raises(YtError):
            map(
                in_='//tmp/t1',
                out='<sorted_by=[key]>//tmp/t2',
                command=command,
                opt=['/spec/job_count=2'])

    def test_sorted_output_job_failure(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        for i in xrange(2):
            write('<append=true>//tmp/t1', {"key": "foo", "value": "ninja"})

        command = 'cat >/dev/null; echo {key=2; value=one}; {key=1; value=two}'

        with pytest.raises(YtError):
            map(
                in_='//tmp/t1',
                out='<sorted_by=[key]>//tmp/t2',
                command=command,
                opt=['/spec/job_count=2'])

    @only_linux
    def test_job_count(self):
        create('table', '//tmp/t1')
        for i in xrange(5):
            write('<append=true>//tmp/t1', {"foo": "bar"})

        command = "cat > /dev/null; echo {hello=world}"

        def check(table_name, job_count, expected_num_records):
            create('table', table_name)
            map(in_='//tmp/t1',
                out=table_name,
                command=command,
                opt=['/spec/job_count=%d' % job_count])
            assert read(table_name) == [{'hello': 'world'} for i in xrange(expected_num_records)]

        check('//tmp/t2', 3, 3)
        check('//tmp/t3', 10, 5) # number of jobs can't be more that number of chunks

    @only_linux
    def test_with_user_files(self):
        create('table', '//tmp/input')
        write('//tmp/input', {"foo": "bar"})

        create('table', '//tmp/output')

        file1 = '//tmp/some_file.txt'
        file2 = '//tmp/renamed_file.txt'

        create('file', file1)
        create('file', file2)

        upload(file1, '{value=42};\n')
        upload(file2, '{a=b};\n')

        create('table', '//tmp/table_file')
        write('//tmp/table_file', {"text": "info"})

        command= "cat > /dev/null; cat some_file.txt; cat my_file.txt; cat table_file;"

        map(in_='//tmp/input',
            out='//tmp/output',
            command=command,
            file=[file1, "<file_name=my_file.txt>" + file2, "<format=yson>//tmp/table_file"])

        assert read('//tmp/output') == [{'value': 42}, {'a': 'b'}, {"text": "info"}]

    @only_linux
    def test_empty_user_files(self):
        create('table', '//tmp/input')
        write('//tmp/input', {"foo": "bar"})

        create('table', '//tmp/output')

        file1 = '//tmp/empty_file.txt'
        create('file', file1)

        table_file = '//tmp/table_file'
        create('table', table_file)

        command= "cat > /dev/null; cat empty_file.txt; cat table_file"

        map(in_='//tmp/input',
            out='//tmp/output',
            command=command,
            file=[file1, '<format=yamr>' + table_file])

        assert read('//tmp/output') == []

    @only_linux
    def test_multi_chunk_user_files(self):
        create('table', '//tmp/input')
        write('//tmp/input', {"foo": "bar"})

        create('table', '//tmp/output')

        file1 = '//tmp/regular_file'
        create('file', file1)
        upload(file1, '{value=42};\n')
        set(file1 + '/@compression_codec', 'lz4')
        upload('<append=true>' + file1, '{a=b};\n')

        table_file = '//tmp/table_file'
        create('table', table_file)
        write(table_file, {"text": "info"})
        set(table_file + '/@compression_codec', 'snappy')
        write('<append=true>' + table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_='//tmp/input',
            out='//tmp/output',
            command=command,
            file=[file1, '<format=yson>' + table_file])

        assert read('//tmp/output') == [{'value': 42}, {'a': 'b'}, {"text": "info"}, {"text": "info"}]

    @pytest.mark.xfail(run = True, reason = 'No support for erasure chunks in user files')
    def test_erasure_user_files(self):
        create('table', '//tmp/input')
        write('//tmp/input', {"foo": "bar"})

        create('table', '//tmp/output')

        file1 = '//tmp/regular_file'
        create('file', file1)
        set(file1 + '/@erasure_codec', 'lrc_12_2_2')
        upload(file1, '{value=42};\n')
        upload(file1, '{a=b};\n')

        table_file = '//tmp/table_file'
        create('table', table_file)
        set(table_file + '/@erasure_codec', 'reed_solomon_6_3')
        for i in xrange(2):
            write(table_file, {"text": "info"})

        command= "cat > /dev/null; cat regular_file; cat table_file"

        map(in_='//tmp/input',
            out='//tmp/output',
            command=command,
            file=[file1, '<format=yson>' + table_file])

        assert read('//tmp/output') == [{'value': 42}, {'a': 'b'}, {"text": "info"}, {"text": "info"}]

    @only_linux
    def run_many_output_tables(self, yamr_mode=False):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write('//tmp/t_in', {"a": "b"})

        if yamr_mode:
            mapper = "cat  > /dev/null; echo {v = 0} >&3; echo {v = 1} >&4; echo {v = 2} >&5"
        else:
            mapper = "cat  > /dev/null; echo {v = 0} >&1; echo {v = 1} >&4; echo {v = 2} >&7"

        create('file', '//tmp/mapper.sh')
        upload('//tmp/mapper.sh', mapper)

        map(in_='//tmp/t_in',
            out=output_tables,
            command='bash mapper.sh',
            file='//tmp/mapper.sh',
            opt='/spec/mapper/use_yamr_descriptors=%s' % ('true' if yamr_mode else 'false'))

        assert read(output_tables[0]) == [{'v': 0}]
        assert read(output_tables[1]) == [{'v': 1}]
        assert read(output_tables[2]) == [{'v': 2}]

    @only_linux
    def test_many_output_yt(self):
        self.run_many_output_tables()

    @only_linux
    def test_many_output_yamr(self):
        self.run_many_output_tables(True)

    @only_linux
    def test_output_tables_switch(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write('//tmp/t_in', {"a": "b"})
        mapper = "cat  > /dev/null; echo '<table_index=2>#;{v = 0};{v = 1};<table_index=0>#;{v = 2}'"

        create('file', '//tmp/mapper.sh')
        upload('//tmp/mapper.sh', mapper)

        map(in_='//tmp/t_in',
            out=output_tables,
            command='bash mapper.sh',
            file='//tmp/mapper.sh')

        assert read(output_tables[0]) == [{'v': 2}]
        assert read(output_tables[1]) == []
        assert read(output_tables[2]) == [{'v': 0}, {'v': 1}]

    @only_linux
    def test_tskv_input_format(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['tskv', 'foo=bar']
print '{hello=world}'

"""
        create('file', '//tmp/mapper.sh')
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt='/spec/mapper/input_format=<line_prefix=tskv>dsv')

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    @only_linux
    def test_tskv_output_format(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '<"table_index"=0>#;'
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "tskv" + "\\t" + "hello=world"
"""
        create('file', '//tmp/mapper.sh')
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt=['/spec/mapper/enable_input_table_index=true',
                 '/spec/mapper/input_format=<format=text>yson',
                 '/spec/mapper/output_format=<line_prefix=tskv>dsv'])

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    @only_linux
    def test_yamr_output_format(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"foo": "bar"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n')
assert input == '{"foo"="bar"};'
print "key\\tsubkey\\tvalue"

"""
        create('file', '//tmp/mapper.sh')
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

    @only_linux
    def test_yamr_input_format(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"value": "value", "subkey": "subkey", "key": "key", "a": "another"})

        mapper = \
"""
import sys
input = sys.stdin.readline().strip('\\n').split('\\t')
assert input == ['key', 'subkey', 'value']
print '{hello=world}'

"""
        create('file', '//tmp/mapper.sh')
        upload('//tmp/mapper.sh', mapper)

        create('table', '//tmp/t_out')
        map(in_='//tmp/t_in',
            out='//tmp/t_out',
            command="python mapper.sh",
            file='//tmp/mapper.sh',
            opt='/spec/mapper/input_format=<has_subkey=true>yamr')

        assert read('//tmp/t_out') == [{'hello': 'world'}]

    @only_linux
    def test_executable_mapper(self):
        create('table', '//tmp/t_in')
        write('//tmp/t_in', {"foo": "bar"})

        mapper =  \
"""
#!/bin/sh
cat > /dev/null; echo {hello=world}
"""

        create('file', '//tmp/mapper.sh')
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
        write('//tmp/t', {"foo": "bar"})

        op_id = map(dont_track=True,
            in_='//tmp/t',
            out='//tmp/t',
            command="sleep 1")

        path = '//sys/operations/%s/@state' % op_id
        # check running
        abort_op(op_id)
        assert get(path) == 'aborted'


    @only_linux
    def test_table_index(self):
        create('table', '//tmp/t1')
        create('table', '//tmp/t2')
        create('table', '//tmp/out')

        write('//tmp/t1', {"key": "a", "value": "value"})
        write('//tmp/t2', {"key": "b", "value": "value"})

        mapper = \
"""
import sys
table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index

table_index = sys.stdin.readline().strip()
row = sys.stdin.readline().strip()
print row + table_index
"""

        create('file', '//tmp/mapper.py')
        upload('//tmp/mapper.py', mapper)

        map(in_=['//tmp/t1', '//tmp/t2'],
            out='//tmp/out',
            command="python mapper.py",
            file='//tmp/mapper.py',
            opt=['/spec/mapper/format=<enable_table_index=true>yamr'])

        expected = [{'key': 'a', 'value': 'value0'},
                    {'key': 'b', 'value': 'value1'}]
        self.assertItemsEqual(read('//tmp/out'), expected)

    def test_insane_demand(self):
        create('table', '//tmp/t_in')
        create('table', '//tmp/t_out')

        write('//tmp/t_in', {"cool": "stuff"})

        with pytest.raises(YtError):
            map(in_='//tmp/t_in', out='//tmp/t_out', command='cat',
                opt=['/spec/mapper/memory_limit=1000000000000'])
