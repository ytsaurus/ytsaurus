import pytest

from yt_env_setup import YTEnvSetup, linux_only
from yt_commands import *
from yt.yson import YsonEntity


##################################################################

class TestSchedulerReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @linux_only
    def test_tricky_chunk_boundaries(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': "0", 'value': 1},
                {'key': "2", 'value': 2}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': "2", 'value': 6},
                {'key': "5", 'value': 8}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/out')

        reduce(
            in_=['//tmp/in1{key}', '//tmp/in2{key}'],
            out=['<sorted_by=[key]>//tmp/out'],
            command='uniq',
            reduce_by='key',
            spec={"reducer": {"format": yson.loads("<line_prefix=tskv>dsv")},
                  "data_size_per_job": 1})

        assert read_table('//tmp/out') == \
            [
                {'key': "0"},
                {'key': "2"},
                {'key': "5"}
            ]

        assert get('//tmp/out/@sorted')

    @linux_only
    def test_cat(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ],
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': -1,'value': 5},
                {'key': 1, 'value': 6},
                {'key': 3, 'value': 7},
                {'key': 5, 'value': 8}
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        reduce(
            in_=['//tmp/in1', '//tmp/in2'],
            out='<sorted_by=[key]>//tmp/out',
            command='cat',
            spec={"reducer": {"format": "dsv"}})

        assert read_table('//tmp/out') == \
            [
                {'key': "-1",'value': "5"},
                {'key': "0", 'value': "1"},
                {'key': "1", 'value': "6"},
                {'key': "2", 'value': "2"},
                {'key': "3", 'value': "7"},
                {'key': "4", 'value': "3"},
                {'key': "5", 'value': "8"},
                {'key': "7", 'value': "4"}
            ]

        assert get('//tmp/out/@sorted')

    @linux_only
    def test_control_attributes_yson(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            {'key': 4, 'value': 3},
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            {'key': 1, 'value': 6},
            sorted_by = 'key')

        create('table', '//tmp/out')

        op_id = reduce(
            dont_track=True,
            in_=['//tmp/in1', '//tmp/in2'],
            out='<sorted_by=[key]>//tmp/out',
            command='cat > /dev/stderr',
            spec={
                "reducer" : {"format" : yson.loads("<format=text>yson")},
                "job_io" : 
                    {"control_attributes" : 
                        {"enable_table_index" : "true", 
                         "enable_row_index" : "true"}},
                "job_count" : 1})
        
        track_op(op_id)
        
        job_ids = ls('//sys/operations/{0}/jobs'.format(op_id))
        assert len(job_ids) == 1
        assert read_file('//sys/operations/{0}/jobs/{1}/stderr'.format(op_id, job_ids[0])) == \
            '<"table_index"=1;>#;\n' \
            '<"row_index"=0;>#;\n' \
            '{"key"=1;"value"=6;};\n' \
            '<"table_index"=0;>#;\n' \
            '<"row_index"=0;>#;\n' \
            '{"key"=4;"value"=3;};\n'

    @linux_only
    def test_cat_teleport(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': 8,'value': 5},
                {'key': 9, 'value': 6},
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in3')
        write_table(
            '//tmp/in3',
            [ {'key': 8,'value': 1}, ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in4')
        write_table(
            '//tmp/in4',
            [ {'key': 9,'value': 7}, ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/out1')
        create('table', '//tmp/out2')

        reduce(
            in_ = ['<teleport=true>//tmp/in1', '<teleport=true>//tmp/in2', '//tmp/in3', '//tmp/in4'],
            out = ['<sorted_by=[key]; teleport=true>//tmp/out1', '<sorted_by=[key]>//tmp/out2'],
            command = 'cat>/dev/fd/4',
            reduce_by = 'key',
            spec={"reducer": {"format": "dsv"}})

        assert read_table('//tmp/out1') == \
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 7, 'value': 4}
            ]

        assert read_table('//tmp/out2') == \
            [
                {'key': "8",'value': "1"},
                {'key': "8",'value': "5"},
                {'key': "9", 'value': "6"},
                {'key': "9",'value': "7"},
            ]

        assert get('//tmp/out1/@sorted')
        assert get('//tmp/out2/@sorted')

    @linux_only
    def test_maniac_chunk(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 9}
            ],
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': 2, 'value': 6},
                {'key': 2, 'value': 7},
                {'key': 2, 'value': 8}
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        reduce(
            in_ = ['//tmp/in1', '//tmp/in2'],
            out = ['<sorted_by=[key]>//tmp/out'],
            command = 'cat',
            spec={"reducer": {"format": "dsv"}})

        assert read_table('//tmp/out') == \
            [
                {'key': "0", 'value': "1"},
                {'key': "2", 'value': "9"},
                {'key': "2", 'value': "6"},
                {'key': "2", 'value': "7"},
                {'key': "2", 'value': "8"}
            ]

        assert get('//tmp/out/@sorted')


    def test_empty_in(self):
        create('table', '//tmp/in')

        # TODO(panin): replace it with sort of empty input (when it will be fixed)
        write_table('//tmp/in', {'foo': 'bar'}, sorted_by='a')
        erase('//tmp/in')

        create('table', '//tmp/out')

        reduce(
            in_ = '//tmp/in',
            out = '//tmp/out',
            command = 'cat')

        assert read_table('//tmp/out') == []

    def test_duplicate_key_columns(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')

        with pytest.raises(YtError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat',
                reduce_by=["a", "b", "a"])

    def test_unsorted_input(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write_table('//tmp/in', {'foo': 'bar'})

        with pytest.raises(YtError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat')

    def test_non_prefix(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write_table('//tmp/in', {'key': '1', 'subkey': '2'}, sorted_by=['key', 'subkey'])

        with pytest.raises(YtError):
            reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat',
                reduce_by='subkey')

    def test_short_limits(self):
        create('table', '//tmp/in1')
        create('table', '//tmp/in2')
        create('table', '//tmp/out')
        write_table('//tmp/in1', [{'key': '1', 'subkey': '2'}, {'key': '2'}], sorted_by=['key', 'subkey'])
        write_table('//tmp/in2', [{'key': '1', 'subkey': '2'}, {'key': '2'}], sorted_by=['key', 'subkey'])

        reduce(
            in_ = ['//tmp/in1["1":"2"]', '//tmp/in2'],
            out = '<sorted_by=[key; subkey]>//tmp/out',
            command = 'cat',
            reduce_by=['key', 'subkey'],
            spec={"reducer": {"format": yson.loads("<line_prefix=tskv>dsv")},
              "data_size_per_job": 1})

        assert read_table('//tmp/out') == \
            [
                {'key': "1", 'subkey': "2"},
                {'key': "1", 'subkey': "2"},
                {'key': "2", 'subkey' : YsonEntity()}
            ]

    @linux_only
    def test_many_output_tables(self):
        output_tables = ['//tmp/t%d' % i for i in range(3)]

        create('table', '//tmp/t_in')
        for table_path in output_tables:
            create('table', table_path)

        write_table('//tmp/t_in', [{"k": 10}], sorted_by='k')

        reducer = \
"""
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7

"""
        create('file', '//tmp/reducer.sh')
        write_file('//tmp/reducer.sh', reducer)

        reduce(in_='//tmp/t_in',
            out=output_tables,
            command='bash reducer.sh',
            file='//tmp/reducer.sh')

        assert read_table(output_tables[0]) == [{'v': 0}]
        assert read_table(output_tables[1]) == [{'v': 1}]
        assert read_table(output_tables[2]) == [{'v': 2}]
        
    def test_job_count(self):
        create('table', '//tmp/in', attributes={"compression_codec": "none"})
        create('table', '//tmp/out')

        count = 10000

        # Job count works only if we have enough splits in input chunks.
        # Its default rate 0.0001, so we should have enough rows in input table
        write_table(
            '//tmp/in',
            [ {'key': "%.010d" % num} for num in xrange(count) ],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})

        reduce(
            in_ = '//tmp/in',
            out = '//tmp/out',
            command = 'cat; echo "key=10"',
            reduce_by=['key'],
            spec={"reducer": {"format": "dsv"},
                  "data_size_per_job": 1})

        # Check that operation has more than 1 job
        assert get("//tmp/out/@row_count") >= count + 2

    def test_key_switch_yamr(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')

        write_table(
            '//tmp/in',
            [
                {'key': 'a', 'value': ''},
                {'key': 'b', 'value': ''},
                {'key': 'b', 'value': ''}
            ],
            sorted_by = ['key'])

        op_id = reduce(
            in_='//tmp/in',
            out='//tmp/out',
            command='cat 1>&2',
            reduce_by=['key'],
            spec={
                "job_io": {"control_attributes": {"enable_key_switch": "true"}},
                "reducer": {"format": yson.loads("<lenval=true>yamr")},
                "job_count": 1
            })

        jobs_path = "//sys/operations/{0}/jobs".format(op_id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = read_file("{0}/{1}/stderr".format(jobs_path, job_ids[0]))

        assert stderr_bytes.encode("hex") == \
            "010000006100000000" \
            "feffffff" \
            "010000006200000000" \
            "010000006200000000"

    def test_key_switch_yson(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')

        write_table(
            '//tmp/in',
            [
                {'key': 'a', 'value': ''},
                {'key': 'b', 'value': ''},
                {'key': 'b', 'value': ''}
            ],
            sorted_by = ['key'])

        op_id = reduce(
            in_='//tmp/in',
            out='//tmp/out',
            command='cat 1>&2',
            reduce_by=['key'],
            spec={
                "job_io": {"control_attributes": {"enable_key_switch": "true"}},
                "reducer": {"format": yson.loads("<format=text>yson")},
                "job_count": 1
            })

        jobs_path = "//sys/operations/{0}/jobs".format(op_id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = read_file("{0}/{1}/stderr".format(jobs_path, job_ids[0]))

        assert stderr_bytes == \
            '{"key"="a";"value"="";};\n' \
            '<"key_switch"=%true;>#;\n' \
            '{"key"="b";"value"="";};\n' \
            '{"key"="b";"value"="";};\n'

##################################################################

class TestSchedulerReduceCommandsMulticell(TestSchedulerReduceCommands):
    NUM_SECONDARY_MASTER_CELLS = 2
