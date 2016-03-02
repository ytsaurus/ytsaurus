import pytest

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *
from yt.yson import YsonEntity


##################################################################

class TestSchedulerJoinReduceCommands(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @unix_only
    def test_join_reduce_tricky_chunk_boundaries(self):
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

        join_reduce(
            in_ = ['//tmp/in1{key}', '<foreign=true>//tmp/in2{key}'],
            out = ['<sorted_by=[key]>//tmp/out'],
            command = 'cat',
            join_by = 'key',
            spec = {
                "reducer": {
                    "format": yson.loads("<line_prefix=tskv;enable_table_index=true>dsv")},
                "data_size_per_job": 1})

        rows = read_table('//tmp/out')
        assert len(rows) == 3
        assert rows == \
            [
                {'key': "0", '@table_index': "0"},
                {'key': "2", '@table_index': "0"},
                {'key': "2", '@table_index': "1"}
            ]

        assert get('//tmp/out/@sorted')

    @unix_only
    def test_join_reduce_cat_simple(self):
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
                {'key': -1, 'value': 5},
                {'key': 1, 'value': 6},
                {'key': 3, 'value': 7},
                {'key': 5, 'value': 8}
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        join_reduce(
            in_ = ['<foreign=true>//tmp/in1', '//tmp/in2'],
            out = '<sorted_by=[key]>//tmp/out',
            command = 'cat',
            spec = {
                "reducer": {
                    "format": "dsv"}})

        assert read_table('//tmp/out') == \
            [
                {'key': "-1", 'value': "5"},
                {'key': "0", 'value': "1"},
                {'key': "1", 'value': "6"},
                {'key': "2", 'value': "2"},
                {'key': "3", 'value': "7"},
                {'key': "4", 'value': "3"},
                {'key': "5", 'value': "8"},
            ]

        assert get('//tmp/out/@sorted')

    @unix_only
    def test_join_reduce_primary_attribute_compatibility(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", [{"key": 2*i, "value": i+1} for i in range(4)], sorted_by = "key")

        create("table", "//tmp/in2")
        write_table("//tmp/in2", [{"key": 2*i-1, "value": i+5} for i in range(4)], sorted_by = "key")

        create("table", "//tmp/out")

        join_reduce(
            in_ = ["//tmp/in1", "<primary=true>//tmp/in2"],
            out = "<sorted_by=[key]>//tmp/out",
            command = "cat",
            spec = {
                "reducer": {
                    "format": "dsv"}})

        assert read_table("//tmp/out") == \
            [
                {"key": "-1", "value": "5"},
                {"key": "0", "value": "1"},
                {"key": "1", "value": "6"},
                {"key": "2", "value": "2"},
                {"key": "3", "value": "7"},
                {"key": "4", "value": "3"},
                {"key": "5", "value": "8"},
            ]

        assert get("//tmp/out/@sorted")

    @unix_only
    def test_join_reduce_control_attributes_yson(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': 1, 'value': 7},
                {'key': 5, 'value': 3},
            ],
            sorted_by = 'key')

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': 0, 'value': 4},
                {'key': 2, 'value': 6},
                {'key': 4, 'value': 8},
                {'key': 6, 'value': 10},
            ],
            sorted_by = 'key')

        create('table', '//tmp/out')

        op = join_reduce(
            in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
            out = '<sorted_by=[key]>//tmp/out',
            command = 'cat 1>&2',
            spec = {
                "reducer": {
                    "format": yson.loads("<format=text>yson")},
                "job_io": {
                    "control_attributes": {
                        "enable_table_index": "true",
                        "enable_row_index": "true"}},
                "job_count": 1})

        job_ids = ls('//sys/operations/{0}/jobs'.format(op.id))
        assert len(job_ids) == 1
        assert read_file('//sys/operations/{0}/jobs/{1}/stderr'.format(op.id, job_ids[0])) == \
            '<"table_index"=0;>#;\n' \
            '<"row_index"=0;>#;\n' \
            '{"key"=1;"value"=7;};\n' \
            '<"table_index"=1;>#;\n' \
            '<"row_index"=1;>#;\n' \
            '{"key"=2;"value"=6;};\n' \
            '{"key"=4;"value"=8;};\n' \
            '<"table_index"=0;>#;\n' \
            '<"row_index"=1;>#;\n' \
            '{"key"=5;"value"=3;};\n'

    @unix_only
    def test_join_reduce_cat_two_output(self):
        create('table', '//tmp/in1')
        write_table(
            '//tmp/in1',
            [
                {'key': 0, 'value': 1},
                {'key': 2, 'value': 2},
                {'key': 4, 'value': 3},
                {'key': 8, 'value': 4}
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in2')
        write_table(
            '//tmp/in2',
            [
                {'key': 2,'value': 5},
                {'key': 3, 'value': 6},
            ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in3')
        write_table(
            '//tmp/in3',
            [ {'key': 2,'value': 1}, ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/in4')
        write_table(
            '//tmp/in4',
            [ {'key': 3,'value': 7}, ],
            sorted_by = ['key', 'value'])

        create('table', '//tmp/out1')
        create('table', '//tmp/out2')

        join_reduce(
            in_ = ['//tmp/in1', '<foreign=true>//tmp/in2', '<foreign=true>//tmp/in3', '<foreign=true>//tmp/in4'],
            out = ['<sorted_by=[key]>//tmp/out1', '<sorted_by=[key]>//tmp/out2'],
            command = 'cat | tee /dev/fd/4 | grep @table_index=0',
            join_by = 'key',
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true>dsv")}})

        assert read_table('//tmp/out1') == \
            [
                {'key': "0", 'value': "1", '@table_index': "0"},
                {'key': "2", 'value': "2", '@table_index': "0"},
                {'key': "4", 'value': "3", '@table_index': "0"},
                {'key': "8", 'value': "4", '@table_index': "0"},
            ]

        assert read_table('//tmp/out2') == \
            [
                {'key': "0", 'value': "1", '@table_index': "0"},
                {'key': "2", 'value': "2", '@table_index': "0"},
                {'key': "2", 'value': "5", '@table_index': "1"},
                {'key': "2", 'value': "1", '@table_index': "2"},
                {'key': "3", 'value': "6", '@table_index': "1"},
                {'key': "3", 'value': "7", '@table_index': "3"},
                {'key': "4", 'value': "3", '@table_index': "0"},
                {'key': "8", 'value': "4", '@table_index': "0"},
            ]

        assert get('//tmp/out1/@sorted')
        assert get('//tmp/out2/@sorted')

    @unix_only
    def test_join_reduce_empty_in(self):
        create('table', '//tmp/in1', schema=[{'name': 'key', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/in2', schema=[{'name': 'key', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/out')

        join_reduce(
            in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
            out = '//tmp/out',
            command = 'cat')

        assert read_table('//tmp/out') == []

    @unix_only
    def test_join_reduce_duplicate_key_columns(self):
        create('table', '//tmp/in1', schema=[
            {'name': 'a', 'type': 'any', 'sort_order': 'ascending'}, 
            {'name': 'b', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/in2', schema=[
            {'name': 'a', 'type': 'any', 'sort_order': 'ascending'}, 
            {'name': 'b', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/out')

        # expected error: Duplicate key column name "a"
        with pytest.raises(YtError):
            join_reduce(
                in_ = '//tmp/in',
                out = '//tmp/out',
                command = 'cat',
                join_by=["a", "b", "a"])

    @unix_only
    def test_join_reduce_unsorted_input(self):
        create('table', '//tmp/in1')
        write_table('//tmp/in1', {'foo': 'bar'})
        create('table', '//tmp/in2', schema=[{'name': 'foo', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/out')

        # expected error: Input table //tmp/in1 is not sorted
        with pytest.raises(YtError):
            join_reduce(
                in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
                out = '//tmp/out',
                command = 'cat')

    @unix_only
    def test_join_reduce_different_key_column(self):
        create('table', '//tmp/in1')
        write_table('//tmp/in1', {'foo': 'bar'}, sorted_by=['foo'])
        create('table', '//tmp/in2', schema=[{'name': 'baz', 'type': 'any', 'sort_order': 'ascending'}])
        create('table', '//tmp/out')

        # expected error: Key columns do not match
        with pytest.raises(YtError):
            join_reduce(
                in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
                out = '//tmp/out',
                command = 'cat')

    @unix_only
    def test_join_reduce_non_prefix(self):
        create('table', '//tmp/in')
        create('table', '//tmp/out')
        write_table('//tmp/in', {'key': '1', 'subkey': '2'}, sorted_by=['key', 'subkey'])

        # expected error: Input table is sorted by columns that are not compatible with the requested columns"
        with pytest.raises(YtError):
            join_reduce(
                in_ = ['//tmp/in', '<foreign=true>//tmp/in'],
                out = '//tmp/out',
                command = 'cat',
                join_by = 'subkey')

    @unix_only
    def test_join_reduce_short_limits(self):
        create('table', '//tmp/in1')
        create('table', '//tmp/in2')
        create('table', '//tmp/out')
        write_table('//tmp/in1', [{'key': '1', 'subkey': '2'}, {'key': '3'}], sorted_by=['key', 'subkey'])
        write_table('//tmp/in2', [{'key': '1', 'subkey': '3'}, {'key': '2'}], sorted_by=['key', 'subkey'])

        join_reduce(
            in_ = ['//tmp/in1["1":"3"]', '<foreign=true>//tmp/in2'],
            out = '<sorted_by=[key; subkey]>//tmp/out',
            command = 'cat',
            join_by = ['key', 'subkey'],
            spec = {
                "reducer": {
                    "format": yson.loads("<line_prefix=tskv>dsv")},
                "data_size_per_job": 1})

        assert read_table('//tmp/out') == \
            [
                {'key': "1", 'subkey': "2"},
                {'key': "1", 'subkey': "3"},
                {'key': "2", 'subkey': YsonEntity()}
            ]

    @unix_only
    def test_join_reduce_many_output_tables(self):
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

        join_reduce(
            in_ = ['//tmp/t_in', '<foreign=true>//tmp/t_in'],
            out = output_tables,
            command = 'bash reducer.sh',
            file = '//tmp/reducer.sh')

        assert read_table(output_tables[0]) == [{'v': 0}]
        assert read_table(output_tables[1]) == [{'v': 1}]
        assert read_table(output_tables[2]) == [{'v': 2}]

    @unix_only
    def test_join_reduce_job_count(self):
        create('table', '//tmp/in1', attributes={"compression_codec": "none"})
        create('table', '//tmp/in2', schema=[{"name": "key", "type": "any", "sort_order": "ascending"}])
        create('table', '//tmp/out')

        count = 1000

        # Job count works only if we have enough splits in input chunks.
        # Its default rate 0.0001, so we should have enough rows in input table
        write_table(
            '//tmp/in1',
            [{'key': "%.010d" % num} for num in xrange(count)],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})
        # write secondary table as one row per chunk
        for num in xrange(0, count, 20):
            write_table(
                '<append=true>//tmp/in2',
                {'key': "%.010d" % num},
                sorted_by = ['key'])

        join_reduce(
            in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
            out = '//tmp/out',
            command = 'echo "key=`wc -l`"',
            join_by = ['key'],
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true>dsv")
                },
                "data_size_per_job": 250
            })

        read_table("//tmp/out");
        get("//tmp/out/@row_count")
        # Check that operation has more than 1 job
        assert get("//tmp/out/@row_count") >= 2

    @unix_only
    def test_join_reduce_key_switch_yamr(self):
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

        op = join_reduce(
            in_ = ['//tmp/in', '<foreign=true>//tmp/in'],
            out = '//tmp/out',
            command = 'cat 1>&2',
            join_by = ['key'],
            spec = {
                "job_io": {
                    "control_attributes": {
                        "enable_key_switch": "true"
                    }
                },
                "reducer": {
                    "format": yson.loads("<lenval=true>yamr"),
                    "enable_input_table_index": False
                },
                "job_count": 1
            })

        jobs_path = "//sys/operations/{0}/jobs".format(op.id)
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = read_file("{0}/{1}/stderr".format(jobs_path, job_ids[0]))

        assert stderr_bytes.encode("hex") == \
            "010000006100000000" \
            "010000006100000000" \
            "feffffff" \
            "010000006200000000" \
            "010000006200000000" \
            "010000006200000000" \
            "010000006200000000"

    @unix_only
    def test_join_reduce_with_small_block_size(self):
        create('table', '//tmp/in1', attributes={"compression_codec": "none"})
        create('table', '//tmp/in2')
        create('table', '//tmp/out')

        count = 300

        write_table(
            '<append=true>//tmp/in1',
            [ {'key': "%05d"%(10000+num/2), 'val1': num} for num in xrange(count) ],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})
        write_table(
            '<append=true>//tmp/in1',
            [ {'key': "%05d"%(10000+num/2), 'val1': num} for num in xrange(count,2*count) ],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})

        write_table(
            '<append=true>//tmp/in2',
            [ {'key': "%05d"%(10000+num/2), 'val2': num} for num in xrange(count) ],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})
        write_table(
            '<append=true>//tmp/in2',
            [ {'key': "%05d"%(10000+num/2), 'val2': num} for num in xrange(count,2*count) ],
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})

        join_reduce(
            in_ = [
                '<ranges=[{lower_limit={row_index=100;key=["10010"]};upper_limit={row_index=540;key=["10280"]}}]>//tmp/in1',
                '<foreign=true>//tmp/in2',
            ],
            out = '//tmp/out',
            command = '''awk '{print $0"\tji="ENVIRON["YT_JOB_INDEX"]"\tsi="ENVIRON["YT_START_ROW_INDEX"]}' ''',
            join_by = ['key'],
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true;table_index_column=ti>dsv")
                },
                "data_size_per_job": 500
            })

        # Compare with manually evaluated number of output rows
        assert get("//tmp/out/@row_count") == 1308

    @unix_only
    def test_join_reduce_uneven_key_distribution(self):
        create('table', '//tmp/in1')
        create('table', '//tmp/in2')
        create('table', '//tmp/out')

        data1 = [ {'key': 'a'} for num in xrange(8000) ] + [ {'key': 'b'} for num in xrange(2000) ]
        #data1 = [item for sublist in lists for item in sublist]
        write_table(
            '//tmp/in1',
            data1,
            sorted_by = ['key'],
            table_writer = {"block_size": 1024})

        data2 = [
            {'key': 'a'},
            {'key': 'b'}
        ]
        write_table(
            '//tmp/in2',
            data2,
            sorted_by = ['key'])

        join_reduce(
            in_ = ['//tmp/in1', '<foreign=true>//tmp/in2'],
            out = ['//tmp/out'],
            command = 'uniq',
            join_by = 'key',
            spec = {
                "reducer": {
                    "format": yson.loads("<enable_table_index=true>dsv")},
                "job_count": 2 })

        assert sorted(read_table('//tmp/out')) == \
            sorted([
                {'key': "a", '@table_index': "0"},
                {'key': "a", '@table_index': "0"},
                {'key': "b", '@table_index': "0"},
                {'key': "a", '@table_index': "1"},
                {'key': "a", '@table_index': "1"},
                {'key': "b", '@table_index': "1"},
            ])
