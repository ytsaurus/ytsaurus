from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    OFFSHORE_DATA_GATEWAYS_SERVICE,
)
from yt.common import YtError
from yt.yson import YsonList, YsonEntity

from yt_commands import (
    authors, extract_statistic_v2, sync_create_cells, create_user, add_member, make_ace,
    create, create_domestic_medium, create_s3_medium, set, remove, exists,
    copy, move, get_singular_chunk_id, wait, get, concatenate, get_table_columnar_statistics, wait_no_assert,
    get_account_disk_space_limit, set_account_disk_space_limit,
    write_table, read_table, sync_mount_table, insert_rows, sync_flush_table, attach_table,
    map, merge, select_rows, lookup_rows, print_debug, write_file, read_file,
    sort, partition_tables, map_reduce, reduce, ls, create_account,
    start_transaction, commit_transaction, abort_transaction, raises_yt_error)

from yt_commands import PrefixExternalSourceSpec, FilesExternalSourceSpec

from yt.test_helpers import assert_items_equal
from yt_type_helpers import optional_type, make_schema, make_column, struct_type, list_type, normalize_schema_v3

from yt.wrapper import YtClient

import yt_error_codes

import json
import time
import pytest
import os
import uuid
import logging
import random
import builtins
import string

import yt.yson as yson
import boto3
from botocore.exceptions import ClientError
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

################################################################################

# Logs from boto3 are too verbose and hinder the readability of test output.
# Increase level when necessary for debugging.
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("boto3").setLevel(logging.INFO)

################################################################################

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

################################################################################

ASCII_LETTERS_BYTES = string.ascii_letters.encode("ascii")
TRANSLATE_TABLE = bytes([ASCII_LETTERS_BYTES[b % len(ASCII_LETTERS_BYTES)] for b in range(256)])

################################################################################


class TestS3MediumBase(YTEnvSetup):
    # TODO(achulkov2): Switch to multidaemon?
    NUM_MASTERS = 1
    NUM_NODES = 3

    NUM_SCHEDULERS = 1

    NUM_OFFSHORE_DATA_GATEWAYS = 1

    USE_DYNAMIC_TABLES = True

    S3_MEDIA = [
        {
            "name": "s3_main",
            "bucket": uuid.uuid4().hex,
        },
        {
            "name": "s3_extra1",
            "bucket": uuid.uuid4().hex,
        },
        {
            "name": "s3_extra2",
            "bucket": uuid.uuid4().hex,
        },
        {
            "name": "s3_prefixed",
            "bucket": uuid.uuid4().hex,
            "prefix": "prefix/",
        },
        {
            "name": "s3_read_only",
        },
    ]

    EXTRA_BUCKET_COUNT = 2
    EXTRA_BUCKETS = [uuid.uuid4().hex for _ in range(EXTRA_BUCKET_COUNT)]

    # NB: The buckets defined above S3_MEDIA and EXTRA_BUCKETS are created for each test
    # invocation and cleaned up afterwards. In order to avoid interference between tests,
    # these are the *only* buckets that should be used in all the tests.

    S3_CLIENT = None

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        }
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "chunk_merger": {
                "enable": True,
                "max_chunk_count": 5,
                "create_chunks_period": 100,
                "schedule_period": 100,
                "session_finalization_period": 100,
                "shallow_merge_validation_probability": 100,
            }
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        # Medium directory is needed to perform S3 reads and right now it is not
                        # passed along with the job spec, so we need to sync it at the start of
                        # each job. This option is needed for production as well!
                        "sync_medium_directory_on_start": True,
                    },
                },
            },
        },
    }

    DELTA_DRIVER_CONFIG = {
        "enable_replication_reader_for_offshore_data": True,
    }

    DELTA_NODE_CONFIG = {
        "data_node": {
            "incremental_heartbeat_period": 100,
        },
        "cluster_connection": {
            "enable_replication_reader_for_offshore_data": True,
        },
    }

    @classmethod
    def make_random_string(cls, length):
        assert length >= 0
        return os.urandom(length).translate(TRANSLATE_TABLE).decode("ascii")

    @staticmethod
    def assert_or_check(expression, message, do_assert):
        if do_assert:
            assert expression, message
        else:
            print_debug(message)
            return False

    @classmethod
    def get_buckets(cls):
        return {medium["bucket"] for medium in cls.S3_MEDIA if "bucket" in medium} | builtins.set(cls.EXTRA_BUCKETS)

    @classmethod
    def get_s3_medium_name(cls, index=0):
        return cls.S3_MEDIA[index]["name"]

    @classmethod
    def get_s3_medium_bucket(cls, index=0):
        return cls.S3_MEDIA[index]["bucket"]

    @classmethod
    def get_s3_medium_config(cls, index=0):
        media = {**cls.S3_MEDIA[index]}
        del media["name"]
        return {
            # Standard S3 environment variables.
            "url": os.getenv("AWS_ENDPOINT_URL"),
            "region": os.getenv("AWS_REGION"),
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            # YT specific configuration.
            **media,
        }

    @classmethod
    def setup_s3_client(cls):
        # Credentials are retrieved from standard S3 environment variables.
        cls.S3_CLIENT = boto3.client('s3')

    @classmethod
    def teardown_s3_client(cls):
        if cls.S3_CLIENT is not None:
            cls.S3_CLIENT.close()

    @classmethod
    def create_bucket(cls, bucket):
        assert cls.S3_CLIENT is not None

        try:
            cls.S3_CLIENT.create_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise

    @classmethod
    def clear_bucket(cls, bucket):
        assert cls.S3_CLIENT is not None

        try:
            objects = cls.S3_CLIENT.list_objects_v2(Bucket=bucket).get('Contents', [])
            for obj in objects:
                cls.S3_CLIENT.delete_object(Bucket=bucket, Key=obj['Key'])
            cls.S3_CLIENT.delete_bucket(Bucket=bucket)
        except ClientError as e:
            print_debug(f"Failed to clear bucket {bucket} due to error: {e}")

    @classmethod
    def object_exists_in_s3(cls, bucket, key):
        assert cls.S3_CLIENT is not None

        try:
            cls.S3_CLIENT.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    @classmethod
    def get_s3_object(cls, bucket, key):
        assert cls.S3_CLIENT is not None

        try:
            response = cls.S3_CLIENT.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            raise

    @classmethod
    def get_chunk_path(cls, chunk_id, s3_medium_index=0, prefix=""):
        return f"{prefix}chunk-data/{chunk_id[-4:-2]}/{chunk_id[-2:]}/{chunk_id}"

    @classmethod
    def get_chunk_meta_path(cls, chunk_id, s3_medium_index=0, prefix=""):
        return f"{prefix}chunk-data/{chunk_id[-4:-2]}/{chunk_id[-2:]}/{chunk_id}.meta"

    @classmethod
    def get_chunk_generated_meta_path(cls, chunk_id, s3_medium_index=0, prefix=""):
        return f"{prefix}generated-chunk-data/{chunk_id[-4:-2]}/{chunk_id[-2:]}/{chunk_id}.meta"

    @classmethod
    def assert_chunk_exists_in_s3(cls, chunk_id, s3_medium_index=0, negate=False):
        bucket = cls.S3_MEDIA[s3_medium_index]["bucket"]

        chunk_path = cls.get_chunk_path(chunk_id, s3_medium_index)
        chunk_meta_path = cls.get_chunk_meta_path(chunk_id, s3_medium_index)

        if negate:
            assert not cls.object_exists_in_s3(bucket, chunk_path), f"File {chunk_path} should not exist in S3"
            assert not cls.object_exists_in_s3(bucket, chunk_meta_path), f"File {chunk_meta_path} should not exist in S3"
        else:
            assert cls.object_exists_in_s3(bucket, chunk_path), f"File {chunk_path} should exist in S3"
            assert cls.object_exists_in_s3(bucket, chunk_meta_path), f"File {chunk_meta_path} should exist in S3"

    @classmethod
    def assert_chunks_exist_in_s3(cls, chunk_ids, s3_medium_index=0, negate=False):
        for chunk_id in chunk_ids:
            cls.assert_chunk_exists_in_s3(chunk_id, s3_medium_index, negate)

    @classmethod
    def assert_table_chunks_exist_in_s3(cls, table_name, s3_medium_index=0, negate=False):
        chunk_ids = get(f"{table_name}/@chunk_ids")
        cls.assert_chunks_exist_in_s3(chunk_ids, s3_medium_index, negate)

    @classmethod
    def setup_class(cls, *args, **kwargs):
        super(TestS3MediumBase, cls).setup_class(*args, **kwargs)

        cls.setup_s3_client()

        disk_space_limit = get_account_disk_space_limit("tmp", "default")

        create_domestic_medium("hdd1")

        for i in range(len(cls.S3_MEDIA)):
            name = cls.get_s3_medium_name(i)
            create_s3_medium(name, cls.get_s3_medium_config(i))
            set_account_disk_space_limit("tmp", disk_space_limit, name)

    @classmethod
    def teardown_class(cls):
        cls.teardown_s3_client()

        super(TestS3MediumBase, cls).teardown_class()

    def setup_method(self, method):
        if self.NUM_OFFSHORE_DATA_GATEWAYS == 0:
            has_mark = any(m for m in method.pytestmark if m.name == "run_with_no_offshore_data_gateways")
            if not has_mark:
                pytest.skip("This test was not marked to be run with no offshore data gateways")

        super(TestS3MediumBase, self).setup_method(method)

        for bucket in self.get_buckets():
            self.create_bucket(bucket)

        # NB(achulkov2): I don't remember the specifics, but some cleanup/setup logic clears these configs, so we need to set them again.
        for i in range(len(self.S3_MEDIA)):
            set(f"//sys/media/{self.get_s3_medium_name(i)}/@config", self.get_s3_medium_config(i))

        # Wait for medium directory synchronizer to do its thing.
        # TODO(achulkov2): This can be removed once/if we return medium directory entries along with chunk specs during fetch.
        time.sleep(1)

        # Necessary for the tests involving dynamic tables.
        sync_create_cells(1)

    def teardown_method(self, method):
        for bucket in self.get_buckets():
            self.clear_bucket(bucket)

        super(TestS3MediumBase, self).teardown_method(method)

    @staticmethod
    def dump_arrow_table_as_bytes(table: pa.Table, **kwargs) -> BytesIO:
        buffer = BytesIO()
        pq.write_table(table, buffer, **kwargs)
        buffer.seek(0)
        return buffer

    @staticmethod
    def assert_columnar_statistics(columnar_statistics, column_name, data_weight, min_value, max_value):
        assert columnar_statistics["column_data_weights"][column_name] == data_weight
        assert columnar_statistics["column_min_values"][column_name] == min_value
        assert columnar_statistics["column_max_values"][column_name] == max_value

    @staticmethod
    def serialize_data_for_s3(data, output_file_format: str):
        """
        Serialize your data to be put into S3 as a file.
        :param data: your data in format list[dict[str, any]]; this function performs no
        validation of the data, so, for instance, if you use CSV format, but have different
        counts of elements in the dictionaries, it's on you.
        :param output_file_format: "parquet" | "jsonl" | "csv"
        """
        if output_file_format == "parquet":
            return TestS3MediumBase.dump_arrow_table_as_bytes(
                pa.Table.from_pandas(pandas.DataFrame.from_records(data))
            )
        elif output_file_format == "jsonl":
            return "".join([
                f"{json.dumps(row)}\n"
                for row
                in data])
        elif output_file_format == "csv":
            result = ",".join(data[0].keys()) + "\n"
            for row in data:
                result += ",".join(str(value) for value in row.values()) + "\n"
            return result
        else:
            pytest.fail("Unknown format is passed to the function")


################################################################################


class TestS3Medium(TestS3MediumBase):
    @authors("pavel-bash")
    @pytest.mark.parametrize("as_superuser", [False, True])
    def test_config_change(self, as_superuser):
        create_user("u")
        if as_superuser:
            add_member("u", "superusers")

        def check_field(config, config_path, field, should_throw):
            # Some fields may not be set in some configs (for instance, 'prefix').
            if field not in config:
                return

            original_value = config[field]
            config[field] = original_value + "_updated"

            if should_throw:
                with pytest.raises(YtError, match="only superusers can change"):
                    set(config_path, config, authenticated_user="u")
            else:
                set(config_path, config, authenticated_user="u")

            config[field] = original_value

        # Map of <field, forbidden to change> pairs.
        fields = [
            ("access_key_id", False),
            ("secret_access_key", False),
            ("url", not as_superuser),
            ("region", not as_superuser),
            ("bucket", not as_superuser),
            ("prefix", not as_superuser),
        ]

        for medium_index in range(0, len(self.S3_MEDIA)):
            config_path = f"//sys/media/{self.get_s3_medium_name(medium_index)}/@config"

            # We can set the same config.
            config = self.get_s3_medium_config(medium_index)
            set(config_path, config)

            for field, forbidden in fields:
                check_field(config, config_path, field, forbidden)

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_tables_simple(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"c": "d"})

        many_blocks = [{f"row_{i}": f"value_{i}"} for i in range(1000)]
        write_table("<append=%true>//tmp/t", many_blocks, table_writer={"block_size": 64, "min_part_size": 6 * MB})

        multiple_parts = [{"key": self.make_random_string(length=MB // 2)} for i in range(25)]
        write_table("<append=%true>//tmp/t", multiple_parts, table_writer={"block_size": 3 * MB, "min_part_size": 6 * MB})

        assert read_table("//tmp/t", verbose=False) == [{"a": "b"}, {"c": "d"}] + many_blocks + multiple_parts

        self.assert_table_chunks_exist_in_s3("//tmp/t")

    @authors("achulkov2")
    def test_operations_simple(self):
        create("table", "//tmp/in", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})

        write_table("//tmp/in", {"a": "b"})
        assert read_table("//tmp/in") == [{"a": "b"}]

        map(
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"max_failed_job_count": 1})

        assert read_table("//tmp/out") == [{"a": "b"}]

        self.assert_table_chunks_exist_in_s3("//tmp/in")
        self.assert_table_chunks_exist_in_s3("//tmp/out")

    @authors("pavel-bash")
    def test_fail_operations_simple(self):
        # Check that after S3 becomes unavailable, the jobs will start to abort.
        create("table", "//tmp/in", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})

        write_table("//tmp/in", {"a": "b"})
        assert read_table("//tmp/in") == [{"a": "b"}]

        set(f"//sys/media/{self.get_s3_medium_name()}/@config/secret_access_key", "foo")
        op = map(
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={
                "job_io": {
                    "table_reader": {
                        # With this parameter the jobs will be aborted after
                        # 5s of unsuccessful attempts to read the data.
                        "session_timeout": "5s",
                    },
                },
            },
            track=False
        )
        wait(lambda: op.get_job_count("aborted") >= 1)

    @authors("achulkov2")
    @pytest.mark.parametrize("with_teleport", [True, False])
    def test_operation_with_multiple_inputs(self, with_teleport):
        create("table", "//tmp/in1", attributes={"primary_medium": self.get_s3_medium_name(1)})
        create("table", "//tmp/in2", attributes={"primary_medium": self.get_s3_medium_name(2)})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name(0)})

        write_table("//tmp/in1", {"a": "b"})
        in1_chunk_id = get_singular_chunk_id("//tmp/in1")

        write_table("//tmp/in2", {"c": "d"})
        in2_chunk_id = get_singular_chunk_id("//tmp/in2")

        merge(
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            mode="ordered",
            spec={
                "max_failed_job_count": 1,
                "force_transform": not with_teleport,
            })

        assert read_table("//tmp/out") == [{"a": "b"}, {"c": "d"}]

        if with_teleport:
            assert get("//tmp/out/@chunk_ids") == [in1_chunk_id, in2_chunk_id]

        self.assert_table_chunks_exist_in_s3("//tmp/in1", 1)
        self.assert_table_chunks_exist_in_s3("//tmp/in2", 2)
        # If chunks were teleported, they should not exist on the main s3 medium. For now.
        # TODO(achulkov2): This will change once we implement replication between S3 media.
        self.assert_table_chunks_exist_in_s3("//tmp/out", negate=with_teleport)

    @authors("achulkov2", "pavel-bash")
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_sort_operation(self, dynamic):
        INPUT_TABLE = "<append=%true>//tmp/input"
        OUTPUT_TABLE = "<append=%true>//tmp/output"

        unsorted_input = [
            {"key": 420, "value": "hello"},
            {"key": 80085, "value": "howdy"},
            {"key": 1, "value": "hi"},
            {"key": 1337, "value": "Ehehe"},
        ]

        create("table", INPUT_TABLE, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": dynamic,
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"}
            ]})

        if dynamic:
            sync_mount_table(INPUT_TABLE)
            insert_rows(INPUT_TABLE, unsorted_input)
            sync_flush_table(INPUT_TABLE)
        else:
            write_table(INPUT_TABLE, unsorted_input)

        create("table", OUTPUT_TABLE, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]})

        sort(in_=INPUT_TABLE, out=OUTPUT_TABLE, sort_by=["key"])

        assert read_table(OUTPUT_TABLE) == sorted(unsorted_input, key=lambda row: row["key"])

    @authors("pavel-bash")
    def test_partition_table(self):
        # Partition on dynamic sorted tables invokes the GetChunkSlices of OffshoreDataGatewayService,
        # which is exactly what we want to test here (see partition_tables.cpp).

        INPUT_TABLE = "//tmp/input1"

        sorted_input = [{"key": i, "value": f"hello_{i}"} for i in range(100)]

        create("table", f"<append=%true>{INPUT_TABLE}", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]})
        sync_mount_table(INPUT_TABLE)

        # Such insertion should get us 2 chunks.
        EXPECTED_CHUNKS_AMOUNT = 2
        insert_rows(INPUT_TABLE, sorted_input[:50])
        sync_flush_table(INPUT_TABLE)
        insert_rows(INPUT_TABLE, sorted_input[50:])
        sync_flush_table(INPUT_TABLE)
        assert get(f"{INPUT_TABLE}/@chunk_count") == EXPECTED_CHUNKS_AMOUNT

        # If we specify the least data_weight_per_partition possible, we should have as many
        # partitions as we have chunks.
        partition_result = partition_tables([INPUT_TABLE], data_weight_per_partition=1)
        assert len(partition_result) == EXPECTED_CHUNKS_AMOUNT

    @authors("achulkov2", "pavel-bash")
    @pytest.mark.parametrize("sorted", [True, False])
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_map_reduce_operation(self, sorted, dynamic):
        if self.NUM_OFFSHORE_DATA_GATEWAYS == 0 and sorted and dynamic:
            pytest.skip("This operation times out if no offshore data gateways are available")

        INPUT_TABLE = "<append=%true>//tmp/input"
        OUTPUT_TABLE = "<append=%true>//tmp/output"

        input = [{"key": i, "value": f"hello_{i}"} for i in range(100)]
        if not sorted:
            random.shuffle(input)

        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ]
        if sorted:
            schema[0]["sort_order"] = "ascending"

        create("table", INPUT_TABLE, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": dynamic,
            "schema": schema,
        })

        if dynamic:
            sync_mount_table(INPUT_TABLE)
            insert_rows(INPUT_TABLE, input)
            sync_flush_table(INPUT_TABLE)
        else:
            write_table(INPUT_TABLE, input)

        create("table", OUTPUT_TABLE, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
            ]})

        map_reduce(
            in_=INPUT_TABLE,
            out=OUTPUT_TABLE,
            reduce_by="key",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat",
        )

        assert read_table(OUTPUT_TABLE) == builtins.sorted(input, key=lambda row: row["key"])

    @authors("achulkov2", "pavel-bash")
    @pytest.mark.parametrize("dynamic", [True, False])
    def test_reduce_operation(self, dynamic):
        INPUT_TABLE1 = "<append=%true>//tmp/input1"
        INPUT_TABLE2 = "<append=%true>//tmp/input2"
        OUTPUT_TABLE = "//tmp/output"

        input = [{"key": i, "value": f"hello_{i}"} for i in range(100)]

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]

        create("table", INPUT_TABLE1, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": dynamic,
            "schema": schema,
        })
        create("table", INPUT_TABLE2, attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": dynamic,
            "schema": schema,
        })

        if dynamic:
            sync_mount_table(INPUT_TABLE1)
            insert_rows(INPUT_TABLE1, [input[i] for i in range(len(input)) if i % 2 == 0])
            sync_flush_table(INPUT_TABLE1)

            sync_mount_table(INPUT_TABLE2)
            insert_rows(INPUT_TABLE2, [input[i] for i in range(len(input)) if i % 2 == 1])
            sync_flush_table(INPUT_TABLE2)
        else:
            write_table(INPUT_TABLE1, [input[i] for i in range(len(input)) if i % 2 == 0])
            write_table(INPUT_TABLE2, [input[i] for i in range(len(input)) if i % 2 == 1])

        create("table", OUTPUT_TABLE, attributes={
            "primary_medium": self.get_s3_medium_name(),
        })

        reduce(
            in_=[INPUT_TABLE1, INPUT_TABLE2],
            out=OUTPUT_TABLE,
            reduce_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table(OUTPUT_TABLE) == [{**row, "key": str(row["key"])} for row in input]

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_files_simple(self):
        create("file", "//tmp/f", attributes={"primary_medium": self.get_s3_medium_name()})

        write_file("//tmp/f", b"Hello, world!")
        assert read_file("//tmp/f") == b"Hello, world!"
        assert read_file("//tmp/f", offset=7) == b"world!"

        chunk_id = get_singular_chunk_id("//tmp/f")
        object_data = self.get_s3_object(self.S3_MEDIA[0]["bucket"], self.get_chunk_path(chunk_id))
        assert object_data == b"Hello, world!"

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_removal(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})

        object_id = get("//tmp/t/@id")
        chunk_id = get_singular_chunk_id("//tmp/t")

        remove("//tmp/t")

        time.sleep(10)

        assert not exists("//tmp/t")
        wait(lambda: not exists(f"#{object_id}"))
        wait(lambda: not exists(f"#{chunk_id}"))

        wait_no_assert(lambda: self.assert_chunk_exists_in_s3(chunk_id, negate=True))

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_copy(self):
        create("table", "//tmp/t1", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t1", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t1")

        copy("//tmp/t1", "//tmp/t2")

        assert get_singular_chunk_id("//tmp/t2") == chunk_id

        assert read_table("//tmp/t1") == [{"a": "b"}]
        assert read_table("//tmp/t2") == [{"a": "b"}]

        remove("//tmp/t1")

        assert exists(f"#{chunk_id}")
        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("achulkov2")
    def test_concatenate(self):
        create("table", "//tmp/in1", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/in1", {"a": "b"})

        create("table", "//tmp/in2", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/in2", {"c": "d"})

        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})

        first_chunk = get_singular_chunk_id("//tmp/in1")
        second_chunk = get_singular_chunk_id("//tmp/in2")

        concatenate(["//tmp/in1", "//tmp/in2", "//tmp/in1"], "//tmp/out")
        assert read_table("//tmp/out") == [{"a": "b"}, {"c": "d"}, {"a": "b"}]
        assert get("//tmp/out/@chunk_ids") == [first_chunk, second_chunk, first_chunk]

        self.assert_table_chunks_exist_in_s3("//tmp/in1")
        self.assert_table_chunks_exist_in_s3("//tmp/in2")
        self.assert_table_chunks_exist_in_s3("//tmp/out")

    @authors("achulkov2")
    def test_sorted_concatenate(self):
        create(
            "table",
            "//tmp/in1",
            attributes={
                "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}],
                "primary_medium": self.get_s3_medium_name(),
            },
        )
        write_table("//tmp/in1", [{"a": 43}])

        create(
            "table",
            "//tmp/in2",
            attributes={
                "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}],
                "primary_medium": self.get_s3_medium_name(),
            },
        )
        write_table("//tmp/in2", [{"a": 15}])

        create(
            "table",
            "//tmp/out",
            attributes={
                "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}],
                "primary_medium": self.get_s3_medium_name(),
            },
        )

        concatenate(["//tmp/in1", "//tmp/in2", "//tmp/in1"], "//tmp/out")

        assert get("//tmp/out/@sorted")
        assert get("//tmp/out/@sorted_by") == ["a"]
        assert read_table("//tmp/out") == [{"a": 15}, {"a": 43}, {"a": 43}]

        first_chunk = get_singular_chunk_id("//tmp/in1")
        second_chunk = get_singular_chunk_id("//tmp/in2")
        assert get("//tmp/out/@chunk_ids") == [second_chunk, first_chunk, first_chunk]

        self.assert_table_chunks_exist_in_s3("//tmp/in1")
        self.assert_table_chunks_exist_in_s3("//tmp/in2")
        self.assert_table_chunks_exist_in_s3("//tmp/out")

    # TODO(achulkov2): Test multi-cell and cross-cell copy.

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_move(self):
        create("table", "//tmp/t1", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t1", {"a": "b"})

        chunk_id = get_singular_chunk_id("//tmp/t1")

        move("//tmp/t1", "//tmp/t2")

        assert not exists("//tmp/t1")
        assert get_singular_chunk_id("//tmp/t2") == chunk_id
        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("achulkov2")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_dynamic_tables(self, sorted):
        schema = [
            {"name": "s", "type": "string", "sort_order": "ascending" if sorted else None},
            {"name": "i", "type": "int64"},
        ]

        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name(), "dynamic": True, "schema": schema})

        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"s": "a", "i": 3}])
        insert_rows("//tmp/t", [{"s": "b", "i": 1}, {"s": "c", "i": 2}])

        # For ordered tables this requires offshore data gateway to be enabled, since only replication reader can discover replicas if seed list is empty.
        assert select_rows("s, i from [//tmp/t] order by i limit 100") == [{"s": "b", "i": 1}, {"s": "c", "i": 2}, {"s": "a", "i": 3}]
        if sorted:
            assert lookup_rows("//tmp/t", [{"s": "b"}]) == [{"s": "b", "i": 1}]

        sync_flush_table("//tmp/t")

        assert select_rows("s, i from [//tmp/t] order by i limit 100") == [{"s": "b", "i": 1}, {"s": "c", "i": 2}, {"s": "a", "i": 3}]
        if sorted:
            assert lookup_rows("//tmp/t", [{"s": "b"}]) == [{"s": "b", "i": 1}]
        assert read_table("//tmp/t") == [{"s": "a", "i": 3}, {"s": "b", "i": 1}, {"s": "c", "i": 2}]

    def _check_requisition(self, chunk_id, conditions_per_medium):
        requisition = get(f"#{chunk_id}/@requisition")

        if len(requisition) != len(conditions_per_medium):
            print_debug(f"Requisition length {len(requisition)} does not match expected length {len(conditions_per_medium)} for chunk {chunk_id}")
            return False

        for entry in requisition:
            medium = entry["medium"]
            if medium not in conditions_per_medium:
                print_debug(f"Medium {medium} is not in expected requisition for chunk {chunk_id}")
                return False

            for condition, expected_value in conditions_per_medium[entry["medium"]].items():
                if entry[condition] != expected_value:
                    print_debug(f"Condition {condition}={expected_value} is not satisfied for medium {medium}, equal to {entry[condition]} instead for chunk {chunk_id}")
                    return False

        return True

    def _check_replication(self, chunk_id, conditions_per_medium):
        replication_statuses = get(f"#{chunk_id}/@replication_status")
        for medium, replication_status in replication_statuses.items():
            if medium not in conditions_per_medium:
                print_debug(f"Chunk has replication on an unexpected medium {medium}")
                return False

            for condition, value in replication_status.items():
                expected_value = conditions_per_medium[medium].get(condition, False)
                if value != expected_value:
                    print_debug(f"Condition {condition}={expected_value} is not satisfied for medium {medium}, equal to {value} instead")
                    return False
        return True

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_multiple_media(self):
        # create("table", "//tmp/t")
        # write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 2, "min_upload_replication_factor": 2})
        # chunk_id = get_singular_chunk_id("//tmp/t")
        # print_debug(f"Chunk id is {chunk_id}")

        # time.sleep(30)

        # return

        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})

        t_media = get("//tmp/t/@media")
        print_debug(t_media)

        t_media["default"] = {"replication_factor": 3, "data_parts_only": False}
        set("//tmp/t/@media", t_media)

        write_table("<append=%true>//tmp/t", {"c": "d"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 2

        time.sleep(30)

        for chunk in chunk_ids:
            print_debug(f"Checking replicas for chunk {chunk}")
            get(f"#{chunk}/@stored_replicas")
            get(f"#{chunk}/@stored_offshore_replicas")

        for chunk_id in chunk_ids:
            wait(lambda: self._check_requisition(chunk_id, {
                self.get_s3_medium_name(): {
                    "replication_policy": {
                        "replication_factor": 1,
                        "data_parts_only": False,
                    },
                },
                "default": {
                    "replication_policy": {
                        "replication_factor": 3,
                        "data_parts_only": False,
                    },
                },
            }))

            wait(lambda: self._check_replication(chunk_id, {
                self.get_s3_medium_name(): {
                    # Everything is false.
                },
                "default": {
                    # Everything is false.
                },
            }))

        # Chunks are not considered lost, since they are present on the S3 medium.
        assert len(get("//sys/lost_vital_chunks")) == 0
        assert len(get("//sys/lost_chunks")) == 0

        for chunk_id in chunk_ids:
            assert len(get(f"#{chunk_id}/@stored_offshore_replicas")) == 1
            assert len(get(f"#{chunk_id}/@stored_replicas")) == 3

        # Let's double check that the table is still readable.
        assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_medium_switch(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})

        write_table("//tmp/t", {"a": "b"}, table_writer={"upload_replication_factor": 3})

        set("//tmp/t/@primary_medium", "default")

        write_table("<append=%true>//tmp/t", {"c": "d"}, table_writer={"upload_replication_factor": 3})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 2

        s3_chunk, default_chunk = chunk_ids

        # Both chunks are requisitioned (i.e. requested) by the default medium only.
        for chunk in [s3_chunk, default_chunk]:
            wait(lambda: self._check_requisition(chunk, {
                "default": {
                    "replication_policy": {
                        "replication_factor": 3,
                        "data_parts_only": False,
                    },
                },
            }))

        # Both chunks should actually move to the default medium.
        for chunk in [s3_chunk, default_chunk]:
            wait(lambda: self._check_replication(chunk, {
                "default": {
                    # Everything is false, chunk is properly replicated.
                },
            }))

        # Chunk files should be removed from S3.
        # We don't wait, because we waited for replication status above.
        self.assert_table_chunks_exist_in_s3("//tmp/t", negate=True)

        # Nothing is lost!
        assert len(get("//sys/lost_vital_chunks")) == 0
        assert len(get("//sys/lost_chunks")) == 0

        # We can still read the whole table, now from default medium.
        assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]

        # Now let's move everything back to S3.
        set("//tmp/t/@primary_medium", self.get_s3_medium_name())

        # Both chunks are requisitioned (i.e. requested) by the S3 medium only.
        for chunk in [s3_chunk, default_chunk]:
            wait(lambda: self._check_requisition(chunk, {
                self.get_s3_medium_name(): {
                    "replication_policy": {
                        "replication_factor": 1,
                        "data_parts_only": False,
                    },
                },
            }))
        
        # Both chunks should actually move back to the S3 medium.
        for chunk in [s3_chunk, default_chunk]:
            wait(lambda: self._check_replication(chunk, {
                self.get_s3_medium_name(): {
                    # Everything is false, chunk is properly replicated.
                },
            }))

        # Chunk files should be present in S3 now.
        # Note that the default chunk never existed there in the first place, it was replicated!
        self.assert_table_chunks_exist_in_s3("//tmp/t")

        # Nothing is lost!
        assert len(get("//sys/lost_vital_chunks")) == 0
        assert len(get("//sys/lost_chunks")) == 0

        # We can still read the whole table, now from S3 medium.
        assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]

        # Table goes bye-bye.
        remove("//tmp/t")

        # TODO(achulkov2): Expose destroyed replica queue sizes for offshore media and check them here.

        # All data should be removed from S3 now.
        # We have to wait, because there is no state we can check in the cluster (yet).
        wait_no_assert(lambda: self.assert_chunks_exist_in_s3(chunk_ids, negate=True))

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_table_and_chunk_attributes(self):
        # This table has rf=3 because that is just how life works.
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})

        write_table("//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"c": "d"})
        write_table("<append=%true>//tmp/t", [{"e": "f"}, {"g": "h"}])

        chunk_media_statistics = get("//tmp/t/@chunk_media_statistics")
        assert len(chunk_media_statistics) == 1

        s3_medium_statistics = chunk_media_statistics[self.get_s3_medium_name()]
        assert s3_medium_statistics["chunk_count"] == 3

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 3

        # TODO(achulkov2): Remove me in main branch?
        # time.sleep(15)

        test_chunk = chunk_ids[0]

        # TODO(achulkov2): Remove me in main branch?
        get(f"#{test_chunk}/@local_requisition_index")
        get(f"#{test_chunk}/@local_requisition")

        for chunk_id in chunk_ids:
            assert self._check_requisition(chunk_id, {
                self.get_s3_medium_name(): {
                    "replication_policy": {
                        "replication_factor": 1,
                        "data_parts_only": False,
                    },
                },
            })

            assert self._check_replication(chunk_id, {
                self.get_s3_medium_name(): {
                    # Everything is false.
                },
            })

            replicas = get(f"#{chunk_id}/@stored_replicas")
            assert len(replicas) == 0

            offshore_replicas = get(f"#{chunk_id}/@stored_offshore_replicas")
            assert len(offshore_replicas) == 1
            offshore_replica = offshore_replicas[0]
            assert str(offshore_replica) == self.get_s3_medium_name()
            assert offshore_replica.attributes["medium"] == self.get_s3_medium_name()
            assert offshore_replica.attributes["medium_type"] == "s3"
            assert offshore_replica.attributes["medium_id"] == get(f"//sys/media/{self.get_s3_medium_name()}/@id")

    @authors("achulkov2")
    @pytest.mark.parametrize("chunk_merger_mode", ["auto", "shallow", "deep"])
    @pytest.mark.run_with_no_offshore_node_proxies
    def test_chunk_merger(self, chunk_merger_mode):
        chunk_count = 10
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        for i in range(chunk_count):
            write_table("<append=%true>//tmp/t", {"a": f"b{i}"})

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        actual_chunk_count = get("//tmp/t/@chunk_count")
        assert actual_chunk_count == chunk_count

        set("//tmp/t/@chunk_merger_mode", chunk_merger_mode)

        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")
        wait(lambda: get("//tmp/t/@chunk_count") < actual_chunk_count)

        chunk_ids = get("//tmp/t/@chunk_ids")

        for chunk_id in chunk_ids:
            wait(lambda: self._check_requisition(chunk_id, {
                self.get_s3_medium_name(): {
                    "replication_policy": {
                        "replication_factor": 1,
                        "data_parts_only": False,
                    },
                },
            }))

            wait(lambda: self._check_replication(chunk_id, {
                self.get_s3_medium_name(): {
                    # Everything is false.
                },
            }))

        assert read_table("//tmp/t") == [{"a": f"b{i}"} for i in range(chunk_count)]

        # TODO(achulkov2): Check that old chunks are eventually deleted, once removal jobs are implemented.
        self.assert_table_chunks_exist_in_s3("//tmp/t")

    @authors("faucct")
    def test_s3_medium_prefix(self):
        create("table", "//tmp/f", attributes={"primary_medium": "s3_prefixed"})
        record1 = {"x": 1, "y": "b"}

        write_table("//tmp/f", [record1])

        chunk_id = get_singular_chunk_id("//tmp/f")
        self.get_s3_object(self.S3_MEDIA[3]["bucket"], self.get_chunk_path(chunk_id, prefix="prefix/"))

    @authors("faucct")
    def test_medium_without_bucket(self):
        create("table", "//tmp/f", attributes={"primary_medium": "s3_read_only"})
        record1 = {"x": 1, "y": "b"}

        with pytest.raises(YtError, match='Cannot place chunks into S3 medium "s3_read_only" without a configured bucket'):
            write_table("//tmp/f", [record1])

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_node_proxies
    def test_erasure_shenanigans(self):
        # We could forbid this specific action (and maybe we should), but it is quite complicated to forbid all possible
        # ways to configure a table to have non-null erasure codec *and* a replication policy on an offshore medium.
        # It looks like it could be done in theory, but there might be caveats that we don't see right now.
        # TODO(achulkov2): Make an attempt to forbid it properly.
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name(), "erasure_codec": "isa_reed_solomon_3_3"})

        # For now, just check that one cannot actually create such chunks.
        with raises_yt_error("Erasure chunks cannot be placed on offshore media"):
            write_table("//tmp/t", {"a": "b"})


    # TODO(achulkov2): Test chunk attributes: ???.

    # TODO(achulkov2): Test operation with inputs from different media.

    # TODO(achulkov2): Test replciation between regular media still works, even if offshore requisition is present.

    # TODO(achulkov2): Test columnar tables.
    # TODO(achulkov2): Test tables with many chunks.
    # TODO(achulkov2): Artifacts/chunk cache. Implement or disable for now.
    # TODO(achulkov2): S3 medium as intermediate medium. Implement or disable for now.

    # TODO(achulkov2): Test master interaction with tables with offshore replicas: concat.
    # TODO(achulkov2): Test forbidden operations: erasure, journals, remote copy jobs (difficult to get creds from other cluster for now),
    # master jobs: replication, merge?, repair (sanity), reincarnation?

    # TODO(achulkov2): Test files.

    # TODO(pavel-bash): Test ban mechanism in replication reader does not ban the offshore data gateway but only a replica
    # when we support multiple replicas; example test as a git diff: https://paste.nebius.dev/paste/993b1203-e882-4256-ad57-762f42043e9c.html.


################################################################################


class TestS3MediumRpcProxy(TestS3Medium):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


################################################################################


class TestS3MediumNoOffshoreDataGateways(TestS3Medium):
    """
    If we have no offshore data gateways, most of the tests will time out as readers&fetchers
    retry indefinitely. However, we have a cluster connection parameter which tells
    the reader to use the raw S3 reader; with that parameter the non-fetcher tests should work.
    """
    NUM_OFFSHORE_DATA_GATEWAYS = 0

    DELTA_DRIVER_CONFIG = {
        "enable_replication_reader_for_offshore_data": False,
    }

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "enable_replication_reader_for_offshore_data": False,
        },
    }


################################################################################


class TestS3MediumOffshoreDataGateway(TestS3MediumBase):
    NUM_OFFSHORE_DATA_GATEWAYS = 3

    @authors("pavel-bash")
    def test_offshore_data_gateways_dying(self):
        # Check that killing offshore data gateways proxies does not impact reliability.
        # Basically, repeating the test_partition_table, but with nodes going back and forth.
        INPUT_TABLE = "//tmp/input1"

        sorted_input = [{"key": i, "value": f"hello_{i}"} for i in range(100)]

        create("table", f"<append=%true>{INPUT_TABLE}", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]})
        sync_mount_table(INPUT_TABLE)

        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))
        create("table", "//tmp/attached", attributes={"primary_medium": self.get_s3_medium_name(), "schema": [
            {"name": "x", "type": "int64"},
            {"name": "y", "type": "string"},
        ]})
        attach_table("//tmp/attached", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        def assert_columnar_statistics():
            columnar_statistics, = get_table_columnar_statistics('["//tmp/attached";]')
            assert columnar_statistics["chunk_row_count"] == 2
            assert columnar_statistics["column_data_weights"]["x"] > 0
            assert columnar_statistics["column_min_values"]["x"] == 1
            assert columnar_statistics["column_max_values"]["x"] == 2
            assert columnar_statistics["column_data_weights"]["y"] > 0
            assert columnar_statistics["column_min_values"]["y"] == "a"
            assert columnar_statistics["column_max_values"]["y"] == "b"

        EXPECTED_CHUNKS_AMOUNT = 2
        insert_rows(INPUT_TABLE, sorted_input[:50])
        sync_flush_table(INPUT_TABLE)
        insert_rows(INPUT_TABLE, sorted_input[50:])
        sync_flush_table(INPUT_TABLE)
        assert get(f"{INPUT_TABLE}/@chunk_count") == EXPECTED_CHUNKS_AMOUNT
        assert_columnar_statistics()

        # 1 node down cases.
        node_ids = ls("//sys/offshore_data_gateways/instances")
        for i in range(0, len(node_ids)):
            with Restarter(self.Env, OFFSHORE_DATA_GATEWAYS_SERVICE, indexes=[i]):
                partition_result = partition_tables([INPUT_TABLE], data_weight_per_partition=1)
                assert len(partition_result) == EXPECTED_CHUNKS_AMOUNT
                assert_columnar_statistics()

        # 2 nodes down cases.
        for i in range(0, len(node_ids)):
            for j in range(i + 1, len(node_ids)):
                with Restarter(self.Env, OFFSHORE_DATA_GATEWAYS_SERVICE, indexes=[i, j]):
                    partition_result = partition_tables([INPUT_TABLE], data_weight_per_partition=1)
                    assert len(partition_result) == EXPECTED_CHUNKS_AMOUNT
                    assert_columnar_statistics()


################################################################################


# This test suite exists to support the test test_enable_replication_reader_option. That test
# changes the cluster configuration under //sys/clusters/..., and those changes will be visible
# to other tests in the same suite, so be careful if you're going to add more tests here.
class TestS3MediumReaderOptions(TestS3MediumBase):
    ENABLE_HTTP_PROXY = True
    DELTA_PROXY_CONFIG = {
        "cluster_connection_dynamic_config_mode": "from_cluster_directory",
    }

    @authors("pavel-bash")
    def test_enable_replication_reader_option(self):
        def read_table_http_proxy(table_name):
            client = YtClient(proxy=self.Env.get_proxy_address())
            return list(client.read_table(table_name, table_reader={"session_timeout": "5s"}))

        # Check that enable_replication_reader_for_offshore_data option can be changed
        # dynamically, and the HTTP proxy will see the difference.
        INPUT_TABLE = "//tmp/t"
        create("table", INPUT_TABLE, attributes={"primary_medium": self.get_s3_medium_name()})
        write_table(INPUT_TABLE, {"a": "b"})

        # Should work through the replication_reader with enabled OffshoreDataGateway.
        set("//sys/clusters/primary/enable_replication_reader_for_offshore_data", True)
        time.sleep(5)
        assert read_table_http_proxy(INPUT_TABLE) == [{"a": "b"}]

        # Should timeout through the replication_reader with disabled OffshoreDataGateway.
        with Restarter(self.Env, OFFSHORE_DATA_GATEWAYS_SERVICE, indexes=list(range(self.NUM_OFFSHORE_DATA_GATEWAYS))):
            with pytest.raises(YtError, match="Replication reader session timed out"):
                assert read_table_http_proxy(INPUT_TABLE) == [{"a": "b"}]

        # Should work through the native client.
        set("//sys/clusters/primary/enable_replication_reader_for_offshore_data", False)
        time.sleep(5)
        assert read_table_http_proxy(INPUT_TABLE) == [{"a": "b"}]

        # Still should work through the native client even with disabled OffshoreDataGateway.
        with Restarter(self.Env, OFFSHORE_DATA_GATEWAYS_SERVICE, indexes=list(range(self.NUM_OFFSHORE_DATA_GATEWAYS))):
            assert read_table_http_proxy(INPUT_TABLE) == [{"a": "b"}]


################################################################################


# TODO(achulkov2): Test suite with multiple masters and multi-cell.


################################################################################


class TestAttachTableBase(TestS3MediumBase):
    pass


################################################################################


# TODO(achulkov2): This suite is getting huge, consider splitting it.
class TestAttachTable(TestAttachTableBase):
    # Add configuration here.

    @authors("pavel-bash")
    def test_attach_empty_sources(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        with pytest.raises(YtError, match="At least one source must be specified"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([]))

    @authors("pavel-bash")
    def test_attach_non_existing_medium(self):
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        medium_name = "definitely_no_such_medium"
        with pytest.raises(YtError, match=f'Node has no child with key "{medium_name}"'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]), medium=medium_name)

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_with_compression_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        row_count = 500
        row_group_count = 25

        data = [{"num": i, "str": f"{i}"} for i in range(row_count)]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(data)),
            row_group_size=row_count // row_group_count, compression="gzip",
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.parquet"]))
        assert_items_equal(read_table("//tmp/imported"), data)

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), records)
        assert self.object_exists_in_s3(bucket, self.get_chunk_generated_meta_path(get("//tmp/imported/@chunk_ids")[0]))

        self.S3_CLIENT.delete_object(Bucket=bucket, Key=self.get_chunk_generated_meta_path(get("//tmp/imported/@chunk_ids")[0]))
        with pytest.raises(YtError):
            read_table("//tmp/imported", table_reader={"session_timeout": "5s"})

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_removal(self):
        bucket = self.get_s3_medium_bucket()

        data_file_path = "foo.csv"
        self.S3_CLIENT.put_object(Bucket=bucket, Key=data_file_path, Body="a,b\n1,2\n3,4\n")

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/{data_file_path}"]), medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/imported"), [{"a": 1, "b": 2}, {"a": 3, "b": 4}])

        chunk_id = get_singular_chunk_id("//tmp/imported")

        assert self.object_exists_in_s3(bucket, data_file_path)
        # TODO(achulkov2): Write helper for this.
        assert self.object_exists_in_s3(bucket, self.get_chunk_generated_meta_path(chunk_id))

        remove("//tmp/imported")

        wait(lambda: not self.object_exists_in_s3(bucket, self.get_chunk_generated_meta_path(chunk_id)))
        # Data file should NOT be removed, as it is externally provided by the user.
        assert self.object_exists_in_s3(bucket, data_file_path)

    @authors("faucct")
    def test_concatenate_different_mediums(self):
        bucket = self.get_s3_medium_bucket()

        create("table", "//tmp/written", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/written", {"a": "b"})

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"e": "f"}]))
        ))
        create("table", "//tmp/attached", attributes={"primary_medium": self.get_s3_medium_name()})
        attach_table("//tmp/attached", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"g": "h"}]))
        ))
        create("table", "//tmp/attached_read_only", attributes={"primary_medium": "s3_read_only"})
        attach_table("//tmp/attached_read_only", FilesExternalSourceSpec([f"s3://{bucket}/bar.parquet"]))

        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})

        concatenate(["//tmp/written", "//tmp/attached", "//tmp/attached_read_only"], "//tmp/out")

        assert_items_equal(read_table("//tmp/out"), [{"a": "b"}, {"e": "f"}, {"g": "h"}])
        assert get("//tmp/out/@chunk_ids") == [
            get_singular_chunk_id("//tmp/written"),
            get_singular_chunk_id("//tmp/attached"),
            get_singular_chunk_id("//tmp/attached_read_only"),
        ]

    @authors("faucct")
    def test_attach_to_read_only_bucket(self):
        create("table", "//tmp/imported", attributes={"primary_medium": "s3_read_only"})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))
        assert read_table("//tmp/imported") == records
        assert not self.object_exists_in_s3(bucket, self.get_chunk_generated_meta_path(get("//tmp/imported/@chunk_ids")[0]))

    @authors("faucct")
    def test_attach_and_merge(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/merged")
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        merge(in_="//tmp/imported", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True})
        assert_items_equal(read_table("//tmp/merged"), records)

        merge(in_="//tmp/imported{y}", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True})
        assert_items_equal(read_table("//tmp/merged"), [{"y": "b"}, {"y": "a"}])

        merge(in_="//tmp/imported{x}", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True})
        assert_items_equal(read_table("//tmp/merged"), [{"x": 1}, {"x": 2}])

    @authors("faucct")
    def test_attach_and_merge_with_columnar_statistics(self):
        if self.NUM_OFFSHORE_DATA_GATEWAYS == 0:
            pytest.skip("This operation times out if no offshore data gateways are available")

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/merged")
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        merge(in_="//tmp/imported", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True, "use_columnar_statistics": True, "use_chunk_slice_statistics": False})
        assert_items_equal(read_table("//tmp/merged"), records)

        merge(in_="//tmp/imported{y}", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True, "use_columnar_statistics": True, "use_chunk_slice_statistics": False})
        assert_items_equal(read_table("//tmp/merged"), [{"y": "b"}, {"y": "a"}])

        merge(in_="//tmp/imported{x}", out="//tmp/merged", mode="ordered", spec={"combine_chunks": True, "use_columnar_statistics": True, "use_chunk_slice_statistics": False})
        assert_items_equal(read_table("//tmp/merged"), [{"x": 1}, {"x": 2}])

    @authors("faucct")
    def test_attach_to_nonexistent_bucket(self):
        medium_name = "s3_extra1"
        create("table", "//tmp/imported", attributes={"primary_medium": medium_name})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        set(f"//sys/media/{medium_name}/@config/bucket", "nonexistent")
        # Wait for medium directory synchronizer to do its thing.
        # TODO(achulkov2): This can be removed once/if we return medium directory entries along with chunk specs during fetch.
        time.sleep(1)

        with pytest.raises(YtError, match="The specified bucket does not exist"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_to_wrong_medium(self):
        create("table", "//tmp/imported")
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Cannot attach external data to table on non-offshore medium "default"'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_wrong_bucket(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        with pytest.raises(YtError, match="Got status code 404 Not Found"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://not-{bucket}/foo.parquet"]))

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_with_schema_and_read(self):
        schema = [
            make_column("x", optional_type("int64"), type="int64", required=False),
            make_column("y", optional_type(list_type(optional_type(list_type(optional_type(
                struct_type([("foo", optional_type("int64"))])
            ))))), type="any", required=False),
        ]
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": schema,
        })
        bucket = self.get_s3_medium_bucket()
        records = [
            {"x": 1, "y": [[{"foo": 1}, {"foo": 3}]]},
            {"x": 2, "y": [[{"foo": 1}], []]},
        ]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), records)
        assert get(f"#{get("//tmp/imported/@chunk_ids")[0]}/@schema") == make_schema(schema, strict=True, unique_keys=False)

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_strings_without_schema_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([
                {"string": "a", "binary": b"b"},
                {"string": "c", "binary": b"d"},
            ]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), [
            {"string": "a", "binary": "b"},
            {"string": "c", "binary": "d"},
        ])

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_strings_with_schema_and_read(self):
        schema = [
            make_column("string", optional_type("utf8"), type="utf8", required=False),
            make_column("binary", optional_type("string"), type="string", required=False),
        ]
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": schema,
        })
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([
                {"string": "a", "binary": b"b"},
                {"string": "c", "binary": b"d"},
            ]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), [
            {"string": "a", "binary": "b"},
            {"string": "c", "binary": "d"},
        ])
        assert get(f"#{get("//tmp/imported/@chunk_ids")[0]}/@schema") == make_schema(schema, strict=True, unique_keys=False)

    @authors("faucct")
    def test_attach_columnar_statistics(self):
        schema = [
            make_column("int64", optional_type("int64"), type="int64", required=False),
            make_column("string", optional_type("string"), type="string", required=False),
            make_column("ints", optional_type(list_type(optional_type("int64"))), type="any", required=False),
            make_column("struct", optional_type(struct_type([("field", optional_type("string"))])), type="any", required=False),
            make_column("structs", optional_type(list_type(optional_type(list_type(optional_type(
                struct_type([("bar", optional_type("int64")), ("foo", optional_type("int64"))]),
            ))))), type="any", required=False),
        ]
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name(), "schema": schema})
        bucket = self.get_s3_medium_bucket()
        records = [
            {"int64": 1, "string": "a", "ints": [1], "struct": {"field": "foo"}, "structs": [[{"foo": 1, "bar": 4}, {"foo": 3, "bar": 6}]]},
            {"int64": 2, "string": "z", "ints": [2, 3], "struct": {"field": "bar"}, "structs": [[{"foo": 1, "bar": 5}], []]},
        ]

        def assert_int64_statistics(columnar_statistics):
            assert columnar_statistics["column_data_weights"]["int64"] > 0
            assert columnar_statistics["column_min_values"]["int64"] == 1
            assert columnar_statistics["column_max_values"]["int64"] == 2

        def assert_string_statistics(columnar_statistics):
            assert columnar_statistics["column_data_weights"]["string"] > 0
            assert columnar_statistics["column_min_values"]["string"] == "a"
            assert columnar_statistics["column_max_values"]["string"] == "z"

        def assert_ints_statistics(columnar_statistics):
            assert columnar_statistics["column_data_weights"]["ints"] > 0
            assert columnar_statistics["column_min_values"]["ints"] == yson.YsonEntity()
            assert columnar_statistics["column_max_values"]["ints"] == yson.YsonEntity()

        def assert_struct_statistics(columnar_statistics):
            assert columnar_statistics["column_data_weights"]["struct"] > 0
            assert columnar_statistics["column_min_values"]["struct"] == yson.YsonEntity()
            assert columnar_statistics["column_max_values"]["struct"] == yson.YsonEntity()

        def assert_structs_statistics(columnar_statistics):
            assert columnar_statistics["column_data_weights"]["structs"] > 0
            assert columnar_statistics["column_min_values"]["structs"] == yson.YsonEntity()
            assert columnar_statistics["column_max_values"]["structs"] == yson.YsonEntity()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))
        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        assert read_table("//tmp/imported") == records
        assert get(f"#{get("//tmp/imported/@chunk_ids")[0]}/@schema") == make_schema(schema, strict=True, unique_keys=False)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{int64,string,ints,struct,structs}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_int64_statistics(columnar_statistics)
        assert_string_statistics(columnar_statistics)
        assert_ints_statistics(columnar_statistics)
        assert_struct_statistics(columnar_statistics)
        assert_structs_statistics(columnar_statistics)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{int64}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_int64_statistics(columnar_statistics)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{string}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_string_statistics(columnar_statistics)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{ints}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_ints_statistics(columnar_statistics)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{struct}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_struct_statistics(columnar_statistics)

        columnar_statistics, = get_table_columnar_statistics('["//tmp/imported{structs}";]')
        assert columnar_statistics["chunk_row_count"] == 2
        assert_structs_statistics(columnar_statistics)

    @authors("faucct")
    def test_attach_extra_with_strict_schema(self):
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": [make_column("x", optional_type("int64"), type="int64", required=False)],
        })
        bucket = self.get_s3_medium_bucket()
        records = [
            {"x": 1, "y": [[{"foo": 1}, {"foo": 3}]]},
        ]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Column "y" is found in input schema but is missing in output schema'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_with_not_nullable_schema(self):
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": [make_column("x", "int64")],
        })
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1}, {"x": 2}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Type of ".<optional-element>" field is modified in non backward compatible manner'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_with_not_required_schema(self):
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": [{"name": "x", "type": "int64", "required": False}],
        })
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1}, {"x": 2}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_with_erasure_codec(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name(), "erasure_codec": "reed_solomon_6_3"})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Cannot attach external data to table with erasure codec'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    def test_attach_with_mismatching_weak_schema(self):
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": make_schema([make_column("x", "int64")], strict=False),
        })
        bucket = self.get_s3_medium_bucket()
        records = [{"x": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Column "x" input type is incompatible with output type'):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("faucct")
    @pytest.mark.parametrize("allow_incompatible_source_schemas", [True, False])
    def test_attach_with_different_schemas_and_read(self, allow_incompatible_source_schemas):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1}
        record2 = {"x": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))

        if not allow_incompatible_source_schemas:
            with pytest.raises(YtError, match='Column "x" first schema type is incompatible with second schema type'):
                attach_table(
                    "//tmp/imported",
                    FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]),
                    allow_incompatible_source_schemas=allow_incompatible_source_schemas)
            return

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]),
            allow_incompatible_source_schemas=allow_incompatible_source_schemas)

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

        assert_items_equal([
            make_schema([], strict=False, unique_keys=False),
            make_schema([], strict=False, unique_keys=False),
        ], [get(f"#{chunk_id}/@schema") for chunk_id in get("//tmp/imported/@chunk_ids")])

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_attach_multiple_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_overwriting_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), [record2])

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_appending_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.parquet"]))

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

    @authors("faucct")
    @pytest.mark.run_with_no_offshore_data_gateways
    def test_write_and_appending_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        write_table("//tmp/imported", [record1])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.parquet"]))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.jsonl", Body='{"x": 3, "y": "d"}\n')
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.jsonl"]))

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.csv", Body='x,y\n4,c\n')
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/bar.csv"]))

        assert_items_equal(read_table("//tmp/imported"), [record1, record2, {"x": 3, "y": "d"}, {"x": 4, "y": "c"}])

    # TODO(achulkov2): test behaviour with chunks with multiple types of replicas once that is supported
    @authors("faucct")
    def test_attach_and_retry_map(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1, record2]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))
        secret_access_key = get(f"//sys/media/{self.get_s3_medium_name()}/@config/secret_access_key")
        set(f"//sys/media/{self.get_s3_medium_name()}/@config/secret_access_key", "foo")

        op = map(
            command="cat",
            in_="//tmp/imported",
            out="//tmp/out",
            spec={
                "max_failed_job_count": 1000,
                "job_io": {
                    "table_reader": {
                        # With this parameter the jobs will be aborted after
                        # 10s of unsuccessful attempts to read the data.
                        "session_timeout": "5s",
                    },
                },
            },
            track=False,
        )

        wait(lambda: op.get_job_count("aborted") >= 1)
        assert op.get_state() == "running"
        set(f"//sys/media/{self.get_s3_medium_name()}/@config/secret_access_key", secret_access_key)
        op.track()
        assert_items_equal(read_table("//tmp/out"), [record1, record2])

    @authors("faucct")
    def test_attach_and_map(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1, record2]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))
        op = map(
            command="cat",
            in_="//tmp/imported",
            out="//tmp/out",
            spec={"max_failed_job_count": 1})

        assert_items_equal(read_table("//tmp/out"), [record1, record2])

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]

        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_read_from_disk") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_read_from_disk", summary_type="count") > 0

    @authors("faucct")
    def test_attach_and_map_non_strict_schema(self):
        create("table", "//tmp/imported", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "schema": make_schema([
                make_column("x", optional_type("int64"), type="int64", required=False),
            ], strict=False),
        })
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1, record2]))
        ))

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))
        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

        map(
            command="cat",
            in_="//tmp/imported",
            out="//tmp/out",
            spec={"max_failed_job_count": 1})
        assert_items_equal(read_table("//tmp/out"), [record1, record2])

    @authors("pavel-bash")
    @pytest.mark.run_with_no_offshore_data_gateways
    @pytest.mark.parametrize("file_format", ["parquet", "jsonl", "csv"])
    @pytest.mark.parametrize("strategy", ["fast", "precise"])
    @pytest.mark.parametrize("with_pivot_keys", [True, False])
    def test_attach_and_sort(self, file_format, strategy, with_pivot_keys):
        if self.NUM_OFFSHORE_DATA_GATEWAYS == 0 and not with_pivot_keys:
            # The samples cannot be produced without OffshoreDataGateways and there are
            # no pivot keys, so the test will just time out.
            return

        unsorted_input = [
            {"key": 420, "value": "hello"},
            {"key": 80085, "value": "howdy"},
            {"key": 1, "value": "hi"},
            {"key": 1337, "value": "Ehehe"},
        ]
        bucket = self.get_s3_medium_bucket()

        file_name = "foo." + file_format
        self.S3_CLIENT.put_object(
            Bucket=bucket,
            Key=file_name,
            Body=self.serialize_data_for_s3(unsorted_input, file_format)
        )

        attributes = {
            "primary_medium": self.get_s3_medium_name(),
            "schema": make_schema([
                make_column("key", optional_type("int64"), type="int64", required=False),
                make_column("value", optional_type("string"), type="string", required=False),
            ])
        }

        create("table", "//tmp/imported", attributes=attributes)
        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/{file_name}"]), sample_strategy=strategy)

        create("table", "//tmp/output", attributes=attributes)

        spec = {}
        if with_pivot_keys:
            spec = {"pivot_keys": [
                # The values of pivot keys must increase.
                [unsorted_input[2]["key"]],
                [unsorted_input[3]["key"]],
            ]}
        sort(
            in_="//tmp/imported",
            out="//tmp/output",
            sort_by=["key"],
            spec=spec,
        )
        assert read_table("//tmp/output") == sorted(unsorted_input, key=lambda row: row["key"])

        spec = {}
        if with_pivot_keys:
            spec = {"pivot_keys": [
                [unsorted_input[3]["value"]],
                [unsorted_input[0]["value"]]
            ]}
        sort(
            in_="//tmp/imported",
            out="//tmp/output",
            sort_by=["value"],
            spec=spec)
        assert read_table("//tmp/output") == sorted(unsorted_input, key=lambda row: row["value"])

    @authors("pavel-bash")
    def test_attach_and_partition_table(self):
        # This test works the same way as the previous one - attach, sort, perform operation - for
        # the same reasons.
        unsorted_input = [
            {"key": 420, "value": "hello"},
            {"key": 80085, "value": "howdy"},
            {"key": 1, "value": "hi"},
            {"key": 1337, "value": "Ehehe"},
        ]
        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(unsorted_input))
        ))

        attributes = {
            "primary_medium": self.get_s3_medium_name(),
            "schema": make_schema([
                make_column("key", optional_type("int64"), type="int64", required=False),
                make_column("value", optional_type("string"), type="string", required=False),
            ])
        }

        create("table", "//tmp/imported", attributes=attributes)
        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

        create("table", "//tmp/intermediate", attributes={
            "primary_medium": self.get_s3_medium_name(),
        })
        sort(in_="//tmp/imported", out="//tmp/intermediate", sort_by=["key"])

        # Specifying data_weight_per_partition=1 gives us as many partitions as we have entries.
        partition_result = partition_tables(["//tmp/intermediate"], data_weight_per_partition=1)
        assert len(partition_result) == len(unsorted_input)

    @authors("faucct")
    def test_attach_jsonl_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]))

        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("faucct")
    def test_attach_csv_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"col\na\nb\n")

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]))

        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("achulkov2")
    def test_sorted_attach_strict_schema(self):
        create(
            "table",
            "//tmp/imported",
            attributes={
                "primary_medium": self.get_s3_medium_name(),
                "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}],
            },
        )
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"a\n43\n15\n")

        with pytest.raises(YtError, match="Table schemas are incompatible"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]))

    @authors("achulkov2")
    def test_sorted_attach_weak_schema(self):
        create("table", "//tmp/imported")

        write_table("//tmp/imported", [{"a": 43, "b": 5}, {"a": 15, "b": 7}])
        sort(in_="//tmp/imported", out="//tmp/imported", sort_by=["a"])

        assert_items_equal(read_table("//tmp/imported"), [{"a": 15, "b": 7}, {"a": 43, "b": 5}])
        # Column "a" was automatically inferred, since it is the sort key.
        assert len(get("//tmp/imported/@schema")) == 1
        assert get("//tmp/imported/@sorted")
        assert get("//tmp/imported/@schema_mode") == "weak"

        set("//tmp/imported/@primary_medium", self.get_s3_medium_name())

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"a\n27\n99\n")
        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]))

        assert get("//tmp/imported/@schema") == make_schema([], strict=False, unique_keys=False)
        assert get("//tmp/imported/@schema_mode")
        assert not get("//tmp/imported/@sorted")

    @authors("achulkov2")
    def test_sorted_attach_chunk_sort_columns(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"a\n15\n43\n")

        with pytest.raises(YtError, match="Table schemas are incompatible"):
            attach_table("<chunk_sort_columns=[a]>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]))

    @authors("achulkov2")
    def test_attach_wrong_uri(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        with pytest.raises(YtError, match="unexpected scheme"):
            attach_table("//tmp/imported", FilesExternalSourceSpec(["ftp://example.com/foo.parquet"]))

        with pytest.raises(YtError, match="Cannot deduce external source format"):
            attach_table("//tmp/imported", FilesExternalSourceSpec(["s3://my-bucket/foo.brakozyabra"]))

    @authors("achulkov2")
    def test_external_transaction_commit(self):
        tx = start_transaction()

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()}, tx=tx)

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), tx=tx)

        assert not exists("//tmp/imported")
        assert exists("//tmp/imported", tx=tx)

        assert_items_equal(read_table("//tmp/imported", tx=tx), [{"col": "a"}, {"col": "b"}])

        commit_transaction(tx)

        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("achulkov2")
    def test_external_transaction_abort(self):
        tx = start_transaction()

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()}, tx=tx)

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), tx=tx)

        assert not exists("//tmp/imported")
        assert exists("//tmp/imported", tx=tx)

        chunk_ids = get("//tmp/imported/@chunk_ids", tx=tx)
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert_items_equal(read_table("//tmp/imported", tx=tx), [{"col": "a"}, {"col": "b"}])

        abort_transaction(tx)

        assert not exists("//tmp/imported")

        wait(lambda: not exists(f"#{chunk_id}"))

    @authors("achulkov2")
    def test_external_transaction_append_order(self):
        tx1 = start_transaction()
        tx2 = start_transaction()
        tx3 = start_transaction()

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo1.jsonl", Body=b'{"col":"a"}\n')
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo2.jsonl", Body=b'{"col":"b"}\n')
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo3.jsonl", Body=b'{"col":"c"}\n')

        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo1.jsonl"]), tx=tx1)
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo2.jsonl"]), tx=tx2)
        attach_table("<append=%true>//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo3.jsonl"]), tx=tx3)

        commit_transaction(tx2)
        commit_transaction(tx1)
        commit_transaction(tx3)

        # Single chunk per attach, so order is guaranteed.
        assert read_table("//tmp/imported") == [{"col": "b"}, {"col": "a"}, {"col": "c"}]

    @authors("achulkov2")
    def test_separate_bucket(self):
        bucket1 = self.EXTRA_BUCKETS[0]
        bucket2 = self.EXTRA_BUCKETS[1]

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        self.S3_CLIENT.put_object(Bucket=bucket1, Key="foo.jsonl", Body=b'{"col":"a"}\n')
        self.S3_CLIENT.put_object(Bucket=bucket2, Key="bar.jsonl", Body=b'{"col":"b"}\n')

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket1}/foo.jsonl", f"s3://{bucket2}/bar.jsonl"]))
        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("pavel-bash")
    @pytest.mark.parametrize("table_should_exist_before", [True, False])
    def test_no_permission(self, table_should_exist_before):
        if table_should_exist_before:
            create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        create_user("u")
        set(f"//sys/media/{self.get_s3_medium_name()}/@acl", [make_ace("deny", "u", "use")])

        with pytest.raises(YtError, match="User has no access to medium"):
            attach_table(
                "//tmp/imported",
                FilesExternalSourceSpec(["s3://foo/foo.parquet"]),
                medium=(None if table_should_exist_before else self.get_s3_medium_name()),
                authenticated_user="u")

    @authors("pavel-bash")
    def test_table_exists_specify_same_medium(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]),
            medium=self.get_s3_medium_name())
        assert_items_equal(read_table("//tmp/imported"), records)

    @authors("pavel-bash")
    def test_table_exists_specify_another_medium(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        error_match = f'its medium "{self.get_s3_medium_name()}" is not the same as the specified one "{self.get_s3_medium_name(1)}"'
        with pytest.raises(YtError, match=error_match):
            attach_table(
                "//tmp/imported",
                FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]),
                medium=self.get_s3_medium_name(1))

    @authors("pavel-bash")
    @pytest.mark.parametrize("medium_index", [0, 1])
    def test_attach_non_existing_table(self, medium_index):
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]),
            medium=self.get_s3_medium_name(medium_index))

        assert_items_equal(read_table("//tmp/imported"), records)

        new_table_media = get("//tmp/imported/@media")
        assert len(new_table_media) == 1
        assert self.get_s3_medium_name(medium_index) in new_table_media

        assert get("//tmp/imported/@schema_mode") == "strong"

    @authors("pavel-bash")
    def test_attach_non_existing_table_no_medium(self):
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match="check that it exists or specify medium in the parameters"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]))

    @authors("pavel-bash")
    def test_attach_non_existing_table_wrong_medium(self):
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Cannot attach external data to table on non-offshore medium "default"'):
            attach_table(
                "//tmp/imported",
                FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]),
                medium="default")

    @authors("pavel-bash")
    @pytest.mark.parametrize("allow_incompatible_source_schemas", [True, False])
    def test_attach_non_existing_table_incompatible_schemas(self, allow_incompatible_source_schemas):
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1}
        record2 = {"x": "2"}
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))

        if not allow_incompatible_source_schemas:
            with pytest.raises(YtError, match='Column "x" first schema type is incompatible with second schema type'):
                attach_table(
                    "//tmp/imported",
                    FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]),
                    allow_incompatible_source_schemas=allow_incompatible_source_schemas,
                    medium=self.get_s3_medium_name())
            return

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]),
            allow_incompatible_source_schemas=allow_incompatible_source_schemas,
            medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

        assert_items_equal([
            make_schema([], strict=False, unique_keys=False),
            make_schema([], strict=False, unique_keys=False),
        ], [get(f"#{chunk_id}/@schema") for chunk_id in get("//tmp/imported/@chunk_ids")])

    class InferenceTestData(object):
        def __init__(self, record1, record2, columns):
            self.record1 = record1
            self.record2 = record2
            self.schema = make_schema(columns, strict=True, unique_keys=False)

        record1: list[dict]
        record2: list[dict]
        schema: YsonList

    inference_test_parameters = [
        InferenceTestData(
            record1=[
                {"x": 1},
            ],
            record2=[
                {"x": 2},
            ],
            columns=[
                make_column("x", optional_type("int64"), type="int64", required=False),
            ],
        ),
        InferenceTestData(
            record1=[
                {"x": 1.0},
            ],
            record2=[
                {"x": 2.0},
            ],
            columns=[
                make_column("x", optional_type("double"), type="double", required=False),
            ],
        ),
        InferenceTestData(
            record1=[
                {"x": 1},
            ],
            record2=[
                {"y": "2"},
            ],
            columns=[
                make_column("x", optional_type("int64"), type="int64", required=False),
                make_column("y", optional_type("utf8"), type="utf8", required=False),
            ],
        ),
        InferenceTestData(
            record1=[
                {"x": 1, "y": 2},
            ],
            record2=[
                {"y": 3},
            ],
            columns=[
                make_column("x", optional_type("int64"), type="int64", required=False),
                make_column("y", optional_type("int64"), type="int64", required=False),
            ],
        ),
        InferenceTestData(
            record1=[
                {"x": 1, "y": [[{"foo": 1}, {"foo": 3}]]},
            ],
            record2=[
                {"x": 2, "y": [[{"foo": 1}], []]},
            ],
            columns=[
                make_column("x", optional_type("int64"), type="int64", required=False),
                make_column("y", optional_type(list_type(optional_type(list_type(optional_type(
                    struct_type([("foo", optional_type("int64"))])
                ))))), type="any", required=False),
            ],
        ),
    ]

    @authors("achulkov2")
    def test_json_schema_inference(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"x": 1}\n')
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.jsonl", Body=b'{"y": "2"}\n')
        self.S3_CLIENT.put_object(Bucket=bucket, Key="baz.jsonl", Body=b'{"z": {"a": 3, "b": 4}}\n')
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bat.jsonl", Body=b'{"t": {"a": 3, "b": 4}}\n{"t": {"e": 3, "f": 4}}\n')

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl", f"s3://{bucket}/bar.jsonl", f"s3://{bucket}/baz.jsonl", f"s3://{bucket}/bat.jsonl"]),
            medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/imported"), [
            {"x": 1},
            {"y": "2"},
            {"z": {"a": 3, "b": 4}},
            {"t": {"a": 3, "b": 4, "e": YsonEntity(), "f": YsonEntity()}},
            {"t": {"a": YsonEntity(), "b": YsonEntity(), "e": 3, "f": 4}}
        ])

        assert_items_equal(normalize_schema_v3(get("//tmp/imported/@schema")), make_schema([
            make_column("x", optional_type("int64")),
            make_column("y", optional_type("utf8")),
            make_column("z", optional_type(struct_type([
                ("a", optional_type("int64")),
                ("b", optional_type("int64")),
            ]))),
            make_column("t", optional_type(struct_type([
                ("a", optional_type("int64")),
                ("b", optional_type("int64")),
                ("e", optional_type("int64")),
                ("f", optional_type("int64")),
            ]))),
        ]))

        if self.NUM_OFFSHORE_DATA_GATEWAYS:
            min_entity = yson.YsonEntity()
            min_entity.attributes["type"] = "min"
            max_entity = yson.YsonEntity()
            max_entity.attributes["type"] = "max"

            columnar_statistics, = get_table_columnar_statistics('["//tmp/imported"]')
            assert columnar_statistics["chunk_row_count"] == 5
            self.assert_columnar_statistics(columnar_statistics, "x", data_weight=8, min_value=1, max_value=1)
            self.assert_columnar_statistics(columnar_statistics, "y", data_weight=1, min_value="2", max_value="2")
            self.assert_columnar_statistics(columnar_statistics, "z", data_weight=8, min_value=min_entity, max_value=max_entity)
            self.assert_columnar_statistics(columnar_statistics, "t", data_weight=24, min_value=min_entity, max_value=max_entity)

    @authors("achulkov2")
    def test_json_single_file_incompatible_types(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"x": 1}\n{"x": "2"}\n')

        with pytest.raises(YtError, match="JSON parse error"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), medium=self.get_s3_medium_name())

    @authors("achulkov2")
    def test_csv_schema_inference(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"x\na\n")
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.csv", Body=b"y\n2\n")
        self.S3_CLIENT.put_object(Bucket=bucket, Key="baz.csv", Body=b"z.a,z.b\n3.0,4\n1,5\n")

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.csv", f"s3://{bucket}/bar.csv", f"s3://{bucket}/baz.csv"]),
            medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/imported"), [
            {"x": "a"},
            {"y": 2},
            {"z.a": 3.0, "z.b": 4},
            {"z.a": 1.0, "z.b": 5}
        ])

        assert_items_equal(normalize_schema_v3(get("//tmp/imported/@schema")), make_schema([
            make_column("x", optional_type("utf8")),
            make_column("y", optional_type("int64")),
            make_column("z.a", optional_type("double")),
            make_column("z.b", optional_type("int64")),
        ]))

        if self.NUM_OFFSHORE_DATA_GATEWAYS:
            columnar_statistics, = get_table_columnar_statistics('["//tmp/imported"]')
            assert columnar_statistics["chunk_row_count"] == 4
            self.assert_columnar_statistics(columnar_statistics, "x", data_weight=1, min_value="a", max_value="a")
            self.assert_columnar_statistics(columnar_statistics, "y", data_weight=8, min_value=2, max_value=2)
            self.assert_columnar_statistics(columnar_statistics, "z.a", data_weight=16, min_value=1.0, max_value=3.0)
            self.assert_columnar_statistics(columnar_statistics, "z.b", data_weight=16, min_value=4, max_value=5)

    @authors("pavel-bash")
    @pytest.mark.parametrize("data", inference_test_parameters)
    def test_attach_non_existing_table_check_schema_inference(self, data: InferenceTestData):
        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(data.record1))
        ))
        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(data.record2))
        ))

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"]),
            medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/imported"), [*data.record1, *data.record2])
        for chunk_id in get("//tmp/imported/@chunk_ids"):
            assert_items_equal(get(f"#{chunk_id}/@schema"), data.schema)
        assert_items_equal(get("//tmp/imported/@schema"), data.schema)

    @authors("achulkov2")
    def test_format_specification(self):
        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.custom", Body=b'{"col":"a"}\n{"col":"b"}\n')

        with pytest.raises(YtError, match="Cannot deduce external source format"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.custom"]), medium=self.get_s3_medium_name())

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo.custom"]),
            medium=self.get_s3_medium_name(),
            source_format="jsonl")

        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("achulkov2")
    def test_invalid_json(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"\n')

        with pytest.raises(YtError, match="JSON parse error"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), medium=self.get_s3_medium_name())

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.json", Body=b'{"col":"a"}\n{"col":"b"\n')

        with pytest.raises(YtError, match="JSON parse error"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.json"]), medium=self.get_s3_medium_name())

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.bar", Body=b'{"col":"a"}\n{"col":"b"\n')

        with pytest.raises(YtError, match="JSON parse error"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.bar"]), medium=self.get_s3_medium_name(), source_format="jsonl")

    @authors("achulkov2")
    def test_invalid_csv(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b'c1, c2\na,b,c,d\n')

        with pytest.raises(YtError, match="CSV parse error"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]), medium=self.get_s3_medium_name())

    @authors("faucct")
    @pytest.mark.parametrize("count,max_block_size", [
        (1 << 15, 185_498),
        (1 << 16, 382_106),
        (1 << 17, 806_394),
        (1 << 18, 1_048_573),
        (1 << 19, 1_048_579),
    ])
    def test_csv_blocks(self, count, max_block_size):
        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=f'c\n{'\n'.join(builtins.map(str, range(count)))}\n'.encode())
        chunk_info, = attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.csv"]), medium=self.get_s3_medium_name())["chunk_infos"]

        assert_items_equal([row["c"] for row in read_table("//tmp/imported", verbose=False)], list(range(count)))
        assert get(f"//sys/chunks/{chunk_info["chunk_id"]}/@max_block_size") == max_block_size

    @authors("faucct")
    @pytest.mark.parametrize("count,max_block_size", [
        (1 << 14, 201_882),
        (1 << 15, 414_874),
        (1 << 16, 840_858),
        (1 << 17, 1_048_572),
        (1 << 18, 1_048_580),
    ])
    def test_json_blocks(self, count, max_block_size):
        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=f'{'\n'.join(
            json.dumps({"c": i}) for i in range(count)
        )}\n'.encode())
        chunk_info, = attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), medium=self.get_s3_medium_name())["chunk_infos"]

        assert_items_equal([row["c"] for row in read_table("//tmp/imported", verbose=False)], list(range(count)))
        assert get(f"//sys/chunks/{chunk_info["chunk_id"]}/@max_block_size") == max_block_size

    @authors("faucct")
    def test_json_blocks_with_disappearing_columns(self):
        bucket = self.get_s3_medium_bucket()
        count = 1 << 19
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=('{"c":1}\n' + '{}\n' * count).encode())
        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), medium=self.get_s3_medium_name())["chunk_infos"]

        assert_items_equal(
            [yson.dumps(row) for row in read_table("//tmp/imported", verbose=False)],
            [b'{"c"=1;}'] + [b'{"c"=#;}'] * count,
        )

    @authors("achulkov2_forgot_to_lock_laptop")
    def test_invalid_parquet(self):
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=b'not a parquet')

        with pytest.raises(YtError, match="not a parquet file"):
            attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.parquet"]), medium=self.get_s3_medium_name())

    @authors("achulkov2")
    def test_many_files(self):
        bucket = self.get_s3_medium_bucket()

        NUM_FILES = 300
        for i in range(NUM_FILES):
            self.S3_CLIENT.put_object(Bucket=bucket, Key=f"test_many_files/foo_{i}.jsonl", Body=f'{{"col":"{i}"}}\n')

        attach_table("//tmp/imported", PrefixExternalSourceSpec(prefix_uri=f"s3://{bucket}/test_many_files/"), medium=self.get_s3_medium_name(), attach_mode="parallel")

        assert_items_equal(read_table("//tmp/imported"), [{"col": str(i)} for i in range(NUM_FILES)])

    @authors("achulkov2")
    def test_many_files_single_parsing_error(self):
        bucket = self.get_s3_medium_bucket()
        test_dir = "test_many_files_single_parsing_error"

        NUM_FILES = 100
        for i in range(NUM_FILES):
            key = f"{test_dir}/foo_{i}.jsonl" if i == 42 else f"{test_dir}/foo_{i}.parquet"
            self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=self.dump_arrow_table_as_bytes(
                pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": str(i)}]))
            ))

        with pytest.raises(YtError, match="JSON parse error"):
            attach_table("//tmp/imported", PrefixExternalSourceSpec(prefix_uri=f"s3://{bucket}/{test_dir}/"), medium=self.get_s3_medium_name(), attach_mode="parallel")

    @authors("achulkov2")
    def test_many_files_incompatible_schema(self):
        bucket = self.get_s3_medium_bucket()
        test_dir = "test_many_files_incompatible_schema"

        NUM_FILES = 300
        for i in range(NUM_FILES):
            key = f"{test_dir}/foo_{i}.parquet"
            if i == 42:
                self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=self.dump_arrow_table_as_bytes(
                    pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": i}]))
                ))
            else:
                self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=self.dump_arrow_table_as_bytes(
                    pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": str(i)}]))
                ))

        with pytest.raises(YtError, match="incompatible"):
            attach_table("//tmp/imported", PrefixExternalSourceSpec(prefix_uri=f"s3://{bucket}/{test_dir}/"), medium=self.get_s3_medium_name(), attach_mode="parallel")

    @authors("achulkov2")
    def test_many_files_weak_schema(self):
        bucket = self.get_s3_medium_bucket()
        test_dir = "test_many_files_weak_schema"

        NUM_FILES = 100
        for i in range(NUM_FILES):
            key = f"{test_dir}/foo_{i}.parquet"
            if i == 42:
                self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=self.dump_arrow_table_as_bytes(
                    pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": i}]))
                ))
            else:
                self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=self.dump_arrow_table_as_bytes(
                    pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": str(i)}]))
                ))

        attach_table(
            "//tmp/imported",
            PrefixExternalSourceSpec(prefix_uri=f"s3://{bucket}/{test_dir}/"),
            medium=self.get_s3_medium_name(),
            attach_mode="parallel",
            allow_incompatible_source_schemas=True)

        assert_items_equal(read_table("//tmp/imported"), [{"col": str(i)} if i != 42 else {"col": 42} for i in range(NUM_FILES)])
        assert get("//tmp/imported/@schema") == make_schema([], strict=False, unique_keys=False)
        assert get("//tmp/imported/@schema_mode") == "weak"

    @authors("achulkov2")
    def test_many_files_failed_chunk_service_request(self):
        bucket = self.get_s3_medium_bucket()
        test_dir = "test_many_files_failed_chunk_service_request"

        NUM_FILES = 300

        create_account(
            "s3_test_account_with_limits",
            attributes={
                "resource_limits": {
                    "disk_space_per_medium": {
                        self.get_s3_medium_name(): 1 * GB,
                    },
                    "chunk_count": NUM_FILES * 2 // 3,
                    "node_count": 1000,
                }
            },
        )

        for i in range(NUM_FILES):
            self.S3_CLIENT.put_object(Bucket=bucket, Key=f"{test_dir}/foo_{i}.jsonl", Body=f'{{"col":"{i}"}}\n')

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name(), "account": "s3_test_account_with_limits"})

        with raises_yt_error(yt_error_codes.AccountLimitExceeded):
            attach_table("//tmp/imported", PrefixExternalSourceSpec(prefix_uri=f"s3://{bucket}/{test_dir}/"), medium=self.get_s3_medium_name(), attach_mode="parallel")

    @authors("achulkov2")
    def test_sequential_attach(self):
        bucket = self.get_s3_medium_bucket()

        NUM_FILES = 30

        file_keys = [f"test_sequential_attach_mode/foo_{i}.jsonl" for i in range(NUM_FILES)]

        for i, key in enumerate(file_keys):
            self.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=f'{{"col":"{i}"}}\n')

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/{key}" for key in file_keys]), medium=self.get_s3_medium_name(), attach_mode="sequential")

        assert read_table("//tmp/imported") == [{"col": str(i)} for i in range(NUM_FILES)]

    @authors("achulkov2")
    def test_quota_accounting(self):
        bucket = self.get_s3_medium_bucket()

        body = b'{"col":"a"}\n{"col":"b"}\n'

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=body)

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"]), medium=self.get_s3_medium_name())

        account = get("//tmp/imported/@account")
        used_space = get(f"//sys/accounts/{account}/@resource_usage/disk_space_per_medium/{self.get_s3_medium_name()}")
        assert used_space >= len(body)

    # TODO(pavel-bash): Add test to check SampleStrategy parameter when the meta generated during attach
    # will be saved so S3; we'll be able to read it and actually check which samples were selected.

    # TODO(pavel-bash): Add test to check the sampling when we can infer the sorted schema of the
    # attached tables (run an operation over an attached sorted table).

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_node_proxies
    def test_attach_duplicate_source_uris(self):
        bucket = self.get_s3_medium_bucket()

        chunk_count = 3

        body = b'{"col":"a"}\n{"col":"b"}\n'
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=body)

        attach_table("//tmp/imported", FilesExternalSourceSpec([f"s3://{bucket}/foo.jsonl"] * chunk_count), medium=self.get_s3_medium_name(), attach_mode="sequential")

        assert get("//tmp/imported/@chunk_count") == chunk_count

        assert read_table("//tmp/imported") == [{"col": "a"}, {"col": "b"}] * chunk_count

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_node_proxies
    def test_attached_data_chunk_merger(self):
        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        bucket = self.get_s3_medium_bucket()

        chunk_count = 10

        # TODO(achulkov2): Test all that none of the supported formats trigger actual chunk merging after rebasing on commits with Pasha's new function.
        body = b'{"col":"a"}\n{"col":"b"}\n'

        for i in range(chunk_count):
            self.S3_CLIENT.put_object(Bucket=bucket, Key=f"foo_{i}.jsonl", Body=body)

        attach_table(
            "//tmp/imported",
            FilesExternalSourceSpec([f"s3://{bucket}/foo_{i}.jsonl" for i in range(chunk_count)]),
            medium=self.get_s3_medium_name(),
            attach_mode="sequential")

        assert read_table("//tmp/imported") == [{"col": "a"}, {"col": "b"}] * chunk_count

        assert get("//tmp/imported/@chunk_count") == chunk_count

        set("//tmp/imported/@chunk_merger_mode", "auto")

        time.sleep(10)

        merged_chunk_count = get("//tmp/imported/@chunk_count")
        assert merged_chunk_count == chunk_count


################################################################################


class TestAttachTableRpcProxy(TestAttachTable):
    NUM_OFFSHORE_DATA_GATEWAYS = 1
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


################################################################################


class TestAttachTableNoOffshoreDataGateways(TestAttachTable):
    NUM_OFFSHORE_DATA_GATEWAYS = 0

    DELTA_DRIVER_CONFIG = {
        "enable_replication_reader_for_offshore_data": False,
    }

    DELTA_NODE_CONFIG = {
        "cluster_connection": {
            "enable_replication_reader_for_offshore_data": False,
        },
    }


################################################################################

class TestAttachTableApiSpecification(TestAttachTableBase):
    # Add configuration here.

    LETTER_TO_URI = {}

    @classmethod
    def put_object(cls, bucket, key, body, letter):
        cls.S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=body)
        cls.LETTER_TO_URI[letter] = f"s3://{bucket}/{key}"

    @classmethod
    def init_source_files(cls):
        bucket = cls.get_s3_medium_bucket()

        cls.put_object(bucket, "first_level_dir_0/f1.jsonl", b'{"col":"a"}\n', "a")
        cls.put_object(bucket, "first_level_dir_0/f2.jsonl", b'{"col":"b"}\n', "b")
        cls.put_object(bucket, "first_level_dir_0/f3.csv", b"col\nc\n", "c")
        cls.put_object(bucket, "first_level_dir_1/f4.csv", b"col\nd\n", "d")
        cls.put_object(bucket, "first_level_dir_1/second_level_dir/f5.csv", b"col\ne\n", "e")
        cls.put_object(bucket, "first_level_dir_1/second_level_dir/f6.jsonl", b'{"col":"f"}\n', "f")
        cls.put_object(bucket, "f8.parquet", cls.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": "g"}]))
        ), "g")
        cls.put_object(bucket, "first_level_dir_1/second_level_dir/third_level_dir/f9.parquet", cls.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": "h"}]))
        ), "h")

        bucket1 = cls.EXTRA_BUCKETS[0]

        cls.put_object(bucket1, "f10.jsonl", b'{"col":"i"}\n\n', "i")
        cls.put_object(bucket1, "first_level_dir_0/f11.csv", b"col\nj\n", "j")
        cls.put_object(bucket1, "first_level_dir_2/second_level_dir/f12.parquet", cls.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": "k"}]))
        ), "k")
        cls.put_object(bucket1, "f13.csv", b"col\nl\n", "l")

        bucket2 = cls.EXTRA_BUCKETS[1]
        cls.put_object(bucket2, "f14.jsonl", b'{"col":"m"}\n\n', "m")
        cls.put_object(bucket2, "first_level_dir_3/f15.csv", b"col\nn\n", "n")
        cls.put_object(bucket2, "f16.parquet", cls.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([{"col": "o"}]))
        ), "o")
        cls.put_object(bucket2, "f17.csv", b"col\np\n", "p")
        cls.put_object(bucket2, "first_level_dir_4/f18.jsonl", b'{"col":"q"}\n\n', "q")
        cls.put_object(bucket2, "first_level_dir_4/f19.brakozyabra", b'{"col":"r"}\n\n', "r")

    def setup_method(self, method):
        super(TestAttachTableApiSpecification, self).setup_method(method)

        self.init_source_files()

    def teardown_method(self, method):
        self.LETTER_TO_URI.clear()

        # This will remove all created files.
        super(TestAttachTableApiSpecification, self).teardown_method(method)

    @classmethod
    def check_source_spec(cls, source_spec, expected_row_letters, **kwargs):
        table_name = f"//tmp/imported_{uuid.uuid4().hex}"
        attached_chunk_infos = attach_table(table_name, source_spec=source_spec, medium=cls.get_s3_medium_name(index=1), **kwargs)

        assert_items_equal(read_table(table_name), [{"col": x} for x in expected_row_letters])

        assert attached_chunk_infos["total_chunk_count"] == len(expected_row_letters)
        assert attached_chunk_infos["total_row_count"] == len(expected_row_letters)
        assert attached_chunk_infos["total_uncompressed_data_size"] > 0

        assert len(attached_chunk_infos["chunk_infos"]) == len(expected_row_letters)

        attached_source_uris = {chunk_info["source_uri"] for chunk_info in attached_chunk_infos["chunk_infos"]}
        expected_source_uris = {cls.LETTER_TO_URI[x] for x in expected_row_letters}
        assert attached_source_uris == expected_source_uris

        assert_items_equal([chunk_info["chunk_id"] for chunk_info in attached_chunk_infos["chunk_infos"]], get(f"{table_name}/@chunk_ids"))

        for chunk_info in attached_chunk_infos["chunk_infos"]:
            assert chunk_info["row_count"] == 1
            assert chunk_info["uncompressed_data_size"] > 0

    @authors("achulkov2")
    def test_prefix_source_spec(self):
        bucket = self.get_s3_medium_bucket()
        bucket_1 = self.EXTRA_BUCKETS[0]
        bucket_2 = self.EXTRA_BUCKETS[1]

        # Default mode is recursive.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}"), "abcdefgh")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_1}"), "ijkl")

        # Additional slashes in the beginning of the key are ignored by us.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/"), "abcdefgh")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_1}//"), "ijkl")

        # Non-recursive mode. We add trailing slashes ourselves, if needed.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", recursive=False), "g")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_1}/", recursive=False), "il")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_0/", recursive=False), "abc")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_1", recursive=False), "d")

        # Multiple trailing slashes are trimmed.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_0///", recursive=False), "abc")

        # Include regexes.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*\.jsonl$"]), "abf")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*\.csv$"]), "cde")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*\.parquet$"]), "gh")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*\.jsonl$", r".*\.csv$"]), "abcdef")

        # Include/exclude regexes work on full relative path from the prefix.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r"first_level_dir_0.*"]), "abc")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", exclude_regexes=[r"first_level_dir_0.*"]), "defgh")
        # Even with trailing slashes in the beginning and end of the key!
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}////first_level_dir_1////", include_regexes=[r"second_level_dir/.*"]), "efh")
        # So don't add extra slashes in the filter patterns.
        with pytest.raises(YtError, match="At least one source must be specified"):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_1////", include_regexes=[r"/second_level_dir/.*"]), "efh")

        # Non-directory mode, i.e. searching by partial prefix, also works.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}/first_level"), "abcdefh")

        # In the second bucket there are some files that cannot be auto-parsed.
        with pytest.raises(YtError, match="Cannot deduce external source format"):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}"), "mnopqr")

        # Good example to test exclude regexes!
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", exclude_regexes=[r".*\.brakozyabra$"]), "mnopq")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", exclude_regexes=[r".*\.brakozyabra$", r".*\.jsonl$"]), "nop")
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", exclude_regexes=[r".*\.brakozyabra$", r".*\.jsonl$", r".*\.csv$"]), "o")

        # Or you can override format on your own. Last param is passed to attach_table.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", include_regexes=[r".*\.brakozyabra$", r".*\.jsonl$"]), "mqr", source_format="jsonl")

        # Excluding all files throws an error.
        with pytest.raises(YtError):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", exclude_regexes=[r".*\.brakozyabra$", r".*\.jsonl$", r".*\.csv$", r".*\.parquet$"]), "")

        # Excludes override includes.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket_2}", include_regexes=[r".*\.brakozyabra$", r".*\.jsonl$"], exclude_regexes=[r".*\.brakozyabra$"]), "mq")

    @authors("achulkov2")
    def test_prefix_source_spec_regex_cases(self):
        bucket = self.get_s3_medium_bucket()

        # Keep in mind, you are matching the relative path from the prefix, not the full S3 key.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r"first_level_dir_0/f1\.jsonl"]), "a")

        # Invalid regex.
        with pytest.raises(YtError, match="Error parsing RE2 regex"):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r"*\.jsonl$"]), "abf")

        # Regex that matches nothing.
        with pytest.raises(YtError, match="At least one source must be specified"):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r"nomatch.*jsonl$"]), "a")

        # Full match is performed, so end anchor is not required.
        self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*jsonl"]), "abf")

        # This, however, matches nothing!
        with pytest.raises(YtError, match="At least one source must be specified"):
            self.check_source_spec(PrefixExternalSourceSpec(f"s3://{bucket}", include_regexes=[r".*json"]), "abf")

    @authors("achulkov2")
    def test_files_source_spec(self):
        bucket = self.get_s3_medium_bucket()
        bucket_1 = self.EXTRA_BUCKETS[0]
        bucket_2 = self.EXTRA_BUCKETS[1]

        self.check_source_spec(FilesExternalSourceSpec([
            f"s3://{bucket}/first_level_dir_0/f1.jsonl",
            f"s3://{bucket}/first_level_dir_0/f2.jsonl",
            f"s3://{bucket}/first_level_dir_0/f3.csv",
            f"s3://{bucket}/first_level_dir_1/f4.csv",
            f"s3://{bucket}/first_level_dir_1/second_level_dir/f5.csv",
            f"s3://{bucket}/first_level_dir_1/second_level_dir/f6.jsonl",
            f"s3://{bucket}/f8.parquet",
            f"s3://{bucket}/first_level_dir_1/second_level_dir/third_level_dir/f9.parquet",
        ]), "abcdefgh")

        self.check_source_spec(FilesExternalSourceSpec([
            f"s3://{bucket_1}/f10.jsonl",
            f"s3://{bucket_1}/first_level_dir_0/f11.csv",
            f"s3://{bucket_1}/first_level_dir_2/second_level_dir/f12.parquet",
            f"s3://{bucket_1}/f13.csv",
            f"s3://{bucket_2}/f14.jsonl",
            f"s3://{bucket_2}/first_level_dir_3/f15.csv",
            f"s3://{bucket_2}/f16.parquet",
            f"s3://{bucket_2}/f17.csv",
            f"s3://{bucket_2}/first_level_dir_4/f18.jsonl",
        ]), "ijklmnopq")

        # Non-existing file.
        with pytest.raises(YtError, match="404 Not Found"):
            self.check_source_spec(FilesExternalSourceSpec([
                f"s3://{bucket}/first_level_dir_0/f1.jsonl",
                f"s3://{bucket}/non_existing_file.jsonl",
            ]), "a")

    @authors("achulkov2")
    def test_attached_chunk_infos(self):
        bucket = self.get_s3_medium_bucket()

        result = attach_table("//tmp/attached", source_spec=PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_0/", recursive=False), medium=self.get_s3_medium_name())

        assert_items_equal(read_table("//tmp/attached"), [{"col": x} for x in "abc"])

        print_debug(type(result), result)

        assert result["total_chunk_count"] == 3
        assert result["total_row_count"] == 3
        assert result["total_uncompressed_data_size"] > 0
        assert len(result["chunk_infos"]) == 3

        chunk_ids = get("//tmp/attached/@chunk_ids")
        assert_items_equal([info["chunk_id"] for info in result["chunk_infos"]], chunk_ids)
        assert_items_equal([info["source_uri"] for info in result["chunk_infos"]], [
            f"s3://{bucket}/first_level_dir_0/f1.jsonl",
            f"s3://{bucket}/first_level_dir_0/f2.jsonl",
            f"s3://{bucket}/first_level_dir_0/f3.csv",
        ])

        for attached_chunk_info in result["chunk_infos"]:
            assert attached_chunk_info["row_count"] == 1
            assert attached_chunk_info["uncompressed_data_size"] > 0
            assert attached_chunk_info["source_format"] in ("jsonl", "csv")
            assert attached_chunk_info["chunk_format"] in ("table_unversioned_arrow_csv", "table_unversioned_arrow_json_lines")
            assert attached_chunk_info["source_uri"].endswith(attached_chunk_info["source_format"])

    @authors("achulkov2")
    def test_source_order(self):
        bucket = self.get_s3_medium_bucket()

        attach_table("//tmp/attached", source_spec=FilesExternalSourceSpec([
            f"s3://{bucket}/first_level_dir_0/f2.jsonl",
            f"s3://{bucket}/first_level_dir_0/f1.jsonl",
            f"s3://{bucket}/first_level_dir_0/f3.csv",
        ]), medium=self.get_s3_medium_name(), attach_mode="sequential")
        assert read_table("//tmp/attached") == [{"col": x} for x in "bac"]

        attach_table("//tmp/attached", source_spec=FilesExternalSourceSpec([
            f"s3://{bucket}/first_level_dir_0/f2.jsonl",
            f"s3://{bucket}/first_level_dir_0/f1.jsonl",
            f"s3://{bucket}/first_level_dir_0/f3.csv",
        ]), medium=self.get_s3_medium_name(), attach_mode="sequential", source_order="lex_desc")
        assert read_table("//tmp/attached") == [{"col": x} for x in "cba"]

        attach_table(
            "//tmp/attached2",
            source_spec=PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_0/", recursive=False),
            medium=self.get_s3_medium_name(),
            attach_mode="sequential",
            source_order="lex_asc")
        assert read_table("//tmp/attached2") == [{"col": x} for x in "abc"]

        attach_table(
            "//tmp/attached3",
            source_spec=PrefixExternalSourceSpec(f"s3://{bucket}/first_level_dir_0/", recursive=False),
            medium=self.get_s3_medium_name(),
            attach_mode="sequential",
            source_order="lex_desc")
        assert read_table("//tmp/attached3") == [{"col": x} for x in "cba"]


################################################################################


class TestAttachTableApiSpecificationRpcProxy(TestAttachTableApiSpecification):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


################################################################################
