from yt_env_setup import (
    YTEnvSetup,
)
from yt.common import YtError

from yt_commands import (
    authors, sync_create_cells,
    create, create_domestic_medium, create_s3_medium, set, remove, exists,
    copy, move, get_singular_chunk_id, wait, get, concatenate,
    get_account_disk_space_limit, set_account_disk_space_limit,
    write_table, read_table, sync_mount_table, sync_unmount_table, insert_rows, sync_flush_table,
    map, raises_yt_error, select_rows, lookup_rows, print_debug, write_file, read_file)

import time
import pytest
import os
import uuid
import logging
import builtins
import string

import boto3
from botocore.exceptions import ClientError

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

    AWS_ENDPOINT_URL = "http://127.0.0.1:8080"
    AWS_REGION = "eu-north-1"
    AWS_ACCESS_KEY_ID = "S3_PROXY_IDENTITY"
    AWS_SECRET_ACCESS_KEY = "S3_PROXY_CREDENTIAL"

    # NB: The buckets defined above S3_MEDIA and EXTRA_BUCKETS are created for each test
    # invocation and cleaned up afterwards. In order to avoid interference between tests,
    # these are the *only* buckets that should be used in all the tests.

    S3_CLIENT = None

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_offshore_media": True,
        },
    }

    DELTA_MASTER_CONFIG = {
        "chunk_manager": {
            "allow_multiple_erasure_parts_per_node": True
        },
        "logging": {
            "abort_on_alert": False,
        },
    }

    MEDIUM_DIRECTORY_SYNCHRONIZER_DELTA = {
        "sync_period": 10,
        "use_cache": False,
    }
    CLUSTER_CONNECTION_DELTA = {
        "medium_directory_synchronizer": MEDIUM_DIRECTORY_SYNCHRONIZER_DELTA
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "forbid_operations_on_offshore_media": True,
        },
        "cluster_connection": CLUSTER_CONNECTION_DELTA
    }
    DELTA_DRIVER_CONFIG = {
        "medium_directory_synchronizer": MEDIUM_DIRECTORY_SYNCHRONIZER_DELTA
    }
    DELTA_RPC_DRIVER_CONFIG = {
        "medium_directory_synchronizer": MEDIUM_DIRECTORY_SYNCHRONIZER_DELTA
    }
    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_DELTA
    }
    DELTA_OFFSHORE_DATA_GATEWAY_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_DELTA
    }
    DELTA_NODE_CONFIG = {
        "cluster_connection": CLUSTER_CONNECTION_DELTA
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
            },
            "allow_offshore_media": True,
        }
    }

    @classmethod
    def make_random_string(cls, length):
        assert length >= 0
        return os.urandom(length).translate(TRANSLATE_TABLE).decode("ascii")

    @classmethod
    def get_buckets(cls):
        return (
            {medium["bucket"] for medium in cls.S3_MEDIA if "bucket" in medium}
            | builtins.set(cls.EXTRA_BUCKETS)
        )

    @classmethod
    def get_s3_medium(cls, index=0):
        return cls.S3_MEDIA[index]

    @classmethod
    def get_s3_medium_name(cls, index=0):
        return cls.get_s3_medium(index=index)["name"]

    @classmethod
    def get_s3_medium_config(cls, index=0):
        media = {**cls.S3_MEDIA[index]}
        del media["name"]
        return {
            # Standard S3 environment variables populated by local_s3_recipe.
            "url": os.getenv("AWS_ENDPOINT_URL", cls.AWS_ENDPOINT_URL),
            "region": os.getenv("AWS_REGION", cls.AWS_REGION),
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", cls.AWS_ACCESS_KEY_ID),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", cls.AWS_SECRET_ACCESS_KEY),
            # YT specific configuration.
            **media,
        }

    @classmethod
    def setup_s3_client(cls):
        cls.S3_CLIENT = boto3.client(
            service_name='s3',
            endpoint_url=cls.AWS_ENDPOINT_URL,
            aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY)

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
    def assert_chunk_exists_in_s3(cls, chunk_id, s3_medium_index=0, negate=False):
        bucket = cls.S3_MEDIA[s3_medium_index]["bucket"]

        chunk_path = cls.get_chunk_path(chunk_id, s3_medium_index)
        chunk_meta_path = cls.get_chunk_meta_path(chunk_id, s3_medium_index)

        if negate:
            assert not cls.object_exists_in_s3(bucket, chunk_path)
            assert not cls.object_exists_in_s3(bucket, chunk_meta_path)
        else:
            assert cls.object_exists_in_s3(bucket, chunk_path)
            assert cls.object_exists_in_s3(bucket, chunk_meta_path)

    @classmethod
    def assert_table_chunks_exist_in_s3(cls, table_name, s3_medium_index=0, negate=False):
        chunk_ids = get(f"{table_name}/@chunk_ids")
        for chunk_id in chunk_ids:
            cls.assert_chunk_exists_in_s3(chunk_id, s3_medium_index, negate)

    @classmethod
    def setup_class(cls, *args, **kwargs):
        super(TestS3MediumBase, cls).setup_class(*args, **kwargs)

        cls.setup_s3_client()

        disk_space_limit = get_account_disk_space_limit("tmp", "default")

        create_domestic_medium("hdd1")

        set("//sys/@config/chunk_manager/allow_offshore_media", True)

        for i in range(len(cls.S3_MEDIA)):
            name = cls.get_s3_medium_name(i)
            create_s3_medium(name, cls.get_s3_medium_config(i))
            set_account_disk_space_limit("tmp", disk_space_limit, name)

    @classmethod
    def teardown_class(cls):
        cls.teardown_s3_client()

        super(TestS3MediumBase, cls).teardown_class()

    def setup_method(self, method):
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


################################################################################


class TestS3Medium(TestS3MediumBase):
    @authors("achulkov2")
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
        time.sleep(3)
        create("table", "//tmp/in", attributes={"primary_medium": self.get_s3_medium_name()})
        create("table", "//tmp/out", attributes={"primary_medium": self.get_s3_medium_name()})

        write_table("//tmp/in", {"a": "b"})
        assert read_table("//tmp/in") == [{"a": "b"}]

        with raises_yt_error("Operations on tables with offshore medium are forbidden"):
            map(
                command="cat",
                in_="//tmp/in",
                out="//tmp/out",
                spec={"max_failed_job_count": 1})

    @authors("achulkov2")
    def test_files_simple(self):
        create("file", "//tmp/f", attributes={"primary_medium": self.get_s3_medium_name()})

        write_file("//tmp/f", b"Hello, world!")
        assert read_file("//tmp/f") == b"Hello, world!"
        assert read_file("//tmp/f", offset=7) == b"world!"

        chunk_id = get_singular_chunk_id("//tmp/f")
        object_data = self.get_s3_object(self.S3_MEDIA[0]["bucket"], self.get_chunk_path(chunk_id))
        assert object_data == b"Hello, world!"

    @authors("achulkov2")
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

        # TODO(achulkov2): When it is implemented, check that the data is actually removed from the S3 bucket.

    @authors("achulkov2")
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
            print_debug(f"Requisition length {len(requisition)} does not match expected length {len(conditions_per_medium)}")
            return False

        for entry in requisition:
            medium = entry["medium"]
            if medium not in conditions_per_medium:
                print_debug(f"Medium {medium} is not in expected requisition")
                return False

            for condition, expected_value in conditions_per_medium[entry["medium"]].items():
                if entry[condition] != expected_value:
                    print_debug(f"Condition {condition}={expected_value} is not satisfied for medium {medium}, equal to {entry[condition]} instead")
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
    def test_multiple_media(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})

        t_media = get("//tmp/t/@media")
        print_debug(t_media)

        t_media["default"] = {"replication_factor": 3, "data_parts_only": False}
        with pytest.raises(YtError, match='Cannot set replication with offshore media'):
            set("//tmp/t/@media", t_media)

    @authors("achulkov2")
    def test_medium_switch(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})

        with pytest.raises(YtError, match='Cannot set replication with offshore media'):
            set("//tmp/t/@primary_medium", "default")

    @authors("achulkov2")
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

    @authors("achulkov2")
    @pytest.mark.run_with_no_offshore_node_proxies
    def test_chunk_merger(self):
        chunk_count = 10
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        for i in range(chunk_count):
            write_table("<append=%true>//tmp/t", {"a": f"b{i}"})

        set("//sys/accounts/tmp/@merge_job_rate_limit", 10)
        set("//sys/accounts/tmp/@chunk_merger_node_traversal_concurrency", 1)

        actual_chunk_count = get("//tmp/t/@chunk_count")
        assert actual_chunk_count == chunk_count

        set("//tmp/t/@chunk_merger_mode", "auto")
        # For now we just check that nothing crashes
        wait(lambda: get("//tmp/t/@chunk_merger_status") == "not_in_merge_pipeline")

    @authors("faucct")
    def test_medium_without_bucket(self):
        create("table", "//tmp/f", attributes={"primary_medium": "s3_read_only"})
        record1 = {"x": 1, "y": "b"}

        with pytest.raises(YtError, match='without a configured bucket'):
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

    @authors("pavel-bash")
    def test_dynamic_table_in_memory(self):
        # A very specific test to trigger the GetBlockRange on offshore data gateway in both in_memory
        # dynamic table preload path (in_memory_manager.cpp) and memory tracking chunk reader path
        # (block_tracking_chunk_reader.cpp).
        INPUT_TABLE = "//tmp/input"

        sorted_input = [{"key": i, "value": f"hello_{i}"} for i in range(100)]

        create("table", f"<append=%true>{INPUT_TABLE}", attributes={
            "primary_medium": self.get_s3_medium_name(),
            "dynamic": True,
            "in_memory_mode": "uncompressed",
            "chunk_writer": {"block_size": 1},
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"}
            ]})

        sync_mount_table(INPUT_TABLE)
        insert_rows(INPUT_TABLE, sorted_input)
        sync_unmount_table(INPUT_TABLE)

        sync_mount_table(INPUT_TABLE)
        wait(lambda: get(f"{INPUT_TABLE}/@preload_state") == "complete")
        self.assert_table_chunks_exist_in_s3(INPUT_TABLE)

        assert lookup_rows(INPUT_TABLE, [{"key": 25}]) == [{"key": 25, "value": "hello_25"}]


################################################################################


class TestS3MediumRpcProxy(TestS3Medium):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
