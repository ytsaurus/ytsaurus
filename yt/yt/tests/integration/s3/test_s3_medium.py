from yt.common import YtError
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, make_random_string, sync_create_cells,
    create, create_domestic_medium, create_s3_medium, set, remove, exists,
    copy, move, get_singular_chunk_id, wait, get, concatenate,
    get_account_disk_space_limit, set_account_disk_space_limit,
    write_table, read_table, sync_mount_table, insert_rows, sync_flush_table, attach_table,
    map, merge, select_rows, lookup_rows, print_debug, write_file, read_file, sort,
    start_transaction, commit_transaction, abort_transaction)

from yt.test_helpers import assert_items_equal
from yt_type_helpers import optional_type, make_schema, make_column, struct_type, list_type

import time
import pytest
import os
import uuid
import logging
import builtins

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
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

################################################################################

def is_s3_configured():
    s3_client = None
    try:
        s3_client = boto3.client('s3')
        s3_client.list_buckets()
        return True
    except (NoCredentialsError, ClientError) as e:
        print_debug(f"S3 configuration check failed: {e}")
        return False
    finally:
        if s3_client:
            s3_client.close()

################################################################################


# TODO(achulkov2): [PLater] Move the test for manipulating the S3 medium master object from master/test_media.py here?
class TestS3MediumBase(YTEnvSetup):
    # TODO(achulkov2): [PLater] Multidaemon?
    NUM_MASTERS = 1
    NUM_NODES = 3

    NUM_SCHEDULERS = 1

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
    ]

    EXTRA_BUCKET_COUNT = 2
    EXTRA_BUCKETS = [uuid.uuid4().hex for _ in range(EXTRA_BUCKET_COUNT)]

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

    @classmethod
    def get_buckets(cls):
        return {medium["bucket"] for medium in cls.S3_MEDIA} | builtins.set(cls.EXTRA_BUCKETS)

    @classmethod
    def get_s3_medium_name(cls, index=0):
        return cls.S3_MEDIA[index]["name"]

    @classmethod
    def get_s3_medium_bucket(cls, index=0):
        return cls.S3_MEDIA[index]["bucket"]

    @classmethod
    def get_s3_medium_config(cls, index=0):
        return {
            # Standard S3 environment variables.
            "url": os.getenv("AWS_ENDPOINT_URL"),
            "region": os.getenv("AWS_REGION"),
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            # YT specific configuration.
            "bucket": cls.S3_MEDIA[index]["bucket"],
            # TODO(achulkov2): Prefix for placing chunks.
        }

    @classmethod
    def setup_s3_client(cls):
        # Credentials are retrieved from standard S3 environment variables.
        cls.S3_CLIENT =  boto3.client('s3')

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
    def get_chunk_path(cls, chunk_id, s3_medium_index=0):
        # TODO(achulkov2): Prefix for placing chunks.
        prefix = "chunk-data"
        return f"{prefix}/{chunk_id}"
    
    @classmethod
    def get_chunk_meta_path(cls, chunk_id, s3_medium_index=0):
        # TODO(achulkov2): Prefix for placing chunks.
        prefix = "chunk-data"
        return f"{prefix}/{chunk_id}.meta"

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
    def setup_class(cls):
        super(TestS3MediumBase, cls).setup_class()

        cls.setup_s3_client()

        for bucket in cls.get_buckets():
            cls.create_bucket(bucket)

        disk_space_limit = get_account_disk_space_limit("tmp", "default")

        create_domestic_medium("hdd1")

        for i in range(len(cls.S3_MEDIA)):
            name = cls.get_s3_medium_name(i)
            create_s3_medium(name, cls.get_s3_medium_config(i))
            set_account_disk_space_limit("tmp", disk_space_limit, name)

    @classmethod
    def teardown_class(cls):
        super(TestS3MediumBase, cls).teardown_class()

        for bucket in cls.get_buckets():
            cls.clear_bucket(bucket)

        cls.teardown_s3_client()

    def setup_method(self, method):
        super(TestS3MediumBase, self).setup_method(method)

        # TODO(achulkov2): Fix related part in yt_env, since we plan to forbid changing some parameters of the medium after it is created.
        for i in range(len(self.S3_MEDIA)):
            set(f"//sys/media/{self.get_s3_medium_name(i)}/@config", self.get_s3_medium_config(i))

        # Wait for medium directory synchronizer to do its thing.
        # TODO(achulkov2): This can be removed once/if we return medium directory entries along with chunk specs during fetch.
        time.sleep(1)


@pytest.mark.skipif(not is_s3_configured(), reason="S3 is not configured")
class TestS3Medium(TestS3MediumBase):
    @authors("achulkov2")
    def test_tables_simple(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})
        write_table("<append=%true>//tmp/t", {"c": "d"})

        many_blocks = [{f"row_{i}": f"value_{i}"} for i in range(1000)]
        write_table("<append=%true>//tmp/t", many_blocks, table_writer={"block_size": 64, "min_part_size": 6 * MB})

        multiple_parts = [{"key": make_random_string(length=MB // 2)} for i in range(25)]
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

    @authors("achulkov2")
    def test_sort_operation(self):
        # TODO(achulkov2): Implement this.
        pass

    @authors("achulkov2")
    def test_map_reduce_operation(self):
        # TODO(achulkov2): Implement this.
        pass

    @authors("achulkov2")
    def test_reduce_operation(self):
        # TODO(achulkov2): Implement this.
        pass

    @authors("achulkov2")
    def test_files_simple(self):
        create("file", "//tmp/f", attributes={"primary_medium": self.get_s3_medium_name()})

        write_file("//tmp/f", b"Hello, world!")
        assert read_file("//tmp/f") == b"Hello, world!"
        assert read_file("//tmp/f", offset=7) == b"world!"

        chunk_id = get_singular_chunk_id("//tmp/f")
        object_data = self.get_s3_object(self.S3_MEDIA[0]["bucket"], f"chunk-data/{chunk_id}")
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
        # TODO(achulkov2): Fix in one way or the other.
        if not sorted:
            pytest.skip("Ordered dynamic tables do not update replicas on stores update yet :(")

        sync_create_cells(1)

        schema = [
            {"name": "s", "type": "string", "sort_order": "ascending" if sorted else None},
            {"name": "i", "type": "int64"},
        ]

        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name(), "dynamic": True, "schema": schema})

        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"s": "a", "i": 3}])
        insert_rows("//tmp/t", [{"s": "b", "i": 1}, {"s": "c", "i": 2}])

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
        set("//tmp/t/@media", t_media)

        write_table("<append=%true>//tmp/t", {"c": "d"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 2

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
                    "lost": True,
                },
            }))

        # Chunks are not considered lost, since they are present on the S3 medium.
        assert len(get("//sys/lost_vital_chunks")) == 0
        assert len(get("//sys/lost_chunks")) == 0

        # Let's double check that the table is still readable.
        assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]

    @authors("achulkov2")
    def test_medium_switch(self):
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        write_table("//tmp/t", {"a": "b"})

        set("//tmp/t/@primary_medium", "default")

        write_table("<append=%true>//tmp/t", {"c": "d"})

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 2

        s3_chunk = chunk_ids[0]
        default_chunk = chunk_ids[1]

        def s3_chunk_move_to_default_requested():
            requisition = get(f"#{s3_chunk}/@requisition")
            return len(requisition) == 1 and requisition[0]["medium"] == "default"
        wait(s3_chunk_move_to_default_requested)

        wait(lambda: self._check_requisition(s3_chunk, {
            "default": {
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
            },
        }))

        wait(lambda: self._check_requisition(default_chunk, {
            "default": {
                "replication_policy": {
                    "replication_factor": 3,
                    "data_parts_only": False,
                },
            },
        }))

        wait(lambda: self._check_replication(s3_chunk, {
            self.get_s3_medium_name(): {
                "overreplicated": True,
                "unexpected_overreplicated": True,
            },
            "default": {
                "lost": True,
            },
        }))

        wait(lambda: self._check_replication(default_chunk, {
            "default": {
                # Everythign is false.
            },
        }))

        assert len(get("//sys/lost_vital_chunks")) == 0
        assert len(get("//sys/lost_chunks")) == 0

        # It should still be possible to read the whole table, because the first chunk still has replicas on the S3 medium.
        assert read_table("//tmp/t") == [{"a": "b"}, {"c": "d"}]

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

        # time.sleep(15)

        test_chunk = chunk_ids[0]

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
    def test_chunk_merger(self):
        chunk_count = 10
        create("table", "//tmp/t", attributes={"primary_medium": self.get_s3_medium_name()})
        for i in range(chunk_count):
            write_table("<append=%true>//tmp/t", {"a": "b"})

        actual_chunk_count = get("//tmp/t/@chunk_count")
        assert actual_chunk_count == chunk_count

        # TODO(achulkov2): [PDuringReview] Test all chunk merger modes explicitly.
        set("//tmp/t/@chunk_merger_mode", "auto")

        time.sleep(3)

        # TODO(achulkov2): [PDuringReview] Fix once chunk merger supports offshore replicas.

        merged_chunk_count = get("//tmp/t/@chunk_count")
        assert merged_chunk_count == actual_chunk_count


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


################################################################################


@pytest.mark.skipif(not is_s3_configured(), reason="S3 is not configured")
class TestS3MediumRpcProxy(TestS3Medium):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


################################################################################


# TODO(achulkov2): Test suite with multiple masters and multi-cell.


################################################################################


@pytest.mark.skipif(not is_s3_configured(), reason="S3 is not configured")
class TestAttachTable(TestS3MediumBase):
    # Add configuration here.

    @staticmethod
    def dump_arrow_table_as_bytes(table: pa.Table, **kwargs) -> BytesIO:
        buffer = BytesIO()
        pq.write_table(table, buffer, **kwargs)
        buffer.seek(0)
        return buffer

    @authors("faucct")
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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/bar.parquet"])
        assert read_table("//tmp/imported") == data

    @authors("faucct")
    def test_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

        assert read_table("//tmp/imported") == records

    @authors("faucct")
    def test_attach_to_wrong_medium(self):
        create("table", "//tmp/imported")
        bucket = self.get_s3_medium_bucket()
        records = [{"x": 1, "y": "b"}, {"x": 2, "y": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Cannot attach external data to table on non-offshore medium "default"'):
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

    @authors("faucct")
    def test_attach_wrong_bucket(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        with pytest.raises(YtError, match="Got status code 404 Not Found"):
            attach_table("//tmp/imported", source_uris=[f"s3://not-{bucket}/foo.parquet"])

    @authors("faucct")
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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

        assert read_table("//tmp/imported") == records
        assert get(f"#{get("//tmp/imported/@chunk_ids")[0]}/@schema") == make_schema(schema, strict=True, unique_keys=False)

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
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

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
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

    @authors("faucct")
    def test_attach_with_erasure_codec(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name(), "erasure_codec": "reed_solomon_6_3"})
        bucket = self.get_s3_medium_bucket()
        records = [{"x": "a"}]

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records(records))
        ))

        with pytest.raises(YtError, match='Cannot attach external data to table with erasure codec'):
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

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
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

    @authors("faucct")
    def test_attach_with_different_schemas_and_read(self):
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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"])

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

        assert_items_equal([
            make_schema([], strict=False, unique_keys=False),
            make_schema([], strict=False, unique_keys=False),
        ], [get(f"#{chunk_id}/@schema") for chunk_id in get("//tmp/imported/@chunk_ids")])

    @authors("faucct")
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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet", f"s3://{bucket}/bar.parquet"])

        # TODO(achulkov2): Order is guaranteed, we don't need to use the helper.
        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

    @authors("faucct")
    def test_overwriting_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/bar.parquet"])

        assert_items_equal(read_table("//tmp/imported"), [record2])

    @authors("faucct")
    def test_appending_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record1]))
        ))

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/bar.parquet"])

        assert_items_equal(read_table("//tmp/imported"), [record1, record2])

    @authors("faucct")
    def test_write_and_appending_attach_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()
        record1 = {"x": 1, "y": "b"}
        record2 = {"x": 2, "y": "a"}

        write_table("//tmp/imported", [record1])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.parquet", Body=self.dump_arrow_table_as_bytes(
            pa.Table.from_pandas(pandas.DataFrame.from_records([record2]))
        ))
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/bar.parquet"])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.jsonl", Body='{"x": 3, "y": "d"}\n')
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/bar.jsonl"])

        self.S3_CLIENT.put_object(Bucket=bucket, Key="bar.csv", Body='x,y\n4,c\n')
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/bar.csv"])

        assert_items_equal(read_table("//tmp/imported"), [record1, record2, {"x": 3, "y": "d"}, {"x": 4, "y": "c"}])

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

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.parquet"])
        map(
            command="cat",
            in_="//tmp/imported",
            out="//tmp/out",
            spec={"max_failed_job_count": 1})

        assert_items_equal(read_table("//tmp/out"), [record1, record2])

    @authors("faucct")
    def test_attach_jsonl_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.jsonl"])

        assert_items_equal(read_table("//tmp/imported"), [{"col": "a"}, {"col": "b"}])

    @authors("faucct")
    def test_attach_csv_and_read(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})
        bucket = self.get_s3_medium_bucket()

        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"col\na\nb\n")

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.csv"])

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
            attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.csv"])

    @authors("achulkov2")
    def test_sorted_attach_weak_schema(self):
        create("table", "//tmp/imported")

        write_table("//tmp/imported", [{"a": 43, "b": 5}, {"a": 15, "b": 7}])
        sort(in_="//tmp/imported", out="//tmp/imported", sort_by=["a"])

        assert read_table("//tmp/imported") == [{"a": 15, "b": 7}, {"a": 43, "b": 5}]
        # Column "a" was automatically inferred, since it is the sort key.
        assert len(get("//tmp/imported/@schema")) == 1
        assert get("//tmp/imported/@sorted")
        assert get("//tmp/imported/@schema_mode") == "weak"

        set("//tmp/imported/@primary_medium", self.get_s3_medium_name())

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"a\n27\n99\n")
        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.csv"])

        assert get("//tmp/imported/@schema") == make_schema([], strict=False, unique_keys=False)
        assert get("//tmp/imported/@schema_mode")
        assert not get("//tmp/imported/@sorted")

    @authors("achulkov2")
    def test_sorted_attach_chunk_sort_columns(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.csv", Body=b"a\n15\n43\n")

        with pytest.raises(YtError, match="Table schemas are incompatible"):
            attach_table("<chunk_sort_columns=[a]>//tmp/imported", source_uris=[f"s3://{bucket}/foo.csv"])

    @authors("achulkov2")
    def test_attach_wrong_uri(self):
        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        with pytest.raises(YtError, match="unexpected scheme"):
            attach_table("//tmp/imported", source_uris=["ftp://example.com/foo.parquet"])

        with pytest.raises(YtError, match="unsupported extension"):
            attach_table("//tmp/imported", source_uris=["s3://my-bucket/foo.brakozyabra"])

    @authors("achulkov2")
    def test_external_transaction_commit(self):
        tx = start_transaction()

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()}, tx=tx)

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.jsonl"], tx=tx)

        assert not exists("//tmp/imported")
        assert exists("//tmp/imported", tx=tx)

        assert read_table("//tmp/imported", tx=tx) == [{"col": "a"}, {"col": "b"}]

        commit_transaction(tx)

        assert read_table("//tmp/imported") == [{"col": "a"}, {"col": "b"}]

    @authors("achulkov2")
    def test_external_transaction_abort(self):
        tx = start_transaction()

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()}, tx=tx)

        bucket = self.get_s3_medium_bucket()
        self.S3_CLIENT.put_object(Bucket=bucket, Key="foo.jsonl", Body=b'{"col":"a"}\n{"col":"b"}\n')

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket}/foo.jsonl"], tx=tx)

        assert not exists("//tmp/imported")
        assert exists("//tmp/imported", tx=tx)

        chunk_ids = get("//tmp/imported/@chunk_ids", tx=tx)
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]

        assert read_table("//tmp/imported", tx=tx) == [{"col": "a"}, {"col": "b"}]

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

        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/foo1.jsonl"], tx=tx1)
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/foo2.jsonl"], tx=tx2)
        attach_table("<append=%true>//tmp/imported", source_uris=[f"s3://{bucket}/foo3.jsonl"], tx=tx3)

        commit_transaction(tx2)
        commit_transaction(tx1)
        commit_transaction(tx3)

        assert read_table("//tmp/imported") == [{"col": "b"}, {"col": "a"}, {"col": "c"}]

    @authors("achulkov2")
    def test_separate_bucket(self):
        bucket1 = self.EXTRA_BUCKETS[0]
        bucket2 = self.EXTRA_BUCKETS[1]

        create("table", "//tmp/imported", attributes={"primary_medium": self.get_s3_medium_name()})

        self.S3_CLIENT.put_object(Bucket=bucket1, Key="foo.jsonl", Body=b'{"col":"a"}\n')
        self.S3_CLIENT.put_object(Bucket=bucket2, Key="bar.jsonl", Body=b'{"col":"b"}\n')

        attach_table("//tmp/imported", source_uris=[f"s3://{bucket1}/foo.jsonl", f"s3://{bucket2}/bar.jsonl"])
        assert read_table("//tmp/imported") == [{"col": "a"}, {"col": "b"}]


################################################################################


@pytest.mark.skipif(not is_s3_configured(), reason="S3 is not configured")
class TestAttachTableRpcProxy(TestAttachTable):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
