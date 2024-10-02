from yt.orm.library.common import wait
from yt.yson import YsonUint64
from yt.common import YtResponseError


def _reshard_table(example_env, table_path, object_type, object_id, tablet_count):
    step = (2**64) // tablet_count
    pivot_keys = [[]]
    for i in range(1, tablet_count):
        pivot_keys.append([YsonUint64(i * step)])

    example_env.yt_client.unmount_table(table_path, sync=True)
    example_env.yt_client.reshard_table(table_path, pivot_keys=pivot_keys, sync=True)
    example_env.yt_client.mount_table(table_path, sync=True)

    def check_liveness():
        try:
            example_env.client.get_object(object_type, object_id, selectors=["/meta/id"])
            return True
        except YtResponseError:
            return False

    wait(check_liveness)


def _count_rows(yt_client, table_path):
    query = "* from [{}]".format(table_path)
    return len(list(yt_client.select_rows(query)))


class TestSweep:
    BOOKS_TABLE_PATH = "//home/example/db/books"
    PENDING_REMOVALS_TABLE_PATH = "//home/example/db/pending_removals"
    PUBLISHER_COUNT = 8
    BOOK_COUNT = 32
    TABLET_COUNT = 3

    def test_sweep_hash_key_field(self, example_env):
        yt_client = example_env.yt_client
        publisher_ids = example_env.client.create_objects(
            (
                ("publisher", dict(spec=dict(name="Publisher #{}".format(i))))
                for i in range(self.PUBLISHER_COUNT)
            ),
        )
        book_ids = example_env.client.create_objects(
            (
                ("book", dict(meta=dict(isbn="isbn", parent_key=publisher_ids[i % self.PUBLISHER_COUNT])))
                for i in range(self.BOOK_COUNT)
            ),
        )
        _reshard_table(
            example_env, self.BOOKS_TABLE_PATH, "book", book_ids[0], self.TABLET_COUNT
        )
        old_count = _count_rows(yt_client, self.BOOKS_TABLE_PATH)
        example_env.client.remove_objects([("book", book_id) for book_id in book_ids])
        wait(
            lambda: _count_rows(yt_client, self.BOOKS_TABLE_PATH) <= old_count - self.BOOK_COUNT,
            iter=100,
            sleep_backoff=5,
        )
        wait(
            lambda: _count_rows(yt_client, self.PENDING_REMOVALS_TABLE_PATH) == 0,
            iter=100,
            sleep_backoff=5,
        )

    def test_sweep_recreated_objects(self, example_env):
        yt_client = example_env.yt_client
        publisher_ids = example_env.client.create_objects(
            (
                ("publisher", dict(spec=dict(name="Publisher #{}".format(i))))
                for i in range(self.PUBLISHER_COUNT)
            ),
        )

        book_ids = example_env.client.create_objects(
            (
                (
                    "book",
                    dict(meta=dict(id=i, isbn="isbn" + str(i), parent_key=publisher_ids[i % self.PUBLISHER_COUNT]))
                )
                for i in range(self.BOOK_COUNT)
            ),
        )

        example_env.client.remove_objects([("book", book_id) for book_id in book_ids])

        assert _count_rows(yt_client, self.PENDING_REMOVALS_TABLE_PATH) > 0

        book_ids = []
        for i in range(self.BOOK_COUNT):
            publisher_id = publisher_ids[i % self.PUBLISHER_COUNT]
            book_id = example_env.client.create_object(
                "book",
                dict(meta=dict(id=i, isbn="isbn" + str(i), parent_key=publisher_id)),
                request_meta_response=True,
            )
            book_ids.append(book_id)

        wait(lambda: _count_rows(yt_client, self.PENDING_REMOVALS_TABLE_PATH) == 0)

        for i in range(self.BOOK_COUNT):
            book_id = book_ids[i]
            isbn = "isbn" + str(i)
            publisher_id = publisher_ids[i % self.PUBLISHER_COUNT]
            assert example_env.client.get_object(
                "book", book_id, selectors=["/meta/isbn", "/meta/parent_key"]
            ) == [isbn, publisher_id]
