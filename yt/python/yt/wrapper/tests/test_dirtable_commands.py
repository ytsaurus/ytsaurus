# -*- coding: utf-8 -*-

from .conftest import authors
from .helpers import TEST_DIR

from yt.wrapper.common import MB
from yt.wrapper import dirtable_commands

import multiprocessing
import pytest


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestDirtableCommands(object):
    @authors("alkedr")
    def test_dirtable_commands(self, tmpdir):
        dir1 = tmpdir.mkdir("dir1")
        dir1.join("file1").write_binary(b"1234")
        dir1.mkdir("subdir").join("file2").write_binary(b"5678")

        yt_table = TEST_DIR + "/dirtable1"

        dirtable_commands.upload_directory_to_yt(
            directory=str(dir1),
            recursive=True,
            yt_table=yt_table,
            part_size=4 * MB,
            process_count=4,
            force=True,
            prepare_for_sky_share=True,
            client=None,
            # Real multiprocessing doesn't work in tests because test YT client config can't be pickled.
            process_pool_class=multiprocessing.pool.ThreadPool,
        )

        dir1.join("file3").write_binary(b"9")
        dirtable_commands.append_single_file(
            yt_table=yt_table,
            fs_path=str(dir1.join("file3")),
            yt_name="file3",
            process_count=4,
            client=None,
            # Real multiprocessing doesn't work in tests because test YT client config can't be pickled.
            process_pool_class=multiprocessing.pool.ThreadPool,
        )

        dirtable_commands.list_files_from_yt(
            yt_table=yt_table,
            client=None,
        )

        dir2 = tmpdir.mkdir("dir2")
        dirtable_commands.download_directory_from_yt(
            directory=dir2,
            yt_table=yt_table,
            process_count=4,
            exact_filenames=None,
            filter_by_regexp=None,
            exclude_by_regexp=None,
            client=None,
            # Real multiprocessing doesn't work in tests because test YT client config can't be pickled.
            process_pool_class=multiprocessing.pool.ThreadPool,
        )

        assert tmpdir.join("dir2").join("file1").read_binary() == b"1234"
        assert tmpdir.join("dir2").join("subdir").join("file2").read_binary() == b"5678"
        assert tmpdir.join("dir2").join("file3").read_binary() == b"9"
