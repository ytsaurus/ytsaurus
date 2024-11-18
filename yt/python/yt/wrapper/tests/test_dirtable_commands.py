# -*- coding: utf-8 -*-
import re
from textwrap import dedent

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
            store_full_path=False,
            exact_filenames=None,
            filter_by_regexp=None,
            exclude_by_regexp=None,
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
            store_full_path=False,
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

    @authors("optimus", "denvr")
    def test_dirtable_commands_full_path(self, tmpdir, capsys):
        """
        testing additional functions / modes:
        - store_full_path in upload to save full relative path
        - filtering in upload
        - list_files_from_yt output
        """
        tmpdir.chdir()  # changing to use relative paths

        dir1 = tmpdir.mkdir("dir1")
        dir1.join("file1").write_binary(b"1234")
        dir1.mkdir("subdir").join("file2").write_binary(b"567")
        dir1.mkdir("another_subdir_do_not_upload").join("file3").write_binary(b"0")

        dir1.join("subdir").join("unwanted_1").write_binary(b"9999")
        dir1.join("subdir").mkdir("subdir11").join("file11a").write_binary(b"1122")
        dir1.join("subdir").join("subdir11").join("unwanted_2").write_binary(b"9999")

        yt_table = TEST_DIR + "/dirtable1"

        # Uploading using relative path, expecting to store this whole relative path starting with `dir1`
        dirtable_commands.upload_directory_to_yt(
            directory="dir1/subdir",
            store_full_path=True,
            exact_filenames=None,
            filter_by_regexp=None,
            exclude_by_regexp=re.compile(".+unwanted.+"),
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

        dir1.join("subdir").join("subdir11").join("file11b").write_binary(b"11333")
        dirtable_commands.append_single_file(
            yt_table=yt_table,
            fs_path="dir1/subdir/subdir11/file11b",
            store_full_path=True,
            yt_name="will_be_overridden_anyways_with_store_full_path",
            process_count=4,
            client=None,
            # Real multiprocessing doesn't work in tests because test YT client config can't be pickled.
            process_pool_class=multiprocessing.pool.ThreadPool,
        )

        dirtable_commands.list_files_from_yt(
            yt_table=yt_table,
            client=None,
            raw=False
        )
        captured_out = capsys.readouterr().out
        target_output = """
            Filename                     | File Size
            ----------------------------------------
            dir1/subdir/file2            |         3
            dir1/subdir/subdir11/file11a |         4
            dir1/subdir/subdir11/file11b |         5
        """
        assert captured_out[captured_out.find("Filename"):].strip() == dedent(target_output).strip()

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

        # Comparing directories contents. Full path with `dir1` is present, but only with the uploaded files from `subdir`
        assert sorted([item.basename for item in dir2.listdir()]) == ["dir1"]
        assert sorted([item.basename for item in dir2.join("dir1").listdir()]) == ["subdir"]
        assert sorted([item.basename for item in dir2.join("dir1").join("subdir").listdir()]) == ["file2", "subdir11"]
        assert sorted([item.basename for item in dir2.join("dir1").join("subdir").join("subdir11").listdir()]) == ["file11a", "file11b"]

        assert tmpdir.join("dir2").join("dir1").join("subdir").join("file2").read_binary() == b"567"
        assert tmpdir.join("dir2").join("dir1").join("subdir").join("subdir11").join("file11a").read_binary() == b"1122"
        assert tmpdir.join("dir2").join("dir1").join("subdir").join("subdir11").join("file11b").read_binary() == b"11333"
