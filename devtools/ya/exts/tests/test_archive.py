# coding: utf-8

import os
import time
import stat
import tarfile
import logging
import filecmp
import subprocess

import pytest

import exts.tmp
import exts.fs
import exts.archive

import yatest.common
import library.python.windows

logger = logging.getLogger(__name__)


def test_extract_from_tar():
    with exts.tmp.temp_dir() as temp_dir:
        test_file_path = os.path.join(temp_dir, "test.txt")
        with open(test_file_path, "w") as f:
            f.write("test")
        tar_file_path = os.path.join(temp_dir, "test.tar")
        with tarfile.open(tar_file_path, "w") as tar_f:
            tar_f.add(test_file_path, os.path.basename(test_file_path))

        with exts.tmp.temp_dir() as temp_tar_dir:
            exts.archive.extract_from_tar(tar_file_path, temp_tar_dir)
            expected_file = os.path.join(temp_tar_dir, "test.txt")
            assert os.path.exists(expected_file)
            with open(expected_file) as f:
                assert f.read() == "test"


def _create_some_files(directory):
    exts.fs.write_file(os.path.join(directory, 'a'), '')
    exts.fs.write_file(os.path.join(directory, 'b'), '')
    exts.fs.create_dirs(os.path.join(directory, 'c'))
    exts.fs.write_file(os.path.join(directory, 'c', 'd'), '')
    exts.fs.create_dirs(os.path.join(directory, 'c', 'e'))


@pytest.mark.parametrize(
    'tar_file, compression_filter, compression_level',
    [
        ('result.tar', None, None),
        ('result.tar.gz', exts.archive.GZIP, exts.archive.Compression.Best),
        ('result.tar.zst', exts.archive.ZSTD, exts.archive.Compression.Best),
    ],
)
def test_pack_unpack(tar_file, compression_filter, compression_level):
    with exts.tmp.temp_dir() as directory, exts.tmp.temp_dir() as result_dir, exts.tmp.temp_dir() as output_dir:
        _create_some_files(directory)
        tar = os.path.join(result_dir, tar_file)
        exts.archive.create_tar(directory, tar, compression_filter, compression_level, fixed_mtime=None)
        exts.archive.extract_from_tar(tar, output_dir)
        assert filecmp.dircmp(directory, output_dir)


def test_check_archive():
    with exts.tmp.temp_dir() as directory, exts.tmp.temp_dir() as temp_dir:
        _create_some_files(directory)
        tar = os.path.join(temp_dir, 'archive.tar.gz')
        exts.archive.create_tar(directory, tar, exts.archive.GZIP, exts.archive.Compression.Best)
        assert exts.archive.check_archive(tar)
        assert not exts.archive.check_archive(directory)


def test_create_tar_multiple_paths():
    exts.fs.create_dirs("one")
    exts.fs.create_dirs("two")

    with open("one/one.txt", "w") as f:
        f.write("one")

    with open("two/two.txt", "w") as f:
        f.write("two")
    tar_path = yatest.common.output_path("test_create_tar_multiple_paths.tar")
    exts.archive.create_tar(
        [
            (
                "one/one.txt",
                "one/one.txt",
            ),
            ("two/two.txt", "two/two.txt"),
        ],
        tar_path,
    )

    expected_tar_members = [
        "one/one.txt",
        "two/two.txt",
    ]

    with tarfile.open(tar_path) as tar:
        members = tar.getnames()
        assert members == expected_tar_members, members


@pytest.mark.skipif(library.python.windows.on_win(), reason='unix specific')
def test_symlinks():
    exts.fs.create_dirs("symlinks")
    exts.fs.create_dirs("external")

    with open("symlinks/1.txt", "w") as f:
        f.write("one")
    with open("external/2.txt", "w") as f:
        f.write("two")

    os.symlink("1.txt", "symlinks/линк.txt")
    os.symlink("external/2.txt", "symlinks/external_link.txt")

    tar_path = yatest.common.output_path("symlinks.tar")
    exts.archive.create_tar("symlinks", tar_path)

    exts.archive.extract_from_tar(tar_path, "symlinks_out")
    assert os.path.isfile("symlinks_out/1.txt")
    assert os.path.islink("symlinks_out/линк.txt")
    with open("symlinks_out/линк.txt") as afile:
        assert afile.read() == "one"
    assert os.path.islink("symlinks/external_link.txt")
    assert not os.path.exists("symlinks/external_link.txt")


@pytest.mark.skipif(library.python.windows.on_win(), reason='unix specific')
def test_hardlinks_unix():
    exts.fs.create_dirs("hardlinks")
    exts.fs.create_dirs("externaldata")

    with open("hardlinks/1.txt", "w") as f:
        f.write("one")
    with open("externaldata/2.txt", "w") as f:
        f.write("two")

    os.link("hardlinks/1.txt", "hardlinks/линк.txt")
    os.link("externaldata/2.txt", "hardlinks/external_link.txt")

    tar_path = yatest.common.output_path("hardlinks.tar")
    exts.archive.create_tar("hardlinks", tar_path)

    exts.archive.extract_from_tar(tar_path, "hardlinks_out")
    assert os.path.isfile("hardlinks_out/1.txt")
    assert os.path.isfile("hardlinks_out/линк.txt")
    with open("hardlinks_out/линк.txt") as afile:
        assert afile.read() == "one"
    assert os.stat("hardlinks_out/1.txt").st_ino == os.stat("hardlinks_out/линк.txt").st_ino

    assert os.path.isfile("hardlinks/external_link.txt")
    with open("hardlinks/external_link.txt") as afile:
        assert afile.read() == "two"


@pytest.mark.skipif(library.python.windows.on_win(), reason='unix specific')
def test_hardlink_to_symlink():
    exts.fs.create_dirs("links")

    with open("links/1.txt", "w") as f:
        f.write("one")
    os.symlink("1.txt", "links/symlink.txt")
    os.link("links/symlink.txt", "links/hardlink_to_symlink.txt")

    tar_path = yatest.common.output_path("links.tar")
    exts.archive.create_tar("links", tar_path)

    exts.archive.extract_from_tar(tar_path, "links_out")
    assert os.path.isfile("links_out/1.txt")
    assert os.path.islink("links_out/symlink.txt")
    with open("links_out/symlink.txt") as afile:
        assert afile.read() == "one"

    assert os.path.islink("links_out/hardlink_to_symlink.txt")
    with open("links_out/hardlink_to_symlink.txt") as afile:
        assert afile.read() == "one"

    assert os.stat("links_out/symlink.txt").st_ino == os.stat("links_out/hardlink_to_symlink.txt").st_ino


@pytest.mark.skipif(
    library.python.windows.on_win(),
    reason="unix specific. windows wouldn't allow to open file which is already opened by another process",
)
def test_archive_file_while_another_process_appending_data():
    exts.fs.create_dirs("appending")

    filename = "appending/1.txt"
    with open(filename, "w") as f:
        f.write("one")

    tar_path = yatest.common.output_path("appending.tar")
    fsize = os.stat(filename).st_size

    cmd = [
        yatest.common.python_path(),
        "-c",
        "f = open('{}', 'a'); [f.write('1') or f.flush() for _ in iter(int, 1)]".format(filename),
    ]
    proc = subprocess.Popen(cmd)
    try:
        # wait until new data been written
        while os.stat(filename).st_size == fsize:
            time.sleep(0.1)

        exts.archive.create_tar("appending", tar_path)

        # wait until even more data been written to validate file sizes won't match
        fsize = os.stat(filename).st_size
        while os.stat(filename).st_size == fsize:
            time.sleep(0.1)
    finally:
        proc.kill()
        proc.wait()

    exts.archive.extract_from_tar(tar_path, "appending_out")
    assert os.path.isfile("appending_out/1.txt")
    with open("appending_out/1.txt") as afile:
        assert afile.read(5) == "one11"

    assert os.stat("appending_out/1.txt").st_size != os.stat(filename).st_size


def test_archive_empty_dir():
    exts.fs.create_dirs("empty_dir")
    exts.fs.create_dirs("empty_dir/empty")

    tar_path = yatest.common.output_path("empty_dir.tar")
    exts.archive.create_tar("empty_dir", tar_path)

    exts.archive.extract_from_tar(tar_path, "empty_dir_out")
    assert os.path.isdir("empty_dir_out/empty")


def test_multiplatform_extraction():
    # came from sandbox - see DATA section
    tar_path = "archive_testdata.tar"
    extract_dir = yatest.common.output_path("multiplatform_extraction")
    exts.archive.extract_from_tar(tar_path, extract_dir)

    for filename in ["hardlink.txt", "symlink.txt", "file.txt", "file2.txt"]:
        filename = os.path.join(extract_dir, filename)
        with open(filename) as afile:
            assert afile.read() == "123\n"

    for filename in ["хардлик.ече", "файл.тхт", "симлинк.ече"]:
        filename = os.path.join(extract_dir, filename)
        with open(filename) as afile:
            assert afile.read() == "привет\n"


def test_archive_postprocess():
    exts.fs.create_dirs("van")
    exts.fs.create_dirs("too")

    with open("van/sri.txt", "w") as f:
        f.write("sri")

    exts.fs.create_dirs("too/fo")
    exts.fs.create_dirs("too/faif")

    with open("too/faif/siks.txt", "w") as f:
        f.write("siks")
    tar_path = yatest.common.output_path("test_archive_postprocess.tar")

    def postprocess(src, dst, st_mode):
        logger.debug("postprocess (%s %s)", src, dst)
        if stat.S_ISREG(st_mode):
            os.unlink(src)

    exts.archive.create_tar(
        [
            (
                "van/sri.txt",
                "sri.txt",
            ),
            ("too", "too"),
        ],
        tar_path,
        postprocess=postprocess,
    )

    expected_tar_members = ['too/faif', 'too/fo', 'too/faif/siks.txt', 'sri.txt']

    with tarfile.open(tar_path) as tar:
        members = tar.getnames()
        assert members == expected_tar_members, members

    assert not os.path.exists('too/faif/siks.txt')
    assert not os.path.exists('van/sri.txt')
    assert os.path.exists('van')
    assert os.path.exists('too/faif')
