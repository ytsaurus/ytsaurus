# coding: utf-8

import os
import shutil
import six

import exts.fs
import exts.tmp

import library.python.archive as archive
from library.python.archive import (  # noqa
    GZIP,
    ZSTD,
    Compression,
    Level,
    get_compression_level,
    get_archive_filter_name,
)  # noqa


def extract_from_tar(tar_file_path, output_dir, strip_components=None, apply_mtime=False, entry_filter=None):
    exts.fs.create_dirs(output_dir)
    archive.extract_tar(
        tar_file_path, output_dir, strip_components=strip_components, apply_mtime=apply_mtime, entry_filter=entry_filter
    )


def create_tar(
    paths,
    tar_file_path,
    compression_filter=None,
    compression_level=None,
    fixed_mtime=0,
    onerror=None,
    postprocess=None,
    dereference=False,
    work_dir=None,
):
    '''
    Creates stable archive by default (bitexact for same content):
     - don't store mtime of the original files being compressed
     - processes files and dirs sorted by name
    Set fixed_mtime as None to disable it.
    :param paths: path to the target or list of tuples (path, archname)
    :param tar_file_path:
    :param compression_filter: None/gz/zstd
    :param compression_level:
    :param fixed_mtime: specify required mtime - allows to create stable archives
    :param onerror: function that accepts three parameters: src, dst, exc_info.
        There will another attempt to archive data if onerror returns True
    :param postprocess: function that accepts tree parameters: src, dst, st_mode
        which will be called when src is successfully added to the archive
    :param dereference: dereference symbolic links
    '''
    if isinstance(paths, six.string_types):
        # (path, arcname)
        paths = [(paths, ".")]
    with exts.tmp.temp_dir(dir=work_dir) as temp_dir:
        temp_tar_path = os.path.join(temp_dir, os.path.basename(tar_file_path))
        archive.tar(
            paths, temp_tar_path, compression_filter, compression_level, fixed_mtime, onerror, postprocess, dereference
        )
        shutil.move(temp_tar_path, tar_file_path)


def check_archive(tar_file_path):
    return archive.check_tar(tar_file_path)


def is_empty(tar_file_path):
    return archive.is_empty(tar_file_path)


_FILTER_SIGNATURE = {
    '7z': b'\x37\x7a\xbc\xaf\x27\x1c',
    'gz': b'\x1f\x8b',
    'lz4': b'\x04\x22\x4d\x18',
    'lzfse': b'\x62\x76\x78\x32',
    'xz': b'\xfd\x37\x7a\x58\x5a\x00\x00',
    'zip': b'\x50\x4b',
    'zstd': b'\x28\xb5\x2f\xfd',
}


def get_filter_type(filename):
    with open(filename, 'rb') as afile:
        magic = afile.read(max(len(v) for v in six.itervalues(_FILTER_SIGNATURE)))

    for t, m in six.iteritems(_FILTER_SIGNATURE):
        if magic.startswith(m):
            return t


def is_tar(filename):
    # https://www.gnu.org/software/tar/manual/html_node/Standard.html
    size = 262
    with open(filename, 'rb') as afile:
        header = afile.read(size)

    return len(header) == size and header.endswith(six.ensure_binary('ustar', encoding='ascii'))


def is_archive_type(filename):
    return is_tar(filename) or get_filter_type(filename)


def get_archive_filenames(filename):
    return archive.get_archive_filenames(filename)
