import hashlib
import os

import pytest
import yatest.common as yc

from mapreduce.yt.python.yt_stuff import yt_stuff  # noqa


_BIN_PATH = yc.binary_path('yt/cpp/mapreduce/library/blob/tools/file-yt/file-yt', )

# tests data
_FILE_480KB = 'file_480KB'
_FILE_480KB_MD5 = 'ab6aa45651e7a9cdbc153ffc1208c1fa'
_FILE_457MB = 'file_457MB'
_FILE_457MB_MD5 = 'd3a36ef8dfdd275f6c047b525230ceb4'
_FILE_0B_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'


@pytest.fixture(scope='module')
def prepare_yt(yt_stuff):  # noqa
    yt_client = yt_stuff.get_yt_client()
    yt_client.create('map_node', '//tmp/yt_wrapper', recursive=True, ignore_existing=True)


#  via http://stackoverflow.com/a/3431835/2513489
def _hash_bytestr_iter(bytesiter, hasher):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest()


def _file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)


def _get_file_md5(path):
    with open(path, 'rb') as in_:
        return _hash_bytestr_iter(_file_as_blockiter(in_), hashlib.md5())


def _touch(path, times=None):
    with open(path, 'a'):
        os.utime(path, times)


def test_upload_one_small_file(yt_stuff, prepare_yt):  # noqa
    table_path = '//one_small_file'
    out_path = yc.test_output_path('uploaded_file')
    cmd_create = (
        _BIN_PATH, 'create',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_upload = (
        _BIN_PATH, 'upload',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--path', os.path.abspath(_FILE_480KB),
        '--name', _FILE_480KB,
    )
    cmd_finish = (
        _BIN_PATH, 'finish',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_download = (
        _BIN_PATH, 'download',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--name', _FILE_480KB,
        '--path', out_path,
    )

    yc.execute(cmd_create, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_upload, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_finish, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_download, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})

    assert _FILE_480KB_MD5 == _get_file_md5(out_path)


def test_upload_one_big_file(yt_stuff, prepare_yt):  # noqa
    table_path = '//one_big_file'
    out_path = yc.test_output_path('uploaded_file')
    cmd_create = (
        _BIN_PATH, 'create',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_upload = (
        _BIN_PATH, 'upload',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--path', os.path.abspath(_FILE_457MB),
        '--name', _FILE_457MB,
    )
    cmd_finish = (
        _BIN_PATH, 'finish',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_download = (
        _BIN_PATH, 'download',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--name', _FILE_457MB,
        '--path', out_path,
    )

    yc.execute(cmd_create, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_upload, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_finish, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_download, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})

    assert _FILE_457MB_MD5 == _get_file_md5(out_path)


def test_upload_one_empty_file(yt_stuff, prepare_yt):  # noqa
    table_path = '//one_empty_file'
    in_path = yc.test_output_path('empty_file_to_upload')
    _touch(in_path)
    in_path_filename = os.path.basename(in_path)
    out_path = yc.test_output_path('uploaded_file')
    cmd_create = (
        _BIN_PATH, 'create',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_upload = (
        _BIN_PATH, 'upload',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--path', in_path,
        '--name', in_path_filename,
    )
    cmd_finish = (
        _BIN_PATH, 'finish',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
    )
    cmd_download = (
        _BIN_PATH, 'download',
        '--yt-proxy', yt_stuff.get_server(),
        '--yt-table', table_path,
        '--name', in_path_filename,
        '--path', out_path,
    )

    yc.execute(cmd_create, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_upload, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_finish, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})
    yc.execute(cmd_download, env={'YT_LOG_LEVEL': 'Error', 'YT_USE_CLIENT_PROTOBUF': '0'})

    assert _FILE_0B_MD5 == _get_file_md5(out_path)
