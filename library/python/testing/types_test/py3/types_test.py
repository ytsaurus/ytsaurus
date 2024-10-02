import os
import sys
import errno
import typing
import hashlib
from unittest import mock

import __res as resource

import mypy.main
from mypy.fscache import FileSystemCache, copy_os_error

import pytest


class ArcadianCache(FileSystemCache):
    _stat_result = os.stat(sys.executable)
    _dirstat_result = os.stat(os.curdir)
    _modules_hierarchy = None

    _PREFIX = b'resfs/file/py/'

    def __init__(self, *args, **kwargs):
        if ArcadianCache._modules_hierarchy is None:
            ArcadianCache._modules_hierarchy = {}
            for _, path in resource.iter_keys(ArcadianCache._PREFIX):
                if path.endswith(b'.yapyc3'):
                    continue

                path = path.decode()
                target = ArcadianCache._modules_hierarchy
                path_chunks = path.split(os.path.sep)
                for chunk in path_chunks:
                    target = target.setdefault(chunk, {})

        super().__init__(*args, **kwargs)

    def __resolve(self, path: str) -> typing.List[str]:
        target = ArcadianCache._modules_hierarchy
        path_chunks = os.path.normpath(path).split(os.path.sep)

        for chunk in path_chunks:
            if chunk and chunk != '.':
                target = target[chunk]
        return list(target.keys())

    def stat(self, path: str) -> os.stat_result:
        if os.path.isabs(path):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        if path in self.stat_cache:
            return self.stat_cache[path]
        elif path in self.stat_error_cache:
            raise copy_os_error(self.stat_error_cache[path])

        try:
            res = bool(self.__resolve(path))
        except KeyError:
            return super().stat(path)
        else:
            result = self._dirstat_result if res else self._stat_result
            self.stat_cache[path] = result
            return result

    def listdir(self, path: str) -> typing.List[str]:
        if os.path.isabs(path):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        path = os.path.normpath(path)
        if path in self.listdir_cache:
            return self.listdir_cache[path]
        elif path in self.listdir_error_cache:
            raise copy_os_error(self.listdir_error_cache[path])

        try:
            target = self.__resolve(path)
        except KeyError:
            return super().listdir(path)
        else:
            self.listdir_cache[path] = target
            return target

    def isfile(self, path: str) -> bool:
        try:
            relpath = os.path.relpath(path)
            if relpath.endswith('.py'):
                modname = relpath[:-3].replace(os.path.sep, '.')
                resource.importer.get_filename(modname)
                return True
        except ImportError:
            pass

        try:
            return not self.__resolve(os.path.relpath(path))
        except KeyError:
            return super().isfile(path)

    def isdir(self, path: str) -> bool:
        if os.path.isabs(path):
            return False
        try:
            return bool(self.__resolve(path))
        except KeyError:
            return super().isdir(path)

    def exists(self, path: str) -> bool:
        if os.path.isabs(path):
            return False
        try:
            self.__resolve(path)
        except KeyError:
            return super().exists(path)
        else:
            return True

    def read(self, path: str) -> bytes:
        if path in self.read_cache:
            return self.read_cache[path]
        elif path in self.read_error_cache:
            raise copy_os_error(self.read_error_cache[path])

        binpath = ArcadianCache._PREFIX + os.path.normpath(path).encode()
        data = resource.find(binpath)
        if data is not None:
            if path not in self.hash_cache:
                self.hash_cache[path] = hashlib.md5(data).hexdigest()

            self.read_cache[path] = data
            return data

        return super().read(path)

    def samefile(self, f1: str, f2: str) -> bool:
        return f1 == f2


file_cache = ArcadianCache()


def _get_config_module():
    import conftest  # To consider: `from __tests__ import conftest`
    return conftest


def gen_filenames() -> typing.List[str]:
    # mypy_check_root should be a fixture defined in user's test case
    conftest = _get_config_module()

    source_root = conftest.mypy_check_root().encode('utf-8')
    result = [
        (source_root + path).decode()
        for _, path in resource.iter_keys(ArcadianCache._PREFIX + source_root)
        if path.endswith(b'.py') and not path.endswith(b'_pb2.py')
    ]
    assert result, 'not allowing an empty types test'
    return result


def gen_extra_params() -> typing.List[str]:
    conftest = _get_config_module()
    func = getattr(conftest, 'mypy_extra_params', None)
    if func is None:
        return []
    result = list(func())
    assert result, 'empty `mypy_extra_params` override is suspicious because it does nothing'
    return result


def get_config_file_resource() -> typing.Tuple[str, str]:
    conftest = _get_config_module()
    func = getattr(conftest, 'mypy_config_resource', None)
    if func is None:
        return None, None
    result = func()
    assert isinstance(result, tuple) and len(result) == 2
    return result


@pytest.fixture(scope='session')
def run_all():
    os.environ['MYPYPATH'] = ':'

    paths = gen_filenames()
    extra_params = gen_extra_params()
    config_file_pkg, config_file_name = get_config_file_resource()

    if config_file_pkg is not None:
        cfgfile_params = (
            '--config-package={}'.format(config_file_pkg),
            '--config-file={}'.format(config_file_name),
        )
    elif config_file_name:
        cfgfile_params = (
            '--config-file={}'.format(config_file_name),
        )
    else:
        cfgfile_params = (
            '--config-file=',  # should disable file-reading
        )
    common_params = (
        '--follow-import=silent',
        '--namespace-packages',
    )
    sources, options = mypy.main.process_options(
        list(common_params) + list(cfgfile_params) + list(extra_params) + list(paths),
        fscache=file_cache,
    )

    # Disable writing cache
    options.cache_dir = os.devnull

    messages = {}

    def flush_errors(new_messages: list[str], is_serious: bool) -> None:
        for message in new_messages:
            filename = message.split(':')[0]
            messages.setdefault(filename, list()).append(message)

    try:
        mypy.build.build(
            sources=sources,
            options=options,
            flush_errors=flush_errors,
            fscache=file_cache,
        )
    except mypy.errors.CompileError as e:
        raise RuntimeError(''.join(s + '\n' for s in e.messages))

    return messages


@mock.patch('mypy.main.FileSystemCache', new=lambda: file_cache)
@pytest.mark.parametrize("arcadia_filename", gen_filenames())
def test_file_types(arcadia_filename, run_all):
    result = run_all.get(arcadia_filename)
    status = bool(result)
    assert not status, '\n'.join([''] + result)
