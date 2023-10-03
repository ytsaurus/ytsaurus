import logging
import os
import threading
from contextlib2 import ExitStack

from exts import filelock, fs, hashing, windows
from yalibrary import toolscache

from .common import (
    clean_dir
)


_GUARD_FILE_NAME = "INSTALLED"
_VERSION = 2
_refetched_paths = set()


logger = logging.getLogger(__name__)


class _InfraProcessSync(object):
    """Not all file locks guarantee synchronization among threads in a process"""
    _lock = threading.Lock()
    _SIZE_OF_LOCK_VECTOR = 128
    _being_downloaded = [None] * _SIZE_OF_LOCK_VECTOR

    @classmethod
    def resource_lock(cls, where):
        bucket = int(hashing.md5_value(where)[0:6], base=16) % cls._SIZE_OF_LOCK_VECTOR
        with cls._lock:
            if cls._being_downloaded[bucket] is None:
                cls._being_downloaded[bucket] = threading.Lock()

        return cls._being_downloaded[bucket]


def safe_resource_lock(resource_path):
    with ExitStack() as stack:
        stack.enter_context(_InfraProcessSync.resource_lock(resource_path))
        stack.enter_context(filelock.FileLock(resource_path + '.lock'))
        return stack.pop_all()


def installed(resource_path):
    try:
        return int(fs.read_file(os.path.join(resource_path, _GUARD_FILE_NAME)).strip()) >= _VERSION
    except Exception:
        logger.debug('not installed %s', resource_path)

    return False


def install_resource(resource_path, installer, force_refetch=False):
    toolscache.notify_tool_cache(resource_path)

    if not force_refetch and installed(resource_path):
        logger.debug('Resource seems to be installed in %s', resource_path)
        return resource_path

    with safe_resource_lock(resource_path):
        if force_refetch and resource_path not in _refetched_paths:
            logger.debug('Force refetch resource in path: %s', resource_path)
        elif installed(resource_path):
            logger.debug('Resource is already installed in %s', resource_path)
            return resource_path
        clean_dir(resource_path)
        installer()
        fs.write_file(os.path.join(resource_path, _GUARD_FILE_NAME), str(_VERSION))
    if force_refetch:
        _refetched_paths.add(resource_path)
    return resource_path


def update_resource(resource_path, updater):
    '''
        Update existing resource cache or create new if it doesn't exist before.
        Newly created resource cache is incomplete (doesn't have resource and guard files)
        and it is subject to delete during `true' resource install (by install_resource() function)
    '''
    with safe_resource_lock(resource_path):
        if not os.path.exists(resource_path):
            fs.create_dirs(resource_path)
        elif not os.path.isdir(resource_path):
            logger.debug('Will not install cache in %s', resource_path)
            return
        updater()


def install_symlink(resource_path, link_path):
    with safe_resource_lock(link_path):
        if not os.path.islink(link_path) or os.readlink(link_path) != resource_path:
            logger.debug('adding %s as alias for %s', link_path, resource_path)
            try:
                fs.ensure_removed(link_path)
                if windows.on_win():
                    fs.hardlink_tree(resource_path, link_path)
                else:
                    fs.symlink(resource_path, link_path)
            except (AttributeError, EnvironmentError) as e:
                logger.debug('can not create alias in %s for %s: %s', link_path, resource_path, e)
    return link_path
