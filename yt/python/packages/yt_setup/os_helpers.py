# -*- coding: utf-8 -*-

import logging
import os
import shutil
from contextlib import contextmanager


logger = logging.getLogger("os_helpers")


def chown_r(path, uid, gid):
    logger.info("Chown %s with uid %d and gid %d", path, uid, gid)
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chown(os.path.join(root, d), uid, gid)
        for f in files:
            if os.path.islink(os.path.join(root, f)):
                continue
            os.chown(os.path.join(root, f), uid, gid)


def chmod_r(path, permissions):
    logger.info("Chmod %s with permissions %o", path, permissions)
    for root, dirs, files in os.walk(path):
        for f in files:
            if os.path.islink(os.path.join(root, f)):
                continue
            os.chmod(os.path.join(root, f), permissions)


def rm_rf(path):
    """remove recursive"""
    logger.info("Remove %s", path)
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.unlink(path)


def cp(source_path, destination_path):
    shutil.copy(source_path, destination_path)


def cp_r(path, dest_dir, permissions=None):
    """copy recursive"""
    logger.info("Copy %s to %s", path, dest_dir)
    assert os.path.isdir(dest_dir)
    if os.path.isdir(path):
        shutil.copytree(path, os.path.join(dest_dir, os.path.basename(path)), symlinks=True)
        if permissions is not None:
            chmod_r(os.path.join(dest_dir, os.path.basename(path)), permissions)
    else:
        shutil.copy2(path, dest_dir)
        if permissions is not None:
            os.chmod(os.path.join(dest_dir, os.path.basename(path)), permissions)


def replace(path, dest_dir):
    dst_path = os.path.join(dest_dir, os.path.basename(path))
    if os.path.exists(dst_path):
        rm_rf(dst_path)
    cp_r(path, dest_dir)


def replace_symlink(source, destination):
    if os.path.lexists(destination):
        logger.info("Remove %s", destination)
        os.remove(destination)
    logger.info("Create symlink %s to %s", destination, source)
    os.symlink(source, destination)


def apply_multiple(times, func, argument):
    for _ in range(times):
        argument = func(argument)
    return argument


def touch(path):
    with open(path, "a"):
        pass


@contextmanager
def cd(dir):
    current_dir = os.getcwd()
    os.chdir(dir)
    try:
        yield
    finally:
        os.chdir(current_dir)
