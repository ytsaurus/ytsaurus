# -*- coding: utf-8 -*-

from six.moves import xrange
import logging
import os
import shutil


logger = logging.getLogger("prepare_source_tree")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def rm_rf(path):
    """remove recursive"""
    logger.info("Remove %s", path)
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.unlink(path)


def cp_r(path, dest_dir):
    """copy recursive"""
    logger.info("Copy %s to %s", path, dest_dir)
    assert os.path.isdir(dest_dir)
    if os.path.isdir(path):
        shutil.copytree(path, os.path.join(dest_dir, os.path.basename(path)))
    else:
        shutil.copy2(path, dest_dir)


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
    for _ in xrange(times):
        argument = func(argument)
    return argument
