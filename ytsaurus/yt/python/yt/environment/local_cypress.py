import os
import os.path
import logging

import yt.yson as yson

from yt.common import YtError

logger = logging.getLogger("YtLocal")


def _get_attributes_from_local_dir(local_path, meta_files_suffix):
    meta_file_path = os.path.join(local_path, meta_files_suffix)
    if os.path.isfile(meta_file_path):
        with open(meta_file_path, "rb") as f:
            try:
                meta = yson.load(f)
            except yson.YsonError:
                logger.exception("Failed to load meta file {0}, meta will not be processed".format(meta_file_path))
                return {}
            return meta.get("attributes", {})
    return {}


def _create_map_node_from_local_dir(local_path, dest_path, meta_files_suffix, client):
    attributes = _get_attributes_from_local_dir(local_path, meta_files_suffix)
    client.create("map_node", dest_path, attributes=attributes, ignore_existing=True)


def _create_node_from_local_file(local_filename, dest_filename, meta_files_suffix, client):
    if not os.path.isfile(local_filename + meta_files_suffix):
        logger.warning("Found file {0} without meta info, skipping".format(local_filename))
        return

    with open(local_filename + meta_files_suffix, "rb") as f:
        try:
            meta = yson.load(f)
        except yson.YsonError:
            logger.exception("Failed to load meta file for table {0}, skipping".format(local_filename))
            return

        attributes = meta.get("attributes", {})

        if meta["type"] == "table":
            if "format" not in meta:
                logger.warning("Found table {0} with unspecified format".format(local_filename))
                return

            sorted_by = attributes.pop("sorted_by", [])

            client.create("table", dest_filename, attributes=attributes)
            with open(local_filename, "rb") as table_file:
                client.write_table(dest_filename, table_file, format=meta["format"], raw=True)

            if sorted_by:
                client.run_sort(dest_filename, sort_by=sorted_by)

        elif meta["type"] == "file":
            client.create("file", dest_filename, attributes=attributes)
            with open(local_filename, "rb") as local_file:
                client.write_file(dest_filename, local_file)

        else:
            logger.warning("Found file {0} with currently unsupported type {1}"
                           .format(local_filename, meta["type"]))


def _synchronize_cypress_with_local_dir(local_cypress_dir, meta_files_suffix, client):
    cypress_path_prefix = "//"

    if meta_files_suffix is None:
        meta_files_suffix = ".meta"

    local_cypress_dir = os.path.abspath(local_cypress_dir)
    if not os.path.exists(local_cypress_dir):
        raise YtError("Local Cypress directory does not exist")

    root_attributes = _get_attributes_from_local_dir(local_cypress_dir, meta_files_suffix)
    for key in root_attributes:
        client.set_attribute("/", key, root_attributes[key])

    for root, dirs, files in os.walk(local_cypress_dir):
        rel_path = os.path.abspath(root)[len(local_cypress_dir) + 1:]  # +1 to skip last /
        for dir in dirs:
            _create_map_node_from_local_dir(os.path.join(root, dir),
                                            os.path.join(cypress_path_prefix, rel_path, dir),
                                            meta_files_suffix,
                                            client)
        for file in files:
            if file.endswith(meta_files_suffix):
                continue
            _create_node_from_local_file(os.path.join(root, file),
                                         os.path.join(cypress_path_prefix, rel_path, file),
                                         meta_files_suffix,
                                         client)
