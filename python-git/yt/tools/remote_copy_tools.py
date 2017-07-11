from yt.wrapper.common import round_up_to, bool_to_string, MB, GB, update, get_disk_size
from yt.wrapper.local_mode import enable_local_files_usage_in_job
from yt.wrapper.ypath import ypath_join
from yt.tools.conversion_tools import transform
from yt.common import get_value, set_pdeathsig

import yt.yson as yson
import yt.json as json
import yt.logger as logger
import yt.subprocess_wrapper as subprocess

from yt.packages.six.moves import xrange, map as imap

import yt.wrapper as yt

import os
import time
import shutil
import sys
import tempfile
import tarfile
from copy import deepcopy

# NB: We have no ability to discover maximum number of jobs in map operation in cluster.
# So we suppose that it is 100000 like on most clusters.
DEFAULT_MAXIMUM_YT_JOB_COUNT = 100000

class IncorrectRowCount(yt.YtError):
    pass

class IncorrectFileSize(yt.YtError):
    pass

class Kiwi(object):
    def __init__(self, url, kwworm_binary, kwworm_options):
        self.url = url
        self.kwworm_binary = kwworm_binary
        self.kwworm_options = kwworm_options

    def _get_option_key(self, option):
        if isinstance(option, list):
            return option[0]
        return option

    def _option_to_str(self, option):
        if isinstance(option, list):
            return " ".join(option)
        return option

    def _join_options(self, base_options, additional_options):
        result = deepcopy(base_options)
        key_to_index = dict((self._get_option_key(option), index) for index, option in enumerate(base_options))
        for option in additional_options:
            key = self._get_option_key(option)
            if key in key_to_index:
                result[key_to_index[key]] = option
            else:
                result.append(option)
        return result

    def get_write_command(self, kiwi_user, kwworm_options=None):
        if kwworm_options is None:
            kwworm_options = []
        command = "./kwworm " + " ".join(imap(self._option_to_str, self._join_options(self.kwworm_options, kwworm_options)))
        return command.format(kiwi_url=self.url, kiwi_user=kiwi_user)

def _force_parent_dir(path, client):
    try:
        type = client.get(path + "/@type")
        if type in ["link", "map_node"]:
            return
        raise yt.YtError("Parent directory {0} exists and has non-directory type {1}".format(path, type))
    except yt.YtResponseError as err:
        if not err.is_resolve_error():
            raise

    client.create("map_node", path, recursive=True, ignore_existing=True)

def _check_output(command, silent=False, **kwargs):
    logger.info("Executing command '{}'".format(command))
    result = subprocess.check_output(command, preexec_fn=set_pdeathsig, **kwargs)
    logger.info("Command '{}' successfully executed".format(command))
    return result

def _which(file):
    for path in os.environ["PATH"].split(":"):
        if os.path.exists(path + "/" + file):
            return path + "/" + file
    return None

def _pack_module(module_name, output_dir):
    module = __import__(module_name)
    module_path = module.__file__
    for suffix in ["__init__.py", "__init__.pyc"]:
        if module_path.endswith(suffix):
            module_path = module_path[:-len(suffix)]
    if module_path.endswith(".egg") or module_path.endswith(".py") or module_path.endswith(".pyc") or module_path.endswith(".so"):
        archive_filename = os.path.join(output_dir, module_name + ".tar")
        tar = tarfile.open(archive_filename, "w")
        tar.add(module_path, os.path.basename(module_path))
        tar.close()
        return archive_filename
    else:
        archive_filename = os.path.join(output_dir, module_name + ".tar")
        tar = tarfile.open(archive_filename, "w")
        for root, dirs, files in os.walk(module_path, followlinks=True):
            for file in files:
                file_path = os.path.join(root, file)
                assert file_path.startswith(module_path)
                destination = os.path.join(module_name, file_path[len(module_path):])
                tar.add(os.path.realpath(file_path), destination)
        tar.close()
        return archive_filename

def _pack_string(name, script, output_dir):
    filename = os.path.join(output_dir, name)
    with open(filename, "w") as fout:
        fout.write(script)
    return filename

def _split(item_count, split_size, total_size):
    if item_count == 0:
        return []
    split_item_count = max(1, (item_count * split_size) // total_size)
    return [(i * split_item_count, min((i + 1) * split_item_count, item_count))
            for i in xrange(1 + ((item_count - 1) // split_item_count))]

def _split_file(split_size, total_size):
    offset = 0
    while total_size > 0:
        yield offset, min(split_size, total_size)
        total_size -= split_size
        offset += split_size

def _get_temp_file_names(temp_dir, ranges):
    return [temp_dir + "/part_" + str(range["offset"]) for range in ranges]

def _estimate_split_size(total_size):
    # XXX(asaitgalin): if table is bigger than 100 TiB and sorted it is not ok to
    # use default 1 GB split_size because job count will be greater than
    # DEFAULT_MAXIMUM_YT_JOB_COUNT and two ranges will be processed by the same job
    # which may violate destination table sort order.
    split_size = 1024 * yt.common.MB
    ranges_count = total_size // split_size
    if ranges_count > DEFAULT_MAXIMUM_YT_JOB_COUNT:
        scale_factor = 1.01 * ranges_count / DEFAULT_MAXIMUM_YT_JOB_COUNT
        return int(scale_factor * split_size)
    return split_size

def _slice_yt_table_evenly(client, table, split_size=None):
    """ Return list with correct YT ranges. """
    # TODO(ignat): Implement fetch command in driver and slice it by chunks.
    chunk_count = client.get(table + "/@chunk_count")
    data_size = client.get(table + "/@uncompressed_data_size")
    split_size = get_value(split_size, _estimate_split_size(data_size))
    return [{"lower_limit": {"chunk_index": start}, "upper_limit": {"chunk_index": end}}
            for start, end in _split(chunk_count, split_size, data_size)]

def _slice_yt_file_evenly(client, file, split_size=None):
    """ Return list with correct YT ranges. """
    data_size = client.get(file + "/@uncompressed_data_size")
    split_size = get_value(split_size, _estimate_split_size(data_size))
    return [{"offset": offset, "limit": limit}
            for offset, limit in _split_file(split_size, data_size)]

def _set_mapper_settings_for_read_from_yt(source_client=None, destination_client=None, spec=None):
    if "mapper" not in spec:
        spec["mapper"] = {}
    # NB: yt2 read usually consumpt less than 600 Mb, but sometimes can use more than 1Gb of memory.
    if "cpu_limit" not in spec["mapper"]:
        spec["mapper"]["cpu_limit"] = 0.5
    spec["mapper"]["memory_limit"] = 4 * GB
    spec["mapper"]["memory_reserve_factor"] = 0.25
    if "tmpfs_size" in spec["mapper"]:
        spec["mapper"]["memory_limit"] += spec["mapper"]["tmpfs_size"]

    if "environment" not in spec["mapper"]:
        spec["mapper"]["environment"] = {}

    if enable_local_files_usage_in_job(destination_client):
        spec["mapper"]["environment"]["PYTHONPATH"] = ":".join(sys.path)

    spec["mapper"]["environment"]["YT_FORBID_REQUESTS_FROM_JOB"] = "0"
    spec["mapper"]["environment"]["YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB"] = "1"

    if source_client is not None:
        spec.setdefault("secure_vault", {})
        spec["secure_vault"]["TOKEN"] = source_client.config["token"]

def _set_tmpfs_settings(spec, files, client, yt_files=None, reserved_size=0):
    yt_files = get_value(yt_files, [])

    if "mapper" not in spec:
        spec["mapper"] = {}

    spec["mapper"]["tmpfs_path"] = "."
    spec["mapper"]["copy_files"] = True

    files_size = 0
    for file_ in files:
        file_size = get_disk_size(file_)

        # NOTE: Assuming here that files compressed by tar without compression and
        # multiplying by two because space for uncompressed files should be added too.
        if os.path.basename(file_).endswith(".tar"):
            files_size += file_size * 2
        else:
            files_size += file_size

    total_size = reserved_size + files_size + \
        sum(round_up_to(client.get_attribute(path, "uncompressed_data_size"), 4 * 1024) for path in yt_files)

    spec["mapper"]["tmpfs_size"] = int(MB + 1.01 * total_size)

def shellquote(s):
    return "'" + s.replace("'", "'\\''") + "'"

class ReadCommandBuilder(object):
    def __init__(self, script_name):
        self._script_name = script_name
        self._args = []
        self._package_files = []

    def add_string_argument(self, argument, value):
        self._args.append((argument, value))
        return self

    def add_file_argument(self, argument, file_path):
        self._package_files.append(file_path)
        self._args.append((argument, os.path.basename(file_path)))
        return self

    def add_file(self, file_path):
        self._package_files.append(file_path)
        return self

    def get_command(self):
        args = " ".join("{0} {1}".format(argument, value) for argument, value in self._args)
        command = self._script_name + " " + args
        return command

    def get_package_files(self):
        return self._package_files

    def build(self):
        return self.get_command(), self.get_package_files()

def _prepare_read_builder(script_name, tmp_dir, data_proxy_role, pack_yt_wrapper, pack_yson_bindings):
    builder = ReadCommandBuilder(script_name)
    if pack_yt_wrapper:
        builder.add_file_argument("--package-file", _pack_module("yt", tmp_dir))
    if pack_yson_bindings:
        builder.add_file_argument("--package-file", _pack_module("yt_yson_bindings", tmp_dir))

    proxy_discovery_url = "hosts"
    if data_proxy_role:
        proxy_discovery_url = "hosts/" + data_proxy_role

    config = {
        "proxy": {
            "proxy_discovery_url": proxy_discovery_url,
        },
        "read_retries": {
            "enable": True,
            "create_transaction_and_take_snapshot_lock": False
        },
        # To use latest version of API.
        "default_api_version_for_http": None
    }

    config_file = _pack_string("config.json", json.dumps(config, indent=2), tmp_dir)
    builder.add_file_argument("--config-file", config_file)
    return builder

def _prepare_read_table_from_yt_command(yt_client, src, format, tmp_dir, data_proxy_role, pack_yt_wrapper,
                                        pack_yson_bindings, input_type="json"):
    if len(yt.TablePath(src, client=yt_client).attributes.get("ranges", [])) > 1:
        raise yt.YtError("Reading slices from table with multiple ranges is not supported")
    assert yt_client.COMMAND_PARAMS["transaction_id"] is not None
    builder = _prepare_read_builder("python read_from_yt.py", tmp_dir, data_proxy_role, pack_yt_wrapper,
                                    pack_yson_bindings)
    command, files = builder \
        .add_string_argument("--proxy", yt_client.config["proxy"]["url"]) \
        .add_string_argument("--format", shellquote(format)) \
        .add_string_argument("--table", shellquote(src)) \
        .add_string_argument("--input-type", input_type) \
        .add_string_argument("--tx", yt_client.COMMAND_PARAMS["transaction_id"]) \
        .add_file(os.path.abspath(os.path.join(os.path.dirname(__file__), "read_from_yt.py"))) \
        .build()
    return command, files

def _prepare_read_file_from_yt_command(destination_client, source_client, src, temp_files_dir, tmp_dir, data_proxy_role,
                                       pack_yt_wrapper, pack_yson_bindings, erasure_codec=None, compression_codec=None):
    assert source_client.COMMAND_PARAMS["transaction_id"] is not None
    assert destination_client.COMMAND_PARAMS["transaction_id"] is not None
    builder = _prepare_read_builder("python read_file_from_yt.py", tmp_dir, data_proxy_role, pack_yt_wrapper,
                                    pack_yson_bindings)
    command, files = builder \
        .add_string_argument("--source-proxy", source_client.config["proxy"]["url"]) \
        .add_string_argument("--destination-proxy", destination_client.config["proxy"]["url"]) \
        .add_string_argument("--file", shellquote(src)) \
        .add_string_argument("--tmp-dir", temp_files_dir) \
        .add_string_argument("--erasure-codec", erasure_codec) \
        .add_string_argument("--compression-codec", compression_codec) \
        .add_string_argument("--src-tx", source_client.COMMAND_PARAMS["transaction_id"]) \
        .add_string_argument("--dst-tx", destination_client.COMMAND_PARAMS["transaction_id"]) \
        .add_file(os.path.abspath(os.path.join(os.path.dirname(__file__), "read_file_from_yt.py"))) \
        .build()
    return command, files

def _raw_read_table_from_yt(source_client, table, format):
    client = deepcopy(source_client)
    client.config["read_retries"]["create_transaction_and_take_snapshot_lock"] = False
    return client.read_table(table, format, raw=True)

def _read_file_from_yt(source_client, file):
    client = deepcopy(source_client)
    client.config["read_retries"]["create_transaction_and_take_snapshot_lock"] = False
    return client.read_file(file)

def check_permission(client, permission, path):
     user_name = client.get_user_name(client.config["token"])
     permission = client.check_permission(user_name, permission, path)
     return permission["action"] == "allow"

def copy_user_attributes(source_client, destination_client, source_table, destination_table):
    source_attributes = source_client.get(source_table + "/@")

    for attribute in source_attributes.get("user_attribute_keys", []):
        destination_client.set_attribute(destination_table, attribute, source_attributes[attribute])

def copy_additional_attributes(source_client, destination_client, source_table, destination_table, attributes_list):
    # It must be called outside of transactions, since some system attributes cannot be set inside transactions.
    if attributes_list is None:
        return

    source_attributes = source_client.get(source_table + "/@")
    for attribute in attributes_list:
        if attribute in source_attributes:
            destination_client.set_attribute(destination_table, attribute, source_attributes[attribute])

def set_codec(yt_client, dst, codec, codec_type, dst_type="table"):
    attribute_name = codec_type + "_codec"
    if codec is not None:
        if not yt_client.exists(dst):
            yt_client.create(dst_type, dst, attributes={attribute_name: codec})
        yt_client.set_attribute(dst, attribute_name, codec)

def copy_yt_to_yt(source_client, destination_client, src, dst, network_name,
                  copy_spec_template=None, postprocess_spec_template=None,
                  compression_codec=None, erasure_codec=None, external=None,
                  additional_attributes=None, schema_inference_mode=None):
    copy_spec_template = get_value(copy_spec_template, {})
    if schema_inference_mode is not None:
        copy_spec_template["schema_inference_mode"] = schema_inference_mode

    src = yt.TablePath(src, client=source_client)
    compressed_data_size = source_client.get_attribute(src, "compressed_data_size")
    chunk_count = source_client.get_attribute(src, "chunk_count")

    if chunk_count > 100 and compressed_data_size // chunk_count < 10 * MB and not src.has_delimiters():
        if check_permission(source_client, "write", str(src)):
            try:
                # TODO(ignat): introduce preprocess spec template
                merge_spec = deepcopy(copy_spec_template)
                del merge_spec["pool"]
                merge_spec["combine_chunks"] = bool_to_string(True)
                source_client.run_merge(src, src, spec=merge_spec)
            except yt.YtError as error:
                raise yt.YtError("Failed to merge source table", inner_errors=[error])
        else:
            raise yt.YtError("Failed to merge source table {0}, no write permission. "
                             "If you can not get a write permission use 'proxy' copy method".format(str(src)))

    _force_parent_dir(os.path.dirname(dst), client=destination_client)

    cluster_connection = None
    try:
        cluster_connection = source_client.get("//sys/@cluster_connection")
    except yt.YtError as err:
        if not err.is_resolve_error():
            raise

    with destination_client.Transaction(attributes={"title": "copy_yt_to_yt transaction"}):
        kwargs = {"cluster_name": source_client._name}
        if cluster_connection is not None:
            kwargs["cluster_connection"] = cluster_connection

        if external is not None:
            destination_client.create("table", dst, attributes={"external": external})

        destination_client.run_remote_copy(
            src,
            dst,
            copy_attributes=True,
            network_name=network_name,
            spec=copy_spec_template,
            **kwargs)

        for attr_name, value in [("compression_codec", compression_codec), ("erasure_codec", erasure_codec), ("optimize_for", None)]:
            if value is None:
                try:
                    destination_client.set(dst + "/@" + attr_name, source_client.get(str(src) + "/@" + attr_name))
                except yt.YtError as err:
                    # Attribute 'optimize_for' may be missing on old tables.
                    if not err.is_resolve_error():
                        raise

        if erasure_codec is not None or compression_codec is not None:
            transform(str(dst), compression_codec=compression_codec, erasure_codec=erasure_codec,
                      yt_client=destination_client, spec=postprocess_spec_template, check_codecs=True)

    copy_additional_attributes(source_client, destination_client, src, dst, additional_attributes)

def copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, data_proxy_role,
                                copy_spec_template=None, postprocess_spec_template=None, default_tmp_dir=None,
                                compression_codec=None, erasure_codec=None, external=None,
                                intermediate_format=None, small_table_size_threshold=None, force_copy_with_operation=False,
                                additional_attributes=None, schema_inference_mode=None, pack_yt_wrapper=True,
                                pack_yson_bindings=True):
    schema_inference_mode = get_value(schema_inference_mode, "auto")
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)

    intermediate_format = yt.create_format(get_value(intermediate_format, "yson"))

    attributes = {"title": "copy_yt_to_yt_through_proxy"}

    _force_parent_dir(os.path.dirname(dst), client=destination_client)

    destination_client.config["table_writer"] = {
        "max_row_weight": 128 * MB,
        "desired_chunk_size": 2 * GB
    }

    try:
        with source_client.Transaction(attributes=attributes), destination_client.Transaction(attributes=attributes):
            # NB: for reliable access to table under snapshot lock we should use id.
            src_name = src
            src = yt.TablePath(src, client=source_client)
            src = yt.TablePath("#" + source_client.get(src + "/@id"), attributes=src.attributes, simplify=False, client=source_client)
            source_client.lock(src, mode="snapshot")

            dst_table = yt.TablePath(dst, simplify=False)

            if compression_codec is None:
                compression_codec = source_client.get(str(src) + "/@compression_codec")
            if erasure_codec is None:
                erasure_codec = source_client.get(str(src) + "/@erasure_codec")
            set_codec(destination_client, dst, compression_codec, "compression")

            try:
                destination_client.set(str(dst) + "/@optimize_for", source_client.get(str(src) + "/@optimize_for"))
            except yt.YtError as err:
                # Attribute 'optimize_for' may be missing on old tables.
                if not err.is_resolve_error():
                    raise

            if external is not None:
                destination_client.set(str(dst) + "/@external", external)

            sorted_by = None
            if source_client.exists(src + "/@sorted_by"):
                sorted_by = source_client.get(src + "/@sorted_by")
            src_schema = source_client.get(src + "/@schema")
            src_schema_mode = source_client.get(src + "/@schema_mode")
            dst_schema_mode = destination_client.get(dst_table + "/@schema_mode")

            if schema_inference_mode == "from_input" or (schema_inference_mode == "auto" and dst_schema_mode == "weak"):
                if src_schema_mode == "strong":
                    dst_table.attributes["schema"] = src_schema
                else:
                    if sorted_by is not None:
                        dst_table.attributes["sorted_by"] = sorted_by

            if small_table_size_threshold is not None and \
                    source_client.get(str(src) + "/@uncompressed_data_size") < small_table_size_threshold and \
                    not force_copy_with_operation:
                logger.info("Source table '%s' is small, copying without operation on destination cluster",
                            str(src_name))
                stream = _raw_read_table_from_yt(source_client, src, intermediate_format)
                try:
                    set_codec(destination_client, dst, erasure_codec, "erasure")
                    destination_client.write_table(dst_table, stream, format=intermediate_format, raw=True)
                finally:
                    stream.close()
            else:
                command, files = _prepare_read_table_from_yt_command(
                    source_client,
                    src.to_yson_string(),
                    str(intermediate_format),
                    tmp_dir,
                    data_proxy_role,
                    pack_yt_wrapper=pack_yt_wrapper,
                    pack_yson_bindings=pack_yson_bindings)

                ranges = _slice_yt_table_evenly(source_client, src)

                temp_table = destination_client.create_temp_table(prefix=os.path.basename(str(src)))
                destination_client.write_table(temp_table, ranges, format=yt.JsonFormat(), raw=False)

                spec = deepcopy(get_value(copy_spec_template, {}))
                spec["data_size_per_job"] = 1

                _set_tmpfs_settings(spec=spec, files=files, client=destination_client)
                _set_mapper_settings_for_read_from_yt(source_client=source_client,
                                                      destination_client=destination_client,
                                                      spec=spec)

                destination_client.run_map(
                    command,
                    temp_table,
                    dst_table,
                    files=files,
                    spec=spec,
                    ordered=True,
                    input_format=yt.JsonFormat(),
                    output_format=intermediate_format)

            row_count = source_client.get(src + "/@row_count")
            result_row_count = destination_client.row_count(dst)
            if not src.has_delimiters() and row_count != result_row_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            transform(
                str(dst_table),
                erasure_codec=erasure_codec,
                yt_client=destination_client,
                spec=postprocess_spec_template,
                check_codecs=True)

            copy_user_attributes(source_client, destination_client, src, dst)

        copy_additional_attributes(source_client, destination_client, src, dst, additional_attributes)

    finally:
        shutil.rmtree(tmp_dir)

def copy_file_yt_to_yt(source_client, destination_client, src, dst, data_proxy_role,
                       copy_spec_template=None, default_tmp_dir=None, compression_codec=None,
                       erasure_codec=None, intermediate_format=None, small_file_size_threshold=None,
                       force_copy_with_operation=False, additional_attributes=None, temp_files_dir=None,
                       pack_yt_wrapper=True, pack_yson_bindings=True):
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)
    intermediate_format = yt.create_format(get_value(intermediate_format, "json"))

    attributes = {"title": "copy_file_yt_to_yt"}

    _force_parent_dir(os.path.dirname(dst), client=destination_client)

    try:
        with source_client.Transaction(attributes=attributes), destination_client.Transaction(attributes=attributes):
            src_name = src
            src = yt.FilePath(src, client=source_client)
            src.name = yson.to_yson_type("#" + source_client.get(src_name + "/@id"), attributes=src.attributes)
            source_client.lock(src, mode="snapshot")

            if temp_files_dir is None:
                temp_files_dir = ypath_join(destination_client.config["remote_temp_files_directory"],
                                            destination_client.COMMAND_PARAMS["transaction_id"])
            if not destination_client.exists(temp_files_dir):
                destination_client.mkdir(temp_files_dir, recursive=True)

            if compression_codec is None:
                compression_codec = source_client.get(str(src) + "/@compression_codec")
            if erasure_codec is None:
                erasure_codec = source_client.get(str(src) + "/@erasure_codec")

            set_codec(destination_client, dst, erasure_codec, "erasure", "file")
            set_codec(destination_client, dst, compression_codec, "compression", "file")

            data_size = source_client.get(src.name + "/@uncompressed_data_size")

            if small_file_size_threshold is not None and \
                    source_client.get(str(src) + "/@uncompressed_data_size") < small_file_size_threshold and \
                    not force_copy_with_operation:
                logger.info("Source file '%s' is small, copying without operation on destination cluster",
                            str(src_name))

                stream = _read_file_from_yt(source_client, src)
                try:
                    destination_client.write_file(dst, stream)
                finally:
                    stream.close()
            else:
                temp_table_input = None
                temp_table_output = None
                try:
                    ranges = _slice_yt_file_evenly(source_client, src.name)
                    if not ranges:
                        destination_client.write_file(dst, "")
                    else:
                        command, files = _prepare_read_file_from_yt_command(
                            destination_client,
                            source_client,
                            src.to_yson_string(),
                            temp_files_dir,
                            tmp_dir,
                            data_proxy_role,
                            pack_yt_wrapper=pack_yt_wrapper,
                            pack_yson_bindings=pack_yson_bindings,
                            erasure_codec=erasure_codec,
                            compression_codec=compression_codec)

                        temp_table_input = destination_client.create_temp_table(prefix=os.path.basename(src.name))
                        temp_table_output = destination_client.create_temp_table(prefix=os.path.basename(src.name))
                        destination_client.write_table(temp_table_input, ranges, format=yt.JsonFormat(), raw=False)

                        spec = deepcopy(get_value(copy_spec_template, {}))
                        spec["data_size_per_job"] = 1
                        _set_tmpfs_settings(spec=spec, files=files, client=destination_client)
                        _set_mapper_settings_for_read_from_yt(source_client=source_client,
                                                              destination_client=destination_client,
                                                              spec=spec)

                        destination_client.run_map(
                            command,
                            temp_table_input,
                            temp_table_output,
                            files=files,
                            spec=spec,
                            ordered=True,
                            input_format=yt.JsonFormat(),
                            output_format=intermediate_format)

                        destination_client.concatenate(_get_temp_file_names(temp_files_dir, ranges), dst)


                finally:
                    destination_client.remove(temp_files_dir, force=True, recursive=True)
                    if temp_table_input is not None:
                        destination_client.remove(temp_table_input, force=True)
                    if temp_table_output is not None:
                        destination_client.remove(temp_table_output, force=True)

            result_data_size = destination_client.get(dst + "/@uncompressed_data_size")
            if data_size != result_data_size:
                error = "Incorrect file size (expected: %d, actual: %d)" % (data_size, result_data_size)
                logger.error(error)
                raise IncorrectFileSize(error)

            copy_user_attributes(source_client, destination_client, src.name, dst)

        copy_additional_attributes(source_client, destination_client, src, dst, additional_attributes)

    finally:
        shutil.rmtree(tmp_dir)

def _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command, ranges, kiwi_user, files=None,
                  yt_files=None, copy_spec_template=None, write_to_table=False, protobin=True,
                  kwworm_options=None, default_tmp_dir=None, write_errors_script=None, yt_client=None):
    extract_value_script_to_table = """\
import sys
import struct

count = 0
while True:
    s = sys.stdin.read(4)
    if not s:
        break
    length = struct.unpack('i', s)[0]
    if count % 2 == 0:
        sys.stdin.read(length)
    else:
        sys.stdout.write('\\x00\\x00\\x00\\x00')
        sys.stdout.write(s)
        sys.stdout.write(sys.stdin.read(length))
    count += 1
"""

    extract_value_script_to_worm = """\
import sys
import struct

count = 0
while True:
    s = sys.stdin.read(4)
    if not s:
        break
    length = struct.unpack('i', s)[0]
    if count % 2 == 0:
        sys.stdin.read(length)
    else:
        sys.stdout.write(sys.stdin.read(length))
    count += 1
"""

    range_table = kiwi_transmittor.create_temp_table(prefix=os.path.basename(src))
    kiwi_transmittor.write_table(range_table, ranges, format=yt.JsonFormat())

    output_table = kiwi_transmittor.create_temp_table()
    kiwi_transmittor.set(output_table + "/@replication_factor", 1)

    spec = deepcopy(get_value(copy_spec_template, {}))
    spec = update(
        {
            "data_size_per_job": 1,
            "locality_timeout": 0,
            "pool": "transfer_kiwi",
            "max_failed_job_count": 1000,
            "mapper": {
                "cpu_limit": 0.5,
                # After investigation of several copy jobs we have found the following:
                # 1) kwworm memory consumption is in range (200-500MB)
                # 2) yt2 read consumption is in range (200-600MB)
                # 3) In some corner cases yt2 can use above than 1GB.
                # 4) Other processes uses less than 100 MB.
                "memory_limit": 4 * GB,
                "memory_reserve_factor": 0.35,
                "environment": {
                    "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
                },
            },
        },
        spec)

    if write_to_table:
        extract_value_script = extract_value_script_to_table
        write_command = ""
        finalize_command = ""
        output_format = yt.YamrFormat(lenval=True,has_subkey=False)
    else:
        extract_value_script = extract_value_script_to_worm
        kiwi_command = kiwi_client.get_write_command(kiwi_user=kiwi_user, kwworm_options=kwworm_options)
        write_command = "| {0} >output 2>&1; ".format(kiwi_command)
        write_errors_command = ""
        if write_errors_script is not None:
            write_errors_command = "cat output | python write_errors.py; "
        finalize_command = "RESULT=$?; cat output; head -n 500 output >&2; tail -n 500 output >&2;{0} exit $RESULT"\
                           .format(write_errors_command)
        output_format = yt.SchemafulDsvFormat(columns=["error"])

    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)
    if files is None:
        files = []
    command_script = "set -o pipefail; {0} | python extract_value.py {1} {2}".format(read_command, write_command, finalize_command)
    files.append(_pack_string("command.sh", command_script, tmp_dir))
    files.append(_pack_string("extract_value.py", extract_value_script, tmp_dir))
    if write_errors_script is not None:
        files.append(_pack_string("write_errors.py", write_errors_script, tmp_dir))
    files.append(kiwi_client.kwworm_binary)

    # NOTE: reserved_size is for "output" file
    _set_tmpfs_settings(spec=spec, files=files, client=kiwi_transmittor, yt_files=yt_files, reserved_size=GB)
    spec["mapper"]["memory_limit"] += spec["mapper"]["tmpfs_size"]

    try:
        kiwi_transmittor.run_map(
            "bash -ux command.sh",
            range_table,
            output_table,
            files=files,
            yt_files=yt_files,
            input_format=yt.JsonFormat(),
            output_format=output_format,
            spec=spec,
            ordered=True)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_kiwi(yt_client, kiwi_client, kiwi_transmittor, src, table_for_errors,
                    pack_yt_wrapper=True, pack_yson_bindings=True, copy_spec_template=None, **kwargs):
    write_errors_script_template = """\
import sys
import os
import yt.wrapper as yt

def gen_rows():
    for line in sys.stdin:
        yield {{"error": line.strip()}}

client = yt.YtClient(proxy="{0}", token=os.environ.get("YT_SECURE_VAULT_TOKEN"))
table = yt.TablePath("{1}", append=True, client=client)
client.create("table", table, ignore_existing=True)
client.write_table(table, gen_rows())
"""

    ranges = _slice_yt_table_evenly(yt_client, src, 512 * yt.common.MB)
    data_proxy_role = kwargs.pop("data_proxy_role", None)

    tmp_dir = tempfile.mkdtemp(dir=kwargs.get("default_tmp_dir"))
    try:
        with yt_client.Transaction(attributes={"title": "copy_yt_to_kiwi"}):
            yt_client.lock(src, mode="snapshot")
            command, files = _prepare_read_table_from_yt_command(
                yt_client,
                src,
                "<lenval=true>yamr",
                tmp_dir,
                data_proxy_role,
                pack_yt_wrapper=pack_yt_wrapper,
                pack_yson_bindings=pack_yson_bindings)

            write_errors_script = None
            if table_for_errors is not None:
                write_errors_script = write_errors_script_template.format(
                    yt_client.config["proxy"]["url"],
                    table_for_errors)

            spec = deepcopy(get_value(copy_spec_template, {}))
            spec.setdefault("secure_vault", {})
            spec["secure_vault"]["TOKEN"] = yt_client.config["token"]

            _copy_to_kiwi(
                kiwi_client,
                kiwi_transmittor,
                src,
                read_command=command,
                ranges=ranges,
                files=files,
                write_errors_script=write_errors_script,
                copy_spec_template=spec,
                **kwargs)

    finally:
        shutil.rmtree(tmp_dir)

def copy_yamr_to_kiwi():
    pass

def copy_hive_to_yt(hive_client, yt_client, source_table, destination_table, copy_spec_template=None,
                    postprocess_spec_template=None, compression_codec=None, erasure_codec=None,
                    json_format_attributes=None):
    source = source_table.split(".", 1)
    read_config, files = hive_client.get_table_config_and_files(*source)
    read_command = hive_client.get_read_command(read_config)

    _force_parent_dir(os.path.dirname(destination_table), client=yt_client)

    json_format_attributes = get_value(json_format_attributes, {})

    with yt_client.Transaction(attributes={"title": "copy_hive_to_yt"}):
        set_codec(yt_client, destination_table, compression_codec, "compression")

        temp_table = yt_client.create_temp_table()
        yt_client.write_table(temp_table, [{"file": file} for file in files], raw=False, format=yt.JsonFormat())

        spec = deepcopy(get_value(copy_spec_template, {}))
        spec["data_size_per_job"] = 1

        _set_tmpfs_settings(spec=spec, files=[hive_client.hive_importer_library],
                            client=yt_client, reserved_size=6 * GB)
        _set_mapper_settings_for_read_from_yt(destination_client=yt_client, spec=spec)

        output_format_attributes = update({"encode_utf8": "false"}, json_format_attributes)
        yt_client.run_map(
            read_command,
            temp_table,
            destination_table,
            input_format=yt.SchemafulDsvFormat(columns=["file"]),
            output_format=yt.JsonFormat(attributes=output_format_attributes),
            files=hive_client.hive_importer_library,
            spec=spec,
            ordered=True)

        postprocess_spec = deepcopy(get_value(postprocess_spec_template, {}))
        transform(destination_table, erasure_codec=erasure_codec, yt_client=yt_client,
                  spec=postprocess_spec, check_codecs=True)

def copy_hadoop_to_hadoop_with_airflow(task_type, airflow_client, source_path, source_cluster,
                                       destination_path, destination_cluster, user):
    task_id = airflow_client.add_task(
        task_type=task_type,
        source_cluster=source_cluster,
        source_path=source_path,
        destination_cluster=destination_cluster,
        destination_path=destination_path,
        owner=user)

    while True:
        task_info = airflow_client.get_task_info(task_id)
        state = task_info.get("state")
        if not airflow_client.is_task_finished(state):
            time.sleep(3.0)
            continue

        if airflow_client.is_task_unsuccessfully_finished(state):
            error = yt.YtError("Copy task unsuccessfully finished with status: " + state)
            error.attributes["details"] = airflow_client.get_task_log(task_id)
            raise error
        else:
            break
