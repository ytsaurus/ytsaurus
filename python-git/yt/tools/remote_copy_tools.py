from yamr import _check_output

from yt.wrapper.common import generate_uuid, bool_to_string, MB
from yt.tools.conversion_tools import transform
from yt.common import get_value
import yt.yson as yson
import yt.json as json
import yt.logger as logger
import yt.wrapper as yt

import os
import time
import shutil
import gzip
import tempfile
import tarfile
from copy import deepcopy

# NB: We have no ability to discover maximum number of jobs in map operation in cluster.
# So we suppose that it is 100000 like on most clusters.
DEFAULT_MAXIMUM_YT_JOB_COUNT = 100000

class IncorrectRowCount(yt.YtError):
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
        command = "./kwworm " + " ".join(map(self._option_to_str, self._join_options(self.kwworm_options, kwworm_options)))
        return command.format(kiwi_url=self.url, kiwi_user=kiwi_user)


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

def _pack_token(token, output_dir):
    if token is None:
        raise yt.YtError("Token is required to perform copy")

    filename = os.path.join(output_dir, "yt_token")
    with open(filename, "wb") as fout:
        # XXX(asaitgalin): using GzipFile here with fileobj parameter
        # to exclude filename from resulting gzip file contents.
        with gzip.GzipFile("", fileobj=fout) as gzip_fout:
            gzip_fout.write(token)

    return filename

def _secure_upload_token(token, destination_client, token_storage_path, tmp_dir):
    user = destination_client.get_user_name()
    yt_token_file = destination_client.find_free_subpath(yt.ypath_join(token_storage_path, user))
    destination_client.create("file", yt_token_file, attributes={
        "acl": [{
            "action": "allow",
            "subjects": [user, "admins"],
            "permissions": ["read", "write", "remove"]
        }],
        "inherit_acl": False
    })
    with open(_pack_token(token, tmp_dir), "rb") as f:
        destination_client.write_file(yt_token_file, f)
    return yson.to_yson_type(yt_token_file, attributes={"file_name": "yt_token"})

def _split(item_count, split_size, total_size):
    if item_count == 0:
        return []
    split_item_count = max(1, (item_count * split_size) / total_size)
    return [(i * split_item_count, min((i + 1) * split_item_count, item_count))
            for i in xrange(1 + ((item_count - 1) / split_item_count))]

def _estimate_split_size(total_size):
    # XXX(asaitgalin): if table is bigger than 100 TiB and sorted it is not ok to
    # use default 1 GB split_size because job count will be greater than
    # DEFAULT_MAXIMUM_YT_JOB_COUNT and two ranges will be processed by the same job
    # which may violate destination table sort order.
    split_size = 1024 * yt.common.MB
    ranges_count = total_size / split_size
    if ranges_count > DEFAULT_MAXIMUM_YT_JOB_COUNT:
        scale_factor = 1.01 * ranges_count / DEFAULT_MAXIMUM_YT_JOB_COUNT
        return int(scale_factor * split_size)
    return split_size

def _slice_yamr_table_evenly(client, table, split_size, record_count=None, size=None):
    """ Return list with row ranges. """
    if _which("mr_slicetable") is None:
        if record_count is None:
            record_count = client.records_count(table, allow_cache=True)
        if size is None:
            size = client.data_size(table)
        return _split(record_count, split_size, size)
    else:
        data = _check_output(["mr_slicetable", "--server", client.server, "--table", table, "--desired-size", str(split_size)])
        return [map(int, line.split()) for line in data.split("\n") if line.strip()]

def _slice_yt_table_evenly(client, table, split_size=None):
    """ Return list with correct YT ranges. """
    # TODO(ignat): Implement fetch command in driver and slice it by chunks.
    chunk_count = client.get(table + "/@chunk_count")
    data_size = client.get(table + "/@uncompressed_data_size")
    split_size = get_value(split_size, _estimate_split_size(data_size))
    return [{"lower_limit": {"chunk_index": start}, "upper_limit": {"chunk_index": end}}
            for start, end in _split(chunk_count, split_size, data_size)]

def _set_mapper_settings_for_read_from_yt(spec):
    if "mapper" not in spec:
        spec["mapper"] = {}
    # NB: yt2 read usually consumpt less than 600 Mb, but sometimes can use more than 1Gb of memory.
    spec["mapper"]["memory_limit"] = 4 * 1024 * 1024 * 1024
    spec["mapper"]["memory_reserve_factor"] = 0.2

def shellquote(s):
    return "'" + s.replace("'", "'\\''") + "'"

def _prepare_read_from_yt_command(yt_client, src, format, tmp_dir, fastbone, pack=False, enable_row_count_check=True,
                                  input_type="json", token_file=None):
    package_files = []
    if pack:
        package_files += [
            _pack_module("yt", tmp_dir),
            _pack_module("yt_yson_bindings", tmp_dir),
        ]

    assert yt_client.TRANSACTION is not None
    config = {
        "proxy": {
            "proxy_discovery_url": ("hosts/fb" if fastbone else "hosts")
        },
        "read_retries": {
            "enable": True,
            "create_transaction_and_take_snapshot_lock": False
        },
        # To use latest version of API.
        "default_api_version_for_http": None
    }

    if len(yt.TablePath(src, client=yt_client).attributes.get("ranges", [])) > 1:
        raise yt.YtError("Reading slices from table with multiple ranges is not supported")

    command = "python read_from_yt.py --proxy {proxy} --format {format} --table {table} --input-type {input_type}"\
        .format(proxy=yt_client.config["proxy"]["url"],
                format=shellquote(format),
                table=shellquote(src),
                input_type=input_type,
                tx=yt_client.TRANSACTION)

    if yt_client.TRANSACTION is not None:
        command += " --tx " + yt_client.TRANSACTION

    for file in package_files:
        command += " --package-file " + os.path.basename(file)

    files = package_files + [os.path.abspath(os.path.join(os.path.dirname(__file__), "read_from_yt.py"))]

    if token_file is not None:
        command += " --token-file " + token_file

    if config:
        config_file = _pack_string("config.json", json.dumps(config, indent=2), tmp_dir)
        files.append(config_file)
        command += " --config-file " + os.path.basename(config_file)

    return command, files

def check_permission(client, permission, path):
     user_name = client.get_user_name(client.config["token"])
     permission = client.check_permission(user_name, permission, path)
     return permission["action"] == "allow"

def copy_user_attributes(source_client, destination_client, source_table, destination_table):
    source_attributes = source_client.get(source_table + "/@")

    for attribute in source_attributes.get("user_attribute_keys", []):
        destination_client.set_attribute(destination_table, attribute, source_attributes[attribute])

def set_compression_codec(yt_client, dst, compression_codec):
    if compression_codec is not None:
        if not yt_client.exists(dst):
            yt_client.create_table(dst, attributes={"compression_codec": compression_codec})
        yt_client.set(dst + "/@compression_codec", compression_codec)

def copy_yt_to_yt(source_client, destination_client, src, dst, network_name,
                  copy_spec_template=None, postprocess_spec_template=None,
                  compression_codec=None, erasure_codec=None):
    copy_spec_template = get_value(copy_spec_template, {})

    src = yt.TablePath(src, client=source_client)
    compressed_data_size = source_client.get_attribute(src, "compressed_data_size")
    chunk_count = source_client.get_attribute(src, "chunk_count")

    if chunk_count > 100 and compressed_data_size / chunk_count < 10 * MB and not src.has_delimiters():
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

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)

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

        destination_client.run_remote_copy(
            src,
            dst,
            copy_attributes=True,
            network_name=network_name,
            spec=copy_spec_template,
            **kwargs)

        for name, codec in [("compression_codec", compression_codec), ("erasure_codec", erasure_codec)]:
            if codec is None:
                destination_client.set(dst + "/@" + name, source_client.get(str(src) + "/@" + name))

        if erasure_codec is not None or compression_codec is not None:
            transform(str(dst), compression_codec=compression_codec, erasure_codec=erasure_codec,
                      yt_client=destination_client, spec=postprocess_spec_template, check_codecs=True)

def copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, fastbone, token_storage_path,
                                copy_spec_template=None, postprocess_spec_template=None, default_tmp_dir=None,
                                compression_codec=None, erasure_codec=None, intermediate_format=None,
                                enable_row_count_check=None):
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)

    intermediate_format = yt.create_format(get_value(intermediate_format, "json"))
    enable_row_count_check = get_value(enable_row_count_check, True)

    attributes = {"title": "copy_yt_to_yt_through_proxy"}

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)
    destination_client.create("map_node", token_storage_path, recursive=True, ignore_existing=True)
    try:
        with source_client.Transaction(attributes=attributes), destination_client.Transaction(attributes=attributes):
            # NB: for reliable access to table under snapshot lock we should use id.
            src = yt.TablePath(src, client=source_client)
            src.name = yson.to_yson_type("#" + source_client.get(src.name + "/@id"), attributes=src.attributes)
            source_client.lock(src, mode="snapshot")

            if compression_codec is None:
                compression_codec = source_client.get(str(src) + "/@compression_codec")
            if erasure_codec is None:
                erasure_codec = source_client.get(str(src) + "/@erasure_codec")
            set_compression_codec(destination_client, dst, compression_codec)

            ranges = _slice_yt_table_evenly(source_client, src.name)

            sorted_by = None
            dst_table = dst
            if source_client.exists(src.name + "/@sorted_by"):
                sorted_by = source_client.get(src.name + "/@sorted_by")
                dst_table = yt.TablePath(dst, client=source_client)
                dst_table.attributes["sorted_by"] = sorted_by
            row_count = source_client.get(src.name + "/@row_count")

            temp_table = destination_client.create_temp_table(prefix=os.path.basename(src.name))
            destination_client.write_table(temp_table, ranges, format=yt.JsonFormat(), raw=False)

            spec = deepcopy(get_value(copy_spec_template, {}))
            spec["data_size_per_job"] = 1
            _set_mapper_settings_for_read_from_yt(spec)
            spec["job_io"] = {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}

            yt_token_file = _secure_upload_token(source_client.config["token"], destination_client,
                                                 token_storage_path, tmp_dir)
            try:
                command, files = _prepare_read_from_yt_command(
                    source_client,
                    src.to_yson_string(),
                    str(intermediate_format),
                    tmp_dir,
                    fastbone,
                    pack=True,
                    enable_row_count_check=enable_row_count_check,
                    token_file=yt_token_file.attributes["file_name"])

                destination_client.run_map(
                    command,
                    temp_table,
                    dst_table,
                    files=files,
                    yt_files=[yt_token_file],
                    spec=spec,
                    input_format=yt.JsonFormat(),
                    output_format=intermediate_format)
            finally:
                destination_client.remove(str(yt_token_file), force=True)

            result_row_count = destination_client.row_count(dst)
            if not src.has_delimiters() and row_count != result_row_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            transform(str(dst_table), erasure_codec=erasure_codec, yt_client=destination_client, spec=postprocess_spec_template, check_codecs=True)
            copy_user_attributes(source_client, destination_client, src.name, dst)

    finally:
        shutil.rmtree(tmp_dir)


def copy_yamr_to_yt_pull(yamr_client, yt_client, src, dst, fastbone, copy_spec_template=None, postprocess_spec_template=None, compression_codec=None, erasure_codec=None, force_sort=None, job_timeout=None):
    proxies = yamr_client.proxies
    if not proxies:
        proxies = [yamr_client.server]

    if job_timeout is None:
        job_timeout = 1800

    transaction_id = None
    if yamr_client.supports_read_snapshots:
        transaction_id = yamr_client.make_read_snapshot(src)
    else:
        temp_yamr_table = "tmp/yt/" + generate_uuid()
        yamr_client.copy(src, temp_yamr_table)
        src = temp_yamr_table

    # NB: Yamr does not support -list under transaction.
    # It means that record count may be inconsistent with locked version of table.
    record_count = yamr_client.records_count(src, allow_cache=True)
    is_sorted = yamr_client.is_sorted(src, allow_cache=True)
    if compression_codec is None:
        yamr_codec_to_yt_codec = {"zlib": "zlib6", "quicklz": "quick_lz", "lzma": "zlib9"}
        codec = yamr_client.get_compression_codec(src, allow_cache=True)
        compression_codec = yamr_codec_to_yt_codec.get(codec)

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, is_sorted)

    data_size = yamr_client.data_size(src)
    #ranges = _split_rows(record_count, _estimate_split_size(data_size), data_size)
    ranges = _slice_yamr_table_evenly(yamr_client, src, _estimate_split_size(data_size), record_count, data_size)

    read_commands = yamr_client.create_read_range_commands(ranges, src, fastbone=fastbone, transaction_id=transaction_id, enable_logging=True, timeout=job_timeout)
    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table, read_commands, format=yt.SchemafulDsvFormat(columns=["command"]), raw=True)

    job_io_config = {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}

    spec = deepcopy(get_value(copy_spec_template, {}))
    spec["data_size_per_job"] = 1
    spec["job_io"] = job_io_config

    postprocess_spec = deepcopy(get_value(postprocess_spec_template, {}))
    postprocess_spec["sort_job_io"] = job_io_config
    postprocess_spec["merge_job_io"] = job_io_config

    command = """\
set -ux
while true; do
    IFS="\t" read -r command;
    if [ "$?" != "0" ]; then
        break;
    fi
    set -e;
    bash -c "${command}";
    set +e;
done"""

    # We need to create it outside of transaction to avoid races.
    yt_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)

    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        with yt_client.Transaction(attributes={"title": "copy_yamr_to_yt"}):
            set_compression_codec(yt_client, dst, compression_codec)

            if is_sorted:
                dst_path = yt.TablePath(dst, client=yt_client)
                dst_path.attributes["sorted_by"] = ["key", "subkey"]
            else:
                dst_path = dst

            yt_client.run_map(
                command,
                temp_table,
                dst_path,
                input_format=yt.SchemafulDsvFormat(columns=["command"]),
                output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                files=yamr_client.binary,
                memory_limit = 2500 * yt.common.MB,
                spec=spec)

            result_record_count = yt_client.row_count(dst)
            if result_record_count != record_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if force_sort and not is_sorted:
                yt_client.run_sort(dst, sort_by=["key", "subkey"], spec=postprocess_spec)

            transform(str(dst), erasure_codec=erasure_codec, yt_client=yt_client, spec=postprocess_spec, check_codecs=True)

    finally:
        if not yamr_client.supports_read_snapshots:
            yamr_client.drop(temp_yamr_table)

def copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, parallel_job_count=None, force_sort=None, fastbone=True, default_tmp_dir=None, enable_row_count_check=None):
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)

    enable_row_count_check = get_value(enable_row_count_check, True)

    try:
        with yt_client.Transaction(attributes={"title": "copy_yt_to_yamr"}):
            # NB: for reliable access to table under snapshot lock we should use id.
            src = yt.TablePath(src, client=yt_client)
            src.name = yson.to_yson_type("#" + yt_client.get(src.name + "/@id"), attributes=src.attributes)
            yt_client.lock(src, mode="snapshot")

            is_sorted = yt_client.exists(src.name + "/@sorted_by")

            row_count = yt_client.get(src.name + "/@row_count")

            ranges = _slice_yt_table_evenly(yt_client, src.name, 1024 * yt.common.MB)

            temp_yamr_table = "tmp/yt/" + generate_uuid()
            yamr_client.write_rows_as_values(temp_yamr_table, ranges)

            token_file = _pack_token(yt_client.config["token"], tmp_dir)
            command, files = _prepare_read_from_yt_command(
                yt_client,
                src.to_yson_string(),
                "<has_subkey=true;lenval=true>yamr",
                tmp_dir,
                fastbone,
                pack=True,
                enable_row_count_check=enable_row_count_check,
                input_type="lenval",
                token_file=os.path.basename(token_file))
            files.append(token_file)

            if parallel_job_count is None:
                parallel_job_count = 200
            job_count = len(ranges)

            yamr_client.run_map(command, temp_yamr_table, dst, files=files,
                                opts="-subkey -lenval -jobcount {0} -opt jobcount.throttle={1} -opt cpu.intensive.mode=1"\
                                     .format(job_count, parallel_job_count))

            result_row_count = yamr_client.records_count(dst)
            if not src.has_delimiters() and row_count != result_row_count:
                yamr_client.drop(dst)
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if (is_sorted and force_sort is None) or force_sort:
                yamr_client.run_sort(dst, dst)

    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_yamr_push(yt_client, yamr_client, src, dst, fastbone, copy_spec_template=None):
    if not yamr_client.is_empty(dst):
        yamr_client.drop(dst)

    record_count = yt_client.row_count(src)

    spec = deepcopy(get_value(copy_spec_template, {}))
    spec["data_size_per_job"] = 2 * 1024 * yt.common.MB

    write_command = yamr_client.get_write_command(dst, fastbone=fastbone)
    logger.info("Running map '%s'", write_command)

    yt_client.run_map(
        write_command,
        src,
        yt_client.create_temp_table(),
        files=yamr_client.binary,
        format=yt.YamrFormat(has_subkey=True, lenval=True),
        memory_limit=2000 * yt.common.MB,
        spec=spec)

    result_record_count = yamr_client.records_count(dst)
    if record_count != result_record_count:
        yamr_client.drop(dst)
        error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
        logger.error(error)
        raise IncorrectRowCount(error)

def _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command, ranges, kiwi_user, files=None,
                  yt_files=None, copy_spec_template=None, write_to_table=False, protobin=True,
                  kwworm_options=None, default_tmp_dir=None):
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
    spec["data_size_per_job"] = 1
    spec["locality_timeout"] = 0
    spec["pool"] = "transfer_kiwi"
    # After investigation of several copy jobs we have found the following:
    # 1) kwworm memory consumption is in range (200-500MB)
    # 2) yt2 read consumption is in range (200-600MB)
    # 3) In some corner cases yt2 can use above than 1GB.
    # 4) Other processes uses less than 100 MB.
    spec["mapper"] = {
        "cpu_limit": 0,
        "memory_limit": 4 * 1024 * 1024 * 1024,
        "memory_reserve_factor": 0.35}
    _set_mapper_settings_for_read_from_yt(spec)
    if "max_failed_job_count" not in spec:
        spec["max_failed_job_count"] = 1000

    if write_to_table:
        extract_value_script = extract_value_script_to_table
        write_command = ""
        output_format = yt.YamrFormat(lenval=True,has_subkey=False)
    else:
        extract_value_script = extract_value_script_to_worm
        kiwi_command = kiwi_client.get_write_command(kiwi_user=kiwi_user, kwworm_options=kwworm_options)
        write_command = "| {0} >output 2>&1; RESULT=$?; cat output; tail -n 100 output >&2; exit $RESULT".format(kiwi_command)
        output_format = yt.SchemafulDsvFormat(columns=["error"])

    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)
    if files is None:
        files = []
    command_script = "set -o pipefail; {0} | python extract_value.py {1}".format(read_command, write_command)
    files.append(_pack_string("command.sh", command_script, tmp_dir))
    files.append(_pack_string("extract_value.py", extract_value_script, tmp_dir))
    files.append(kiwi_client.kwworm_binary)

    try:
        kiwi_transmittor.run_map(
            "bash -ux command.sh",
            range_table,
            output_table,
            files=files,
            yt_files=yt_files,
            input_format=yt.JsonFormat(),
            output_format=output_format,
            spec=spec)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_kiwi(yt_client, kiwi_client, kiwi_transmittor, src, token_storage_path, **kwargs):
    ranges = _slice_yt_table_evenly(yt_client, src, 512 * yt.common.MB)
    fastbone = kwargs.get("fastbone", True)
    if "fastbone" in kwargs:
        del kwargs["fastbone"]

    tmp_dir = tempfile.mkdtemp(dir=kwargs.get("tmp_dir"))
    enable_row_count_check = get_value(kwargs.pop("enable_row_count_check", None), False)
    yt_client.create("map_node", token_storage_path, ignore_existing=True, recursive=True)
    try:
        with yt_client.Transaction(attributes={"title": "copy_yt_to_kiwi"}):
            yt_client.lock(src, mode="snapshot")

            yt_token_file = _secure_upload_token(yt_client.config["token"], kiwi_transmittor,
                                                 token_storage_path, tmp_dir)
            try:
                command, files = _prepare_read_from_yt_command(
                    yt_client,
                    src,
                    "<lenval=true>yamr",
                    tmp_dir,
                    fastbone,
                    pack=True,
                    enable_row_count_check=enable_row_count_check,
                    token_file=yt_token_file.attributes["file_name"])

                _copy_to_kiwi(
                    kiwi_client,
                    kiwi_transmittor,
                    src,
                    read_command=command,
                    ranges=ranges,
                    files=files,
                    yt_files=[yt_token_file],
                    **kwargs)
            finally:
                kiwi_transmittor.remove(str(yt_token_file), force=True)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yamr_to_kiwi():
    pass

def copy_hive_to_yt(hive_client, yt_client, source_table, destination_table, copy_spec_template=None):
    source = source_table.split(".", 1)
    read_config, files = hive_client.get_table_config_and_files(*source)
    read_command = hive_client.get_read_command(read_config)

    temp_table = yt_client.create_temp_table()
    yt_client.write_table(temp_table, [{"file": file} for file in files], raw=False, format=yt.JsonFormat())

    spec = deepcopy(get_value(copy_spec_template, {}))
    spec["data_size_per_job"] = 1
    _set_mapper_settings_for_read_from_yt(spec)
    yt_client.run_map(
        read_command,
        temp_table,
        destination_table,
        input_format=yt.SchemafulDsvFormat(columns=["file"]),
        output_format=yt.JsonFormat(attributes={"encode_utf8": "false"}),
        files=hive_client.hive_importer_library,
        spec=spec)

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
