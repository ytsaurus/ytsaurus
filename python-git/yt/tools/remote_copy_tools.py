from yt.wrapper.common import generate_uuid, bool_to_string, MB
from yt.tools.conversion_tools import convert_to_erasure
from yt.common import get_value
import yt.yson as yson
import yt.logger as logger
import yt.wrapper as yt

import os
import json
import time
import shutil
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

def _split_rows(row_count, split_size, total_size):
    if row_count == 0:
        return []
    split_row_count = max(1, (row_count * split_size) / total_size)
    return [(i * split_row_count, min((i + 1) * split_row_count, row_count)) for i in xrange(1 + ((row_count - 1) / split_row_count))]

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

def _split_rows_yt(yt_client, table, split_size=None):
    row_count = yt_client.get(table + "/@row_count")
    data_size = yt_client.get(table + "/@uncompressed_data_size")
    split_size = get_value(split_size, _estimate_split_size(data_size))
    return _split_rows(row_count, split_size, data_size)

def _set_mapper_settings_for_read_from_yt(spec):
    if "mapper" not in spec:
        spec["mapper"] = {}
    # NB: yt2 read usually consumpt less than 600 Mb, but sometimes can use more than 1Gb of memory.
    spec["mapper"]["memory_limit"] = 4 * 1024 * 1024 * 1024
    spec["mapper"]["memory_reserve_factor"] = 0.2

def _get_read_ranges_command(prepare_command, read_command):
    return """\
set -uxe

{0}

while true; do
    set +e
    read -r start end;
    result="$?"
    set -e
    if [ "$result" != "0" ]; then break; fi;
    {1}
done;""".format(prepare_command, read_command)

def _get_read_from_yt_command(yt_client, src, format, fastbone):
    assert yt_client.TRANSACTION is not None
    config = {
        "proxy": {
            "proxy_discovery_url":("hosts/fb" if fastbone else "hosts")
        },
        "read_retries": {
            "enable": True,
            "create_transaction_and_take_snapshot_lock": False
        },
        # To use latest version of API.
        "default_api_version_for_http": None
    }
    command = """export  PYTHONPATH=.\n"""\
              """TABLE_PATH="$(YT_PROXY={2} python merge_limits.py '{0}' ${{start}} ${{end}})"\n"""\
              """PATH=".:$PATH" yt2 read ${{TABLE_PATH}} --format '{1}' --proxy {2} --config '{3}' --tx {4}"""\
              .format(src, format, yt_client.config["proxy"]["url"], yson.dumps(config, boolean_as_string=False), yt_client.TRANSACTION)

    return command

def _prepare_read_from_yt_command(yt_client, src, format, tmp_dir, fastbone, pack=False):
    merge_limits_script = """
import yt.yson
import yt.wrapper

import sys

def set_row_index(attributes, limit_name, value, func):
    if "ranges" in attributes:
        if not attributes["ranges"]:
            attributes["ranges"].append({})
        obj = attributes["ranges"][0]
    else:
        obj = attributes

    if limit_name not in obj:
        obj[limit_name] = {}
    if "row_index" not in obj[limit_name]:
        obj[limit_name]["row_index"] = value
    else:
        obj[limit_name]["row_index"] = func(obj[limit_name]["row_index"], value)


if __name__ == "__main__":
    path, start_index, end_index = sys.argv[1:]
    start_index = int(start_index)
    end_index = int(end_index)
    parsed_path = yt.wrapper.table.TablePath(path)
    assert len(parsed_path.attributes.get("ranges", [])) <= 1
    set_row_index(parsed_path.attributes, "lower_limit", start_index, max)
    set_row_index(parsed_path.attributes, "upper_limit", end_index, min)
    print parsed_path.to_yson_string()
"""

    files = []
    prepare_command = \
        "set +x\n"\
        "export YT_TOKEN=$(cat yt_token)\n"\
        "set -x\n"
    if "token" in yt_client.config:
        files.append(_pack_string("yt_token", yt_client.config["token"], tmp_dir))
    if pack:
        files += [_pack_module("yt", tmp_dir), _which("yt2")]
        prepare_command += """
set -e
tar xvf yt.tar >/dev/null
set +e"""

        files.append(_pack_module("yt_yson_bindings", tmp_dir))
        prepare_command += """
set -e
tar xvf yt_yson_bindings.tar >/dev/null
set +e"""


    read_command = _get_read_from_yt_command(yt_client, src, format, fastbone)
    files.append(_pack_string("read_from_yt.sh", _get_read_ranges_command(prepare_command, read_command), tmp_dir))
    files.append(_pack_string("merge_limits.py", merge_limits_script, tmp_dir))

    return files

def check_permission(client, permission, path):
     user_name = client.get_user_name(client.config["token"])
     permission = client.check_permission(user_name, permission, path)
     return permission["action"] == "allow"

def copy_codec_attributes(source_client, destination_client, source_table, destination_table):
    for attribute in ["compression_codec", "erasure_codec"]:
        value = source_client.get_attribute(source_table, attribute)
        destination_client.set_attribute(destination_table, attribute, value)

def copy_user_attributes(source_client, destination_client, source_table, destination_table):
    source_attributes = source_client.get(source_table + "/@")

    for attribute in source_attributes.get("user_attribute_keys", []):
        destination_client.set_attribute(destination_table, attribute, source_attributes[attribute])

def run_erasure_merge(source_client, destination_client, source_table, destination_table, spec):
    compression_codec = source_client.get_attribute(source_table, "compression_codec")
    erasure_codec = source_client.get_attribute(source_table, "erasure_codec")

    convert_to_erasure(destination_table, yt_client=destination_client, erasure_codec=erasure_codec,
                       compression_codec=compression_codec, spec=spec)

def apply_compression_codec(yt_client, dst, compression_codec):
    if compression_codec is not None:
        if not yt_client.exists(dst):
            yt_client.create_table(dst, attributes={"compression_codec": compression_codec})
        yt_client.set(dst + "/@compression_codec", compression_codec)

def copy_yt_to_yt(source_client, destination_client, src, dst, network_name,
                  copy_spec_template=None, postprocess_spec_template=None,
                  compression_codec=None, erasure_codec=None):
    copy_spec_template = get_value(copy_spec_template, {})

    compressed_data_size = source_client.get_attribute(src, "compressed_data_size")
    chunk_count = source_client.get_attribute(src, "chunk_count")

    if chunk_count > 100 and compressed_data_size / chunk_count < 10 * MB:
        if check_permission(source_client, "write", src):
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
                             "If you can not get a write permission use 'proxy' copy method".format(src))

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)
    with destination_client.Transaction():
        apply_compression_codec(destination_client, dst, compression_codec)
        destination_client.run_remote_copy(
            src,
            dst,
            cluster_name=source_client._name,
            network_name=network_name,
            spec=copy_spec_template)

        if erasure_codec is None:
            copy_codec_attributes(source_client, destination_client, src, dst)
        else:
            convert_to_erasure(dst, erasure_codec=erasure_codec, yt_client=destination_client, spec=postprocess_spec_template)

        copy_user_attributes(source_client, destination_client, src, dst)

def copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, fastbone,
                                copy_spec_template=None, postprocess_spec_template=None, default_tmp_dir=None,
                                compression_codec=None, erasure_codec=None):
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)
    try:
        with source_client.Transaction(), destination_client.Transaction():
            apply_compression_codec(destination_client, dst, compression_codec)

            # NB: for reliable access to table under snapshot lock we should use id.
            src = yt.TablePath(src, client=source_client)
            src.name = yson.to_yson_type("#" + source_client.get(src.name + "/@id"), attributes=src.attributes)
            source_client.lock(src, mode="snapshot")
            files = _prepare_read_from_yt_command(source_client, src.to_yson_string(), "json", tmp_dir, fastbone, pack=True)

            ranges = _split_rows_yt(source_client, src.name)

            sorted_by = None
            dst_table = dst
            if source_client.exists(src.name + "/@sorted_by"):
                sorted_by = source_client.get(src.name + "/@sorted_by")
                dst_table = yt.TablePath(dst, client=source_client)
                dst_table.attributes["sorted_by"] = sorted_by
            row_count = source_client.get(src.name + "/@row_count")

            temp_table = destination_client.create_temp_table(prefix=os.path.basename(src.name))
            destination_client.write_table(temp_table, (json.dumps({"start": start, "end": end}) for start, end in ranges), format=yt.JsonFormat())

            spec = deepcopy(get_value(copy_spec_template, {}))
            spec["data_size_per_job"] = 1
            _set_mapper_settings_for_read_from_yt(spec)
            spec["job_io"] = {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}

            destination_client.run_map(
                "bash read_from_yt.sh",
                temp_table,
                dst_table,
                files=files,
                spec=spec,
                input_format=yt.SchemafulDsvFormat(columns=["start", "end"]),
                output_format=yt.JsonFormat())

            result_row_count = destination_client.records_count(dst)
            if not src.has_delimiters() and row_count != result_row_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if erasure_codec is None:
                run_erasure_merge(source_client, destination_client, src.name, dst, spec=postprocess_spec_template)
            else:
                convert_to_erasure(dst, erasure_codec=erasure_codec, yt_client=destination_client, spec=postprocess_spec_template)
            copy_user_attributes(source_client, destination_client, src.name, dst)

    finally:
        shutil.rmtree(tmp_dir)


def copy_yamr_to_yt_pull(yamr_client, yt_client, src, dst, fastbone, copy_spec_template=None, postprocess_spec_template=None, compression_codec=None, erasure_codec=None, force_sort=None, job_timeout=None):
    proxies = yamr_client.proxies
    if not proxies:
        proxies = [yamr_client.server]

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

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, is_sorted)

    data_size = yamr_client.data_size(src)
    ranges = _split_rows(record_count, _estimate_split_size(data_size), data_size)

    read_commands = yamr_client.create_read_range_commands(ranges, src, fastbone=fastbone, transaction_id=transaction_id, enable_logging=True, timeout=job_timeout)
    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table, read_commands, format=yt.SchemafulDsvFormat(columns=["command"]))

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
        with yt_client.Transaction():
            apply_compression_codec(yt_client, dst, compression_codec)

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

            result_record_count = yt_client.records_count(dst)
            if result_record_count != record_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if force_sort and not is_sorted:
                yt_client.run_sort(dst, sort_by=["key", "subkey"], spec=postprocess_spec)

            convert_to_erasure(dst, erasure_codec=erasure_codec, yt_client=yt_client, spec=postprocess_spec)

    finally:
        if not yamr_client.supports_read_snapshots:
            yamr_client.drop(temp_yamr_table)

def copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, parallel_job_count=None, force_sort=None, fastbone=True, default_tmp_dir=None):
    tmp_dir = tempfile.mkdtemp(dir=default_tmp_dir)

    lenval_to_nums_file = _pack_string(
        "lenval_to_nums.py",
"""\
import sys
import struct

count = 0

while True:
    s = sys.stdin.read(4)
    if not s:
        break
    length = struct.unpack('i', s)[0]
    sys.stdout.write(sys.stdin.read(length))
    if count % 3 == 2:
        sys.stdout.write('\\n')
    else:
        sys.stdout.write('\\t')

    count += 1""",
        tmp_dir)

    try:
        with yt_client.Transaction():
            # NB: for reliable access to table under snapshot lock we should use id.
            src = yt.TablePath(src, client=yt_client)
            src.name = yson.to_yson_type("#" + yt_client.get(src.name + "/@id"), attributes=src.attributes)
            yt_client.lock(src, mode="snapshot")

            is_sorted = yt_client.exists(src.name + "/@sorted_by")

            row_count = yt_client.get(src.name + "/@row_count")

            ranges = _split_rows_yt(yt_client, src.name, 1024 * yt.common.MB)

            temp_yamr_table = "tmp/yt/" + generate_uuid()
            yamr_client.write(temp_yamr_table,
                              "".join(["\t".join(map(str, range)) + "\n" for range in ranges]))

            files = _prepare_read_from_yt_command(yt_client, src.to_yson_string(), "<has_subkey=true;lenval=true>yamr", tmp_dir, fastbone, pack=True)
            files.append(lenval_to_nums_file)

            command = "python lenval_to_nums.py | bash read_from_yt.sh"

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

    record_count = yt_client.records_count(src)

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

def _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command, ranges, kiwi_user, files=None, copy_spec_template=None, write_to_table=False, protobin=True, kwworm_options=None, default_tmp_dir=None):
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
    kiwi_transmittor.write_table(range_table,
                                 ["\t".join(map(str, range)) + "\n" for range in ranges],
                                 format=yt.YamrFormat(lenval=False, has_subkey=False))

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
            input_format=yt.YamrFormat(lenval=False,has_subkey=False),
            output_format=output_format,
            spec=spec)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_kiwi(yt_client, kiwi_client, kiwi_transmittor, src, **kwargs):
    ranges = _split_rows_yt(yt_client, src, 512 * yt.common.MB)
    fastbone = kwargs.get("fastbone", True)
    if "fastbone" in kwargs:
        del kwargs["fastbone"]

    tmp_dir = tempfile.mkdtemp(dir=kwargs.get("tmp_dir"))
    try:
        with yt_client.Transaction():
            yt_client.lock(src, mode="snapshot")
            files = _prepare_read_from_yt_command(yt_client, src, "<lenval=true>yamr", tmp_dir, fastbone, pack=True)
            _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command="bash read_from_yt.sh", ranges=ranges, files=files, **kwargs)
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
