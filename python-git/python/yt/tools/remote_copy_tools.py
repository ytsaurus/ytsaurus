from yt.wrapper.common import generate_uuid, bool_to_string
from yt.tools.conversion_tools import convert_to_erasure
import yt.yson as yson
import yt.logger as logger
import yt.wrapper as yt

import os
import json
import shutil
import tempfile
import tarfile
from copy import deepcopy

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
        for root, dirs, files in os.walk(module_path):
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
    split_row_count = max(1, (row_count * split_size) / total_size)
    return [(i * split_row_count, min((i + 1) * split_row_count, row_count)) for i in xrange(1 + ((row_count - 1) / split_row_count))]

def _split_rows_yt(yt_client, table, split_size):
    row_count = yt_client.get(table + "/@row_count")
    data_size = yt_client.get(table + "/@uncompressed_data_size")
    return _split_rows(row_count, split_size, data_size)

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
            "create_transaction_and_take_snapshot_lock": True
        }
    }
    command = """PATH=".:$PATH" PYTHONPATH=. """\
              """yt2 read "{0}"'[#'"${{start}}"':#'"${{end}}"']' --format '{1}' --proxy {2} --config '{3}' --tx {4}"""\
              .format(src, format, yt_client.config["proxy"]["url"], yson.dumps(config), yt_client.TRANSACTION)

    return command

def _prepare_read_from_yt_command(yt_client, src, format, tmp_dir, fastbone, pack=False):
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

        if format == "yson":
            files.append(_pack_module("yt_yson_bindings", tmp_dir))
            prepare_command += """
set -e
tar xvf yt_yson_bindings.tar >/dev/null
set +e"""


    read_command = _get_read_from_yt_command(yt_client, src, format, fastbone)
    files.append(_pack_string("read_from_yt.sh", _get_read_ranges_command(prepare_command, read_command), tmp_dir))

    return files

def check_permission(client, permission, path):
     user_name = client.get_user_name(client.config["token"])
     permission = client.check_permission(user_name, permission, path)
     return permission["action"] == "allow"

def run_operation_and_notify(message_queue, run_operation, *args, **kwargs):
    operation = run_operation(*args, sync=False, **kwargs)
    if message_queue is not None:
        message_queue.put({"type": "operation_started",
                           "operation": {
                               "id": operation.id,
                               "cluster_name": operation.client._name
                            }})
    operation.wait()

def copy_user_attributes(source_client, destination_client, source_table, destination_table):
    source_attributes = source_client.get(source_table + "/@")

    for attribute in source_attributes.get("user_attribute_keys", []):
        destination_client.set_attribute(destination_table, attribute, source_attributes[attribute])

def run_erasure_merge(source_client, destination_client, source_table, destination_table):
    compression_codec = source_client.get_attribute(source_table, "compression_codec")
    erasure_codec = source_client.get_attribute(source_table, "erasure_codec")

    convert_to_erasure(destination_table, yt_client=destination_client, erasure_codec=erasure_codec,
                       compression_codec=compression_codec)

def copy_yt_to_yt(source_client, destination_client, src, dst, network_name, spec_template=None, message_queue=None):
    if spec_template is None:
        spec_template = {}

    if check_permission(source_client, "write", src):
        try:
            merge_spec = deepcopy(spec_template)
            merge_spec["combine_chunks"] = bool_to_string(True)
            run_operation_and_notify(
                message_queue,
                source_client.run_merge,
                src,
                src,
                spec=merge_spec)
        except yt.YtError as error:
            raise yt.YtError("Failed to merge source table", inner_errors=[error])

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)
    with destination_client.Transaction():
        run_operation_and_notify(
            message_queue,
            destination_client.run_remote_copy,
            src,
            dst,
            cluster_name=source_client._name,
            network_name=network_name,
            spec=spec_template,
            remote_cluster_token=source_client.config["token"])

        run_erasure_merge(source_client, destination_client, src, dst)
        copy_user_attributes(source_client, destination_client, src, dst)

def copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, fastbone, spec_template=None, message_queue=None):
    if spec_template is None:
        spec_template = {}

    tmp_dir = tempfile.mkdtemp()

    destination_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)
    try:
        with source_client.Transaction(), destination_client.Transaction():
            source_client.lock(src, mode="snapshot")
            files = _prepare_read_from_yt_command(source_client, src, "json", tmp_dir, fastbone, pack=True)

            sorted_by = None
            if source_client.exists(src + "/@sorted_by"):
                sorted_by = source_client.get(src + "/@sorted_by")
            row_count = source_client.get(src + "/@row_count")

            ranges = _split_rows_yt(source_client, src, 1024 * yt.common.MB)
            temp_table = destination_client.create_temp_table(prefix=os.path.basename(src))
            destination_client.write_table(temp_table, (json.dumps({"start": start, "end": end}) for start, end in ranges), format=yt.JsonFormat())

            spec = deepcopy(spec_template)
            spec["data_size_per_job"] = 1
            spec["mapper"] = {"memory_limit": 2 * 1024 * 1024 * 1024}
            spec["job_io"] = {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}

            run_operation_and_notify(
                message_queue,
                destination_client.run_map,
                "bash read_from_yt.sh",
                temp_table,
                dst,
                files=files,
                spec=spec,
                input_format=yt.SchemafulDsvFormat(columns=["start", "end"]),
                output_format=yt.JsonFormat())

            result_row_count = destination_client.records_count(dst)
            if row_count != result_row_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if sorted_by:
                logger.info("Sorting '%s'", dst)
                run_operation_and_notify(
                    message_queue,
                    destination_client.run_sort,
                    dst,
                    sort_by=sorted_by)

            run_erasure_merge(source_client, destination_client, src, dst)
            copy_user_attributes(source_client, destination_client, src, dst)

    finally:
        shutil.rmtree(tmp_dir)


def copy_yamr_to_yt_pull(yamr_client, yt_client, src, dst, fastbone, spec_template=None, sort_spec_template=None, compression_codec=None, erasure_codec=None, force_sort=False, message_queue=None):
    proxies = yamr_client.proxies
    if not proxies:
        proxies = [yamr_client.server]

    record_count = yamr_client.records_count(src, allow_cache=True)
    sorted = yamr_client.is_sorted(src, allow_cache=True)

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, sorted)

    yt_client.mkdir(os.path.dirname(dst), recursive=True)
    if compression_codec is not None:
        if not yt_client.exists(dst):
            yt_client.create_table(dst, attributes={"compression_codec": compression_codec})
        yt_client.set(dst + "/@compression_codec", compression_codec)

    transaction_id = None
    if yamr_client.supports_read_snapshots:
        transaction_id = yamr_client.make_read_snapshot(src)
    else:
        temp_yamr_table = "tmp/yt/" + generate_uuid()
        yamr_client.copy(src, temp_yamr_table)
        src = temp_yamr_table

    ranges = _split_rows(record_count, 1024 * yt.common.MB, yamr_client.data_size(src))
    read_commands = yamr_client.create_read_range_commands(ranges, src, fastbone=fastbone, transaction_id=transaction_id)
    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table, read_commands, format=yt.SchemafulDsvFormat(columns=["command"]))

    job_io_config = {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}

    if spec_template is None:
        spec_template = {}
    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 1
    spec["job_io"] = job_io_config

    if sort_spec_template is None:
        sort_spec = deepcopy(spec)
        del sort_spec["pool"]
    else:
        sort_spec = sort_spec_template
    sort_spec["sort_job_io"] = job_io_config
    sort_spec["merge_job_io"] = job_io_config

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

    yt_client.create("map_node", os.path.dirname(dst), recursive=True, ignore_existing=True)

    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        with yt_client.Transaction():
            run_operation_and_notify(
                message_queue,
                yt_client.run_map,
                command,
                temp_table,
                dst,
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

            if (sorted and force_sort is None) or force_sort:
                logger.info("Sorting '%s'", dst)
                run_operation_and_notify(
                    message_queue,
                    yt_client.run_sort,
                    dst,
                    sort_by=["key", "subkey"],
                    spec=sort_spec)

            convert_to_erasure(dst, erasure_codec=erasure_codec, yt_client=yt_client)

    finally:
        if not yamr_client.supports_read_snapshots:
            yamr_client.drop(temp_yamr_table)

def copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, job_count=None, force_sort=None, fastbone=True, message_queue=None):
    if job_count is None:
        job_count = 200

    tmp_dir = tempfile.mkdtemp()

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
            yt_client.lock(src, mode="snapshot")

            is_sorted = yt_client.exists(src + "/@sorted_by")

            row_count = yt_client.get(src + "/@row_count")

            ranges = _split_rows_yt(yt_client, src, 1024 * yt.common.MB)

            temp_yamr_table = "tmp/yt/" + generate_uuid()
            yamr_client.write(temp_yamr_table,
                              "".join(["\t".join(map(str, range)) + "\n" for range in ranges]))

            files = _prepare_read_from_yt_command(yt_client, src, "<has_subkey=true;lenval=true>yamr", tmp_dir, fastbone, pack=True)
            files.append(lenval_to_nums_file)

            command = "python lenval_to_nums.py | bash read_from_yt.sh"

            yamr_client.run_map(command, temp_yamr_table, dst, files=files,
                                opts="-subkey -lenval -jobcount {0} -opt cpu.intensive.mode=1".format(job_count))

            result_row_count = yamr_client.records_count(dst)
            if row_count != result_row_count:
                yamr_client.drop(dst)
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise IncorrectRowCount(error)

            if (is_sorted and force_sort is None) or force_sort:
                yamr_client.run_sort(dst, dst)

    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_yamr_push(yt_client, yamr_client, src, dst, fastbone, spec_template=None, message_queue=None):
    if not yamr_client.is_empty(dst):
        yamr_client.drop(dst)

    record_count = yt_client.records_count(src)

    if spec_template is None:
        spec_template = {}
    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 2 * 1024 * yt.common.MB

    write_command = yamr_client.get_write_command(dst, fastbone=fastbone)
    logger.info("Running map '%s'", write_command)

    run_operation_and_notify(
        message_queue,
        yt_client.run_map,
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

def _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command, ranges, kiwi_user, files=None, spec_template=None, write_to_table=False, protobin=True, kwworm_options=None, message_queue=None):
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

    if spec_template is None:
        spec_template = {}
    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 1
    spec["locality_timeout"] = 0
    spec["pool"] = "transfer_kiwi"
    spec["mapper"] = {"cpu_limit": 0}
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

    tmp_dir = tempfile.mkdtemp()
    if files is None:
        files = []
    command_script = "set -o pipefail; {0} | python extract_value.py {1}".format(read_command, write_command)
    files.append(_pack_string("command.sh", command_script, tmp_dir))
    files.append(_pack_string("extract_value.py", extract_value_script, tmp_dir))
    files.append(kiwi_client.kwworm_binary)

    try:
        run_operation_and_notify(
            message_queue,
            kiwi_transmittor.run_map,
            "bash -ux command.sh",
            range_table,
            output_table,
            files=files,
            input_format=yt.YamrFormat(lenval=False,has_subkey=False),
            output_format=output_format,
            memory_limit=1280 * yt.common.MB,
            spec=spec)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_kiwi(yt_client, kiwi_client, kiwi_transmittor, src, **kwargs):
    ranges = _split_rows_yt(yt_client, src, 512 * yt.common.MB)
    fastbone = kwargs.get("fastbone", True)
    if "fastbone" in kwargs:
        del kwargs["fastbone"]

    tmp_dir = tempfile.mkdtemp()
    try:
        with yt_client.Transaction():
            yt_client.lock(src, mode="snapshot")
            files = _prepare_read_from_yt_command(yt_client, src, "<lenval=true>yamr", tmp_dir, fastbone, pack=True)
            _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command="bash read_from_yt.sh", ranges=ranges, files=files, **kwargs)
    finally:
        shutil.rmtree(tmp_dir)

def copy_yamr_to_kiwi():
    pass

def copy_hive_to_yt(hive_client, yt_client, source_table, destination_table, spec_template=None, message_queue=None):
    if spec_template is None:
        spec_template = {}

    source = source_table.split(".", 1)
    read_config, files = hive_client.get_table_config_and_files(*source)
    read_command = hive_client.get_read_command(read_config)

    temp_table = yt_client.create_temp_table()
    yt_client.write_table(temp_table, [{"file": file} for file in files], raw=False, format=yt.JsonFormat())

    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 1
    spec["mapper"] = {"memory_limit": 2 * 1024 * 1024 * 1024}
    run_operation_and_notify(
        message_queue,
        yt_client.run_map,
        read_command,
        temp_table,
        destination_table,
        input_format=yt.SchemafulDsvFormat(columns=["file"]),
        output_format=yt.JsonFormat(attributes={"encode_utf8": "false"}),
        files=hive_client.hive_importer_library,
        spec=spec)


