from yt.wrapper.common import generate_uuid
from yt.tools.convertion_tools import convert_to_erasure
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
    def __init__(self, url, kwworm):
        self.url = url
        self.kwworm = kwworm

def _which(file):
    for path in os.environ["PATH"].split(":"):
        if os.path.exists(path + "/" + file):
            return path + "/" + file
    return None

def _pack_module(module_name, output_dir):
    module = __import__(module_name)
    module_path = module.__file__.strip("__init__.py").strip("__init__.pyc")
    if module_path.endswith(".egg"):
        return module_path
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
set -ux

{0}

while true; do
    read -r start end;
    if [ "$?" != "0" ]; then break; fi;
    set -e
    {1}
    set +e
done;""".format(prepare_command, read_command)

def _get_read_from_yt_command(yt_client, src, format, fastbone):
    token = yt_client.token
    if token is None:
        token = ""

    hosts = "hosts"
    if fastbone:
        hosts = "hosts/fb"

    return """PATH=".:$PATH" PYTHONPATH=. YT_RETRY_READ=1 YT_TOKEN={0} YT_HOSTS="{1}" """\
           """yt2 read "{2}"'[#'"${{start}}"':#'"${{end}}"']' --format '{3}' --proxy {4}"""\
           .format(token, hosts, src, format, yt_client.proxy)

def _prepare_read_from_yt_command(yt_client, src, format, tmp_dir, fastbone, pack=False):
    files = []
    prepare_command = "export YT_TOKEN=$(cat yt_token)\n"
    if pack:
        files = [_pack_module("simplejson", tmp_dir), _pack_module("dateutil", tmp_dir), _pack_module("yt", tmp_dir), _which("yt2")]
        prepare_command += """
set -e
tar xvf yt.tar >/dev/null
tar xvf simplejson.tar >/dev/null
tar xvf dateutil.tar >/dev/null
set +e"""

    read_command = _get_read_from_yt_command(yt_client, src, format, fastbone)
    files.append(_pack_string("read_from_yt.sh", _get_read_ranges_command(prepare_command, read_command), tmp_dir))

    return files


class AsyncStrategy(object):
    def process_operation(self, type, operation, finalize, client=None):
        self.type = type
        self.operation_id = operation
        self.finalize = finalize
        self.client = client

    def wait(self):
        yt.WaitStrategy().process_operation(self.type, self.operation_id, self.finalize, self.client)


def run_operation_and_notify(message_queue, yt_client, run_operation):
    strategy = AsyncStrategy()
    run_operation(yt_client, strategy)
    if message_queue:
        message_queue.put({"type": "operation_started",
                           "operation": {
                               "id": strategy.operation_id,
                               "cluster_name": yt_client._name
                            }})
    strategy.wait()


def copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, fastbone, spec_template=None, message_queue=None):
    if spec_template is None:
        spec_template = {}

    tmp_dir = tempfile.mkdtemp()
    files = _prepare_read_from_yt_command(source_client, src, "yson", tmp_dir, fastbone)

    try:
        with source_client.PingableTransaction():
            sorted_by = None
            if source_client.exists(src + "/@sorted_by"):
                sorted_by = source_client.get(src + "/@sorted_by")
            row_count = source_client.get(src + "/@row_count")
            compression_codec = source_client.get(src + "/@compression_codec")
            erasure_codec = source_client.get(src + "/@erasure_codec")

            ranges = _split_rows_yt(source_client, src, 1024 * yt.common.MB)
            temp_table = destination_client.create_temp_table(prefix=os.path.basename(src))
            destination_client.write_table(temp_table, (json.dumps({"start": start, "end": end}) for start, end in ranges), format=yt.JsonFormat())

            spec = deepcopy(spec_template)
            spec["data_size_per_job"] = 1

            destination_client.create("table", dst, ignore_existing=True)
            destination_client.run_map("bash read_from_yt.sh", temp_table, dst, files=files, spec=spec,
                                       input_format=yt.SchemafulDsvFormat(columns=["start", "end"]),
                                       output_format=yt.YsonFormat())

            result_row_count = destination_client.records_count(dst)
            if row_count != result_row_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise yt.IncorrectRowCount(error)

            if sorted_by:
                logger.info("Sorting '%s'", dst)
                run_operation_and_notify(
                    message_queue,
                    destination_client,
                    lambda client, strategy: client.run_sort(dst, sort_by=sorted_by, strategy=strategy))

            convert_to_erasure(dst, yt_client=destination_client, erasure_codec=erasure_codec, compression_codec=compression_codec)

    finally:
        shutil.rmtree(tmp_dir)


def copy_yamr_to_yt_pull(yamr_client, yt_client, src, dst, fastbone, spec_template=None, message_queue=None):
    proxies = yamr_client.proxies
    if not proxies:
        proxies = [yamr_client.server]

    record_count = yamr_client.records_count(src, allow_cache=True)
    sorted = yamr_client.is_sorted(src, allow_cache=True)

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, sorted)

    yt_client.mkdir(os.path.dirname(dst), recursive=True)

    temp_yamr_table = "tmp/yt/" + generate_uuid()
    yamr_client.copy(src, temp_yamr_table)
    src = temp_yamr_table

    ranges = _split_rows(record_count, 1024 * yt.common.MB, yamr_client.data_size(src))
    read_commands = yamr_client.create_read_range_commands(ranges, src, fastbone=fastbone)
    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table, read_commands, format=yt.SchemafulDsvFormat(columns=["command"]))

    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 1

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

    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        with yt_client.PingableTransaction():
            run_operation_and_notify(
                message_queue,
                yt_client,
                lambda client, strategy:
                    client.run_map(
                        command,
                        temp_table,
                        dst,
                        input_format=yt.SchemafulDsvFormat(columns=["command"]),
                        output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                        files=yamr_client.binary,
                        memory_limit = 2500 * yt.common.MB,
                        spec=spec,
                        strategy=strategy))

            result_record_count = yt_client.records_count(dst)
            if result_record_count != record_count:
                error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
                logger.error(error)
                raise yt.IncorrectRowCount(error)

            if sorted:
                logger.info("Sorting '%s'", dst)
                run_operation_and_notify(
                    message_queue,
                    yt_client,
                    lambda client, strategy: client.run_sort(dst, sort_by=["key", "subkey"], strategy=strategy))

    finally:
        yamr_client.drop(temp_yamr_table)

def copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, job_count=None, fastbone=True, message_queue=None):
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

    files = _prepare_read_from_yt_command(yt_client, src, "<has_subkey=true;lenval=true>yamr", tmp_dir, fastbone, pack=True)
    files.append(lenval_to_nums_file)

    command = "python lenval_to_nums.py | bash read_from_yt.sh"

    try:
        with yt_client.PingableTransaction():
            yt_client.lock(src, mode="snapshot")

            row_count = yt_client.get(src + "/@row_count")

            ranges = _split_rows_yt(yt_client, src, 1024 * yt.common.MB)

            temp_yamr_table = "tmp/yt/" + generate_uuid()
            yamr_client.write(temp_yamr_table,
                              "".join(["\t".join(map(str, range)) + "\n" for range in ranges]))

            yamr_client.run_map(command, temp_yamr_table, dst, files=files,
                                opts="-subkey -lenval -jobcount {0} -opt cpu.intensive.mode=1".format(job_count))

            result_row_count = yamr_client.records_count(dst)
            if row_count != result_row_count:
                yamr_client.drop(dst)
                error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
                logger.error(error)
                raise yt.IncorrectRowCount(error)
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
        yt_client,
        lambda client, strategy:
            client.run_map(write_command, src, yt_client.create_temp_table(),
                           files=yamr_client.binary,
                           format=yt.YamrFormat(has_subkey=True, lenval=True),
                           memory_limit=2000 * yt.common.MB,
                           spec=spec,
                           strategy=strategy))

    result_record_count = yamr_client.records_count(dst)
    if record_count != result_record_count:
        yamr_client.drop(dst)
        error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
        logger.error(error)
        raise yt.IncorrectRowCount(error)

def _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command, ranges, kiwi_user=None, spec_template=None, write_to_table=False, protobin=True, message_queue=None):
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

    if kiwi_user is None:
        kiwi_user = "flux"

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
    spec["max_failed_job_count"] = 16384

    if write_to_table:
        extract_value_script = extract_value_script_to_table
        write_command = ""
        output_format = yt.YamrFormat(lenval=True,has_subkey=False)
    else:
        extract_value_script = extract_value_script_to_worm
        write_command = "| {0} -c {1} -6 -u {2} -r 10000 -f 60000 --balance fast --spread 4 --tcp-cork write -f {3} -n 2>&1"\
                        .format(kiwi_client.kwworm, kiwi_client.url, kiwi_user, "protobin" if protobin else "prototext")
        output_format = yt.SchemafulDsvFormat(columns=["error"])

    tmp_dir = tempfile.mkdtemp()
    files = []
    command_script = _get_read_ranges_command(
        "set -o pipefail", "{0} | python extract_value.py {1}".format(read_command, write_command))
    files.append(_pack_string("command.sh", command_script, tmp_dir))
    files.append(_pack_string("extract_value.py", extract_value_script, tmp_dir))

    try:
        run_operation_and_notify(
            message_queue,
            kiwi_transmittor,
            lambda client, strategy:
                client.run_map(
                    "bash -ux command.sh",
                    range_table,
                    output_table,
                    files=files,
                    input_format=yt.YamrFormat(lenval=False,has_subkey=False),
                    output_format=output_format,
                    memory_limit=1000 * yt.common.MB,
                    spec=spec,
                    strategy=strategy))
    finally:
        shutil.rmtree(tmp_dir)

def copy_yt_to_kiwi(yt_client, kiwi_client, kiwi_transmittor, src, **kwargs):
    ranges = _split_rows_yt(yt_client, src, 256 * yt.common.MB)
    fastbone = kwargs.get("fastbone", True)
    if "fasbone" in kwargs:
        del kwargs["fasbone"]
    read_command = _get_read_from_yt_command(yt_client, src, "<lenval=true>yamr", fastbone)
    _copy_to_kiwi(kiwi_client, kiwi_transmittor, src, read_command=read_command, ranges=ranges, **kwargs)

def copy_yamr_to_kiwi():
    pass

