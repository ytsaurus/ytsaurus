from yt.zip import ZipFile
from yt.wrapper.common import generate_uuid
import yt.logger as logger
import yt.wrapper as yt

import os
from copy import deepcopy
from cStringIO import StringIO

def pack_module(module_name, output_dir):
    module = __import__(module_name)
    module_path = module.__file__.strip("__init__.py").strip("__init__.pyc")
    if module_path.endswith(".egg"):
        return module_path
    else:
        zip_filename = os.path.join(output_dir, module_name + ".zip")
        with ZipFile(zip_filename, "w") as zip:
            for root, dirs, files in os.walk(module_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    assert file_path.startswith(module_path)
                    destination = file_path[len(module_path):]
                    if "bindings" in file_path:
                        continue
                    zip.write(file_path, destination)
        return zip_filename

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

def copy_yamr_to_yt_pull(yamr_client, yt_client, src, dst, token, spec_template, mr_user=None, message_queue=None):
    yt_client = deepcopy(yt_client)
    yamr_client = deepcopy(yamr_client)

    if mr_user is not None:
        yamr_client.mr_user = mr_user

    yt_client.token = token
    portion_size = 1024 ** 3

    proxies = yamr_client.proxies
    if not proxies:
        proxies = [yamr_client.server]

    record_count = yamr_client.records_count(src, allow_cache=True)
    sorted = yamr_client.is_sorted(src, allow_cache=True)

    logger.info("Importing table '%s' (row count: %d, sorted: %d)", src, record_count, sorted)

    yt_client.create_table(dst, recursive=True, ignore_existing=True)

    ranges = []
    record_threshold = max(1, record_count * portion_size / yamr_client.data_size(src))
    for i in xrange((record_count - 1) / record_threshold + 1):
        server = proxies[i % len(proxies)]
        start = i * record_threshold
        end = min(record_count, (i + 1) * record_threshold)
        ranges.append((server, start, end))

    temp_table = yt_client.create_temp_table(prefix=os.path.basename(src))
    yt_client.write_table(temp_table,
                          ["\t".join(map(str, range)) + "\n" for range in ranges],
                          format=yt.YamrFormat(lenval=False, has_subkey=True))

    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 1

    temp_yamr_table = "tmp/yt/" + generate_uuid()
    yamr_client.copy(src, temp_yamr_table)
    src = temp_yamr_table

    read_command = yamr_client.get_read_range_command(src)
    command = 'while true; do '\
                  'IFS="\t" read -r server start end; '\
                  'if [ "$?" != "0" ]; then break; fi; '\
                  'set -e; '\
                  '{0}; '\
                  'set +e; '\
              'done;'\
                  .format(read_command)
    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        run_operation_and_notify(
            message_queue,
            yt_client,
            lambda client, strategy:
                client.run_map(
                    command,
                    temp_table,
                    dst,
                    input_format=yt.YamrFormat(lenval=False, has_subkey=True),
                    output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                    files=yamr_client.binary,
                    memory_limit = 2500 * yt.config.MB,
                    spec=spec,
                    strategy=strategy))

        if sorted:
            logger.info("Sorting '%s'", dst)
            run_operation_and_notify(
                message_queue,
                yt_client,
                lambda client, strategy: client.run_sort(dst, sort_by=["key", "subkey"], strategy=strategy))

        result_record_count = yt_client.records_count(dst)
        if result_record_count != record_count:
            error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
            logger.error(error)
            raise yt.YtError(error)

    finally:
        yamr_client.drop(temp_yamr_table)

def copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, mr_user=None, message_queue=None):
    lenval_to_nums_script = """
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

    count += 1
"""

    read_from_yt_script = """
#!/bin/bash -eux

while true; do
    read -r start end;
    if [ "$?" != "0" ]; then break; fi;
    PYTHONPATH=. YT_HOSTS="{0}" yt/wrapper/yt read "{1}"'[#'"${{start}}"':#'"${{end}}"']' --format "<has_subkey=true;lenval=true>yamr" --proxy {2};
done;
""".format(yt_client.hosts, src, yt_client.proxy)

    uuid = generate_uuid()

    tmp_dir = "/tmp/" + uuid
    os.mkdir(tmp_dir)

    dateutil_file = pack_module("dateutil", tmp_dir)
    yt_file = pack_module("yt", tmp_dir)
    lenval_to_nums_file = os.path.join(tmp_dir, "lenval_to_nums.py")
    with open(lenval_to_nums_file, "w") as fout:
        fout.write(lenval_to_nums_script)
    read_from_yt_file = os.path.join(tmp_dir, "read_from_yt.sh")
    with open(read_from_yt_file, "w") as fout:
        fout.write(read_from_yt_script)

    yt_client = deepcopy(yt_client)
    yamr_client = deepcopy(yamr_client)
    if mr_user is not None:
        yamr_client.mr_user = mr_user

    row_count = yt_client.get(src + "/@row_count")
    data_size = yt_client.get(src + "/@uncompressed_data_size")

    # number of rows per job
    rows_per_record = max(1, 1024 ** 3 * row_count / data_size)
    ranges = [(i * rows_per_record, min((i + 1) * rows_per_record, row_count))
              for i in xrange(1 + ((row_count - 1) / rows_per_record))]

    records_stream = StringIO()
    for start, end in ranges:
        records_stream.write(str(start))
        records_stream.write("\t")
        records_stream.write(str(end))
        records_stream.write("\n")

    temp_yamr_table = "tmp/yt/" + generate_uuid()
    yamr_client.write(temp_yamr_table, records_stream.getvalue())

    command = "unzip yt.zip -d yt >/dev/null; "\
              "unzip dateutil.zip -d dateutil >/dev/null; "\
              "python lenval_to_nums.py | bash read_from_yt.sh"
    yamr_client.run_map(command, temp_yamr_table, dst,
                      files=[dateutil_file, yt_file, lenval_to_nums_file, read_from_yt_file],
                      opts="-subkey -lenval -jobcount 500 -opt cpu.intensive.mode=1")

    result_row_count = yamr_client.records_count(dst)
    if row_count != result_row_count:
        yamr_client.drop(dst)
        error = "Incorrect record count (expected: %d, actual: %d)" % (row_count, result_row_count)
        logger.error(error)
        raise yt.YtError(error)

def copy_yt_to_yamr_push(yt_client, yamr_client, src, dst, token=None, spec_template=None, mr_user=None, message_queue=None):
    yt_client = deepcopy(yt_client)
    yamr_client = deepcopy(yamr_client)

    if mr_user is not None:
        yamr_client.mr_user = mr_user
    if token is not None:
        yt_client.token = token

    if not yamr_client.is_empty(dst):
        yamr_client.drop(dst)


    record_count = yt_client.records_count(src)

    if spec_template is None:
        spec_template = {}
    spec = deepcopy(spec_template)
    spec["data_size_per_job"] = 2 * 1024 * yt.config.MB

    write_command = yamr_client.get_write_command(dst)
    logger.info("Running map '%s'", write_command)

    run_operation_and_notify(
        message_queue,
        yt_client,
        lambda client, strategy:
            client.run_map(write_command, src, yt_client.create_temp_table(),
                           files=yamr_client.binary,
                           format=yt.YamrFormat(has_subkey=True, lenval=True),
                           memory_limit=2000 * yt.config.MB,
                           spec=spec,
                           strategy=strategy))

    result_record_count = yamr_client.records_count(dst)
    if record_count != result_record_count:
        yamr_client.drop(dst)
        error = "Incorrect record count (expected: %d, actual: %d)" % (record_count, result_record_count)
        logger.error(error)
        raise yt.YtError(error)
