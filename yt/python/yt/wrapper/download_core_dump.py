from .cypress_commands import get
from .format import YsonFormat
from .job_commands import list_jobs
from .operation_commands import get_operation
from .table_commands import read_table
from .ypath import TablePath

from yt.common import YtError, YtResponseError

import yt.logger as logger

import os.path


def bytes_to_str(byte_string):
    """Converts bytes to str for PY3. Does nothing for PY2.
    """
    return byte_string.decode(encoding="latin-1")


def stringify_core_table(row):
    str_row = {}
    for k, v in row.items():
        if isinstance(k, bytes):
            k = bytes_to_str(k)
        if k != "data" and isinstance(v, bytes):
            v = bytes_to_str(v)
        str_row[k] = v
    return str_row


def get_core_infos(job_id, operation_id=None, client=None):
    """If operation id is specified, retrieves the core infos of the given job.
    """
    if operation_id is None:
        logger.warning("Core dump names will not contain executable names because operation_id is not specified. "
                       "Hint: consider running 'file <core>' to find more information about core dumps.")
        return None

    jobs = list_jobs(operation_id, client=client)["jobs"]
    for failed_job in jobs:
        failed_job_id = failed_job["id"]
        core_infos = failed_job.get("core_infos", [])
        if failed_job_id == job_id:
            if not core_infos:
                raise YtError("Information in operation node in Cypress is inconsistent with core table content: "
                              "missing core infos for job {0} in operation {1}".format(job_id, operation_id))
            return core_infos
    raise YtError("Information in operation node in Cypress in inconsistent with core table content: "
                  "missing node for job {0} in operation {1}".format(job_id, operation_id))


def get_core_table_path(operation_id, client=None):
    """Fetches core_table_path from the operation spec.
    """
    spec = get_operation(operation_id, attributes=["spec"], client=client)["spec"]
    if "core_table_path" not in spec:
        raise YtError("Operation {0} does not have a specified core_table_path".format(operation_id))
    return spec["core_table_path"]


class CoreDumpWriter(object):
    def __init__(self, job_id, core_infos, core_indices, output_directory, sparse):
        self.job_id = job_id
        self.core_infos = core_infos
        self.core_indices = core_indices
        self.output_directory = output_directory

        self.current_core_index = None
        self.current_file = None
        self.current_file_path = None
        self.current_core_size = None
        self.saved_core_dumps = set()
        self.sparse = sparse
        self.buffer = b""
        self.total_size = 0
        self.total_disk_usage = 0

    def process_buffer(self, finalizing=False):
        SPARSE_CORE_DUMP_PAGE_SIZE = 65536
        UINT64_LENGTH = 8

        if self.sparse:
            buffer_ptr = 0
            while buffer_ptr + SPARSE_CORE_DUMP_PAGE_SIZE + 1 < len(self.buffer):
                if self.buffer[buffer_ptr:buffer_ptr + 1] == b"0":
                    zero_block_length = 0
                    for idx in range(buffer_ptr + 1 + UINT64_LENGTH, buffer_ptr, -1):
                        zero_block_length = 256 * zero_block_length + ord(self.buffer[idx:idx + 1])
                    self.current_file.seek(zero_block_length, 1)
                else:
                    if self.buffer[buffer_ptr:buffer_ptr + 1] != b"1":
                        logger.error("Sparse core dump is corrupted")
                        return
                    self.current_file.write(self.buffer[buffer_ptr + 1:buffer_ptr + 1 + SPARSE_CORE_DUMP_PAGE_SIZE])
                buffer_ptr += SPARSE_CORE_DUMP_PAGE_SIZE + 1

            self.buffer = self.buffer[buffer_ptr:]

            if finalizing:
                if self.buffer:
                    if self.buffer[0:1] != b"1":
                        logger.error("Sparse core dump is corrupted")
                        return
                    self.current_file.write(self.buffer[1:])
                self.buffer = b""
        else:
            if not self.current_file:
                return
            self.current_file.write(self.buffer)
            self.buffer = b""

    def flush(self):
        self.process_buffer(True)

        if self.current_file is not None:
            if not self.sparse and self.current_core_size and self.current_core_size != self.current_file.tell():
                logger.error("Information in operation node in Cypress is inconsistent with core table content: "
                             "actual size of core with index {0} is {1} but it should be {2} according to the core info"
                             .format(self.current_core_index, self.current_file.tell(), self.current_core_size))
            self.current_file.close()
            self.current_file = None
            self.saved_core_dumps.add(self.current_core_index)
            self.current_core_index = None

            stat = os.stat(self.current_file_path)
            self.total_size += stat.st_size
            self.total_disk_usage += stat.st_blocks * 512

    def _switch_to(self, core_index):
        self.flush()
        self.current_core_index = core_index
        if self.core_infos is not None:
            core_info = self.core_infos[core_index]
            if "error" in core_info:
                logger.error("Information in operation node in Cypress is inconsistent with core table content: "
                             "core table contains core with index {0}, though its core info contains error '{1}'"
                             .format(core_index, core_info["error"]))
            core_name = "core.{0}.{1}".format(core_info["executable_name"], core_info["process_id"])
            self.current_core_size = core_info["size"]
        else:
            core_name = "core.{0}".format(core_index)
        core_path = os.path.join(self.output_directory, core_name)

        logger.info("Started writing core with index {0} to {1}".format(core_index, core_path))

        self.current_file_path = core_path
        self.current_file = open(core_path, "wb")

    def feed(self, row):
        if row["job_id"] != self.job_id:
            return False

        # TODO: Remove "core_id" code after the replacement to "core_index" in 'yt' submodule.
        if "core_index" in row:
            if self.current_core_index != row["core_index"]:
                self._switch_to(row["core_index"])
        if "core_id" in row:
            if self.current_core_index != row["core_id"]:
                self._switch_to(row["core_id"])

        self.buffer += row["data"]
        self.process_buffer()
        return True

    def finalize(self):
        self.flush()
        if self.core_indices is not None:
            for core_index in self.core_indices:
                if core_index not in self.saved_core_dumps:
                    logger.error("Information in operation node in Cypress is inconsistent with core table content: "
                                 "core table does not contain information about "
                                 "core with index {0}, though it is mentioned in core_infos".format(core_index))

    def print_stats(self):
        logger.info("{0} core(-s) written, total size is {1} bytes, actual disk usage is {2} bytes"
                    .format(len(self.saved_core_dumps), self.total_size, self.total_disk_usage))


def process_errored_core_dumps(core_infos, core_indices):
    for index, core_info in enumerate(core_infos):
        if "error" in core_info and (core_indices is None or index in core_indices):
            logger.warning("Core dump with index {0} was not saved due to the following error: {1}".format(
                index, core_info["error"]))


def download_core_dump(output_directory, job_id=None, operation_id=None, core_table_path=None, core_indices=None,
                       client=None):
    """Downloads core dump for a given operation_id and job_id from a given core_table_path.
    If core_table_path is not specified, operation_id is used to generate core_table_path.
    If job_id is not specified, first dumped job is used.

    :param output_directory:
    :param job_id:
    :param operation_id:
    :param core_table_path:
    :param core_indices:
    :param client:
    :return: None
    """
    if core_table_path is None:
        if operation_id is None:
            raise YtError("At least one of operation_id and core_table_path should be specified")
        core_table_path = get_core_table_path(operation_id=operation_id, client=client)

    if job_id is None and core_indices is None:
        ranges = None
    elif job_id is not None and core_indices is None:
        ranges = [{"exact": {"key": [job_id]}}]
    elif job_id is not None and core_indices is not None:
        ranges = [{"exact": {"key": [job_id, core_index]}} for core_index in core_indices]
    else:
        raise YtError("core_indices could not be specified without specifying job_id")

    table = TablePath(core_table_path, ranges=ranges, client=client)
    rows = map(stringify_core_table,
               read_table(table=table, client=client, raw=False, format=YsonFormat(encoding=None)))

    try:
        sparse = get(core_table_path + "/@sparse", client=client)
    except YtResponseError as err:
        if err.is_resolve_error():
            sparse = False
        else:
            raise

    # If we should choose an arbitrary job, we have to consider the first row in
    # the list of retrieved rows and determine the job_id by it.
    first_row = None
    if job_id is None:
        first_row = next(rows, None)
        if first_row is None:
            raise YtError("Table {0} is empty".format(core_table_path))
        job_id = first_row["job_id"]

    core_infos = get_core_infos(job_id=job_id, operation_id=operation_id, client=client)
    if core_infos is not None:
        core_indices = range(len(core_infos))
        process_errored_core_dumps(core_infos, core_indices)

    logger.info("Started writing core dumps "
                "(job_id: {0}, core_indices: {1}, output_directory: {2}, sparse: {3})"
                .format(job_id, core_indices, output_directory, sparse))

    writer = CoreDumpWriter(job_id, core_infos, core_indices, output_directory, sparse)

    if first_row is not None:
        assert writer.feed(first_row)

    for row in rows:
        if not writer.feed(row):
            break
    writer.finalize()
    writer.print_stats()
