#!/usr/bin/python

import yt.wrapper as yt
import yt.logger as logger

import argparse
import os.path

class LogicError(Exception):
    pass

# If operation id is specified, retrieves the core infos of the given job.
def get_core_infos(parser, job_id, operation_id=None, **kwargs):
    assert not job_id is None
    if operation_id is None:
        logger.warning("Core dump names will not contain executable names because --operation-id is not specified. " +
                       "Hint: consider running 'file <core>' to find more information about core dumps.")
        return None
    
    jobs = yt.list_jobs(operation_id, data_source="archive")["jobs"]
    for failed_job in jobs:
        failed_job_id = failed_job["id"]
        core_infos = failed_job.get("core_infos", [])
        if failed_job_id == job_id:
            if len(core_infos) == 0:
                raise LogicError("Information in operation node in Cypress is inconsistent with core table content: " +
                                 "missing core infos for job {0} in operation {1}".format(job_id, operation_id))
            return core_infos
    raise LogicError("Information in operation node in Cypress in insonsistent with core table content: " +
                     "missing node for job {0} in operation {1}".format(job_id, operation_id))

# If core table path is specified explicitly, returns it, otherwise fetches it from the operation spec.
def get_core_table_path(parser, operation_id=None, core_table_path=None, **kwargs):
    if not core_table_path is None:
        return core_table_path
    if operation_id is None:
        parser.error("at least one of --operation-id and --core-table-path should be specified")
    spec = yt.get_operation(operation_id, attributes=["spec"])["spec"]
    if not "core_table_path" in spec:
        raise LogicError("Operation {0} doesn't have a specified core_table_path".format(operation_id))
    return spec["core_table_path"]

class CoreDumpWriter:
    def __init__(self, job_id, core_infos, core_ids, output_directory):
        self.job_id = job_id
        self.core_infos = core_infos
        self.core_ids = core_ids
        self.output_directory = output_directory

        self.current_core_id = None
        self.current_file = None
        self.current_core_size = None
        self.saved_core_dumps = set()

    def flush(self):
        if not self.current_file is None:
            if self.current_core_size and self.current_core_size != self.current_file.tell():
                logger.error("Information in operation node in Cypress is inconsistent with core table content: " +
                             "actual size of core with id {0} is {1} but it should be {2} according to the core info".format(self.current_core_id,
                                                                                                                             self.current_file.tell(),
                                                                                                                             self.current_core_size))
            self.current_file.close()
            self.current_file = None
            self.saved_core_dumps.add(self.current_core_id)
            self.current_core_id = None

    def _switch_to(self, core_id):
        self.flush()
        self.current_core_id = core_id
        if not self.core_infos is None:
            core_info = self.core_infos[core_id]
            if "error" in core_info:
                logger.error("Information in operation node in Cypress is inconsistent with core table content: " +
                             "core table contains core with id {0}, though its core info contains error '{1}'".format(core_id, core_info["error"]))
            core_name = "core.{0}.{1}".format(core_info["executable_name"], core_info["process_id"])
            self.current_core_size = core_info["size"]
        else:
            core_name = "core.{0}".format(core_id)
        core_path = os.path.join(self.output_directory, core_name)

        logger.info("Started writing core with id {0} to {1}".format(core_id, core_path))

        self.current_file = open(core_path, "w")

    def feed(self, row):
        if row["job_id"] != self.job_id:
            return False
        if self.current_core_id != row["core_id"]:
            self._switch_to(row["core_id"])
        self.current_file.write(row["data"])
        return True

    def finalize(self):
        self.flush()
        if not self.core_ids is None:
            for core_id in self.core_ids:
                if not core_id in self.saved_core_dumps:
                    logger.error("Information in operation node in Cypress is inconsistent with core table content: " +
                                 "core table does not contain information about core with id {0}, though it is mentioned in core_infos".format(core_id))

def process_errored_core_dumps(core_infos, core_ids):
    if core_infos is None:
        return
    for i, core_info in enumerate(core_infos):
        if "error" in core_info and (core_ids is None or i in core_ids):
            logger.warn("Core dump with id {0} was not saved due to the following error: {1}".format(core_info["error"]))

def download_core_dumps(parser, core_table_path, job_id=None, core_ids=None, output_directory=None, **kwargs):
    assert not output_directory is None

    if job_id is None and core_ids is None:
        ranges = None
    elif not job_id is None and core_ids is None:
        ranges = [{"exact": {"key": [job_id]}}]
    elif not job_id is None and not core_ids is None:
        ranges = [{"exact": {"key": [job_id, core_id]}} for core_id in core_ids]
    else:
        parser.error("--core-id couldn't be specified without specifying --job-id")

    rows = yt.read_table(yt.TablePath(core_table_path, ranges=ranges))

    # If we should choose an arbitrary job, we have to consider the first row in
    # the list of retrieved rows and determine the job_id by it.
    first_row = None
    if job_id is None:
        first_row = next(rows, None)
        if first_row is None:
            raise LogicError("Table {0} is empty".format(core_table_path))
        job_id = first_row["job_id"]

    core_infos = get_core_infos(parser, job_id, **kwargs)
    if not core_infos is None:
        core_ids = range(len(core_infos))
    process_errored_core_dumps(core_infos, core_ids)
    writer = CoreDumpWriter(job_id, core_infos, core_ids, output_directory)

    if not first_row is None:
        assert writer.feed(first_row)

    for row in rows:
        if not writer.feed(row):
            break
    writer.finalize()

def main():
    parser = argparse.ArgumentParser(description="Tool for downloading job core dumps.")
    parser.add_argument("-p", "--proxy", type=yt.config.set_proxy, help="YT proxy.")
    parser.add_argument("-o", "--operation-id", type=str, help="Operation id (should be specified if proper core file naming is needed).")
    parser.add_argument("-t", "--core-table-path", type=str, help="A path to the core table.")
    parser.add_argument("-j", "--job-id", type=str, help="Id of a job that produced core dump. If not specified, an arbitrary job with core dumps is taken.")
    parser.add_argument("-c", "--core-id", type=int, action="append", dest="core_ids",
                        help="Indices of core dumps during the given job that should be downloaded, " +
                             "several indices may be specified. If not specified, all core dumps are downloaded. Requires --job-id to be specified.")
    parser.add_argument("-d", "--output-directory", default=".", type=str, help="A directory to save the core dumps. Defaults to the current working directory.")

    args = parser.parse_args()
    args_dict = vars(args)

    try:
        core_table_path = get_core_table_path(parser, **args_dict)
        del args_dict["core_table_path"]
        download_core_dumps(parser, core_table_path, **args_dict)
    except LogicError:
        logger.exception("Can not work due to the following error, aborting:")

if __name__ == "__main__":
    main()
