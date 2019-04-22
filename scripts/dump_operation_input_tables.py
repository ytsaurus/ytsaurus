#!/usr/bin/python

import yt.wrapper as yt
import yt.wrapper.yson as yson
import copy
import sys
import argparse
import logging

logger = logging.getLogger(__name__)

ATTRIBUTES_TO_REMOVE = ["foreign", "columns", "ranges"]

def get_table_name(path):
    if path.startswith("#"):
        return path[1:]
    else:
        return path[path.rfind("/") + 1:]


def get_dump_table_spec_builder(src_path, dst_path, input_tx):
    logger.debug("Dumping table %s to %s", src_path, dst_path)
    src_path = copy.deepcopy(src_path)
    for attribute in ATTRIBUTES_TO_REMOVE:
        src_path.attributes.pop(attribute, None)
    if "transaction_id" in src_path.attributes:
        logger.debug("Input path has tx = %s specified as an attribute, using it", src_path.attributes["transaction_id"])
        tx = src_path.attributes["transaction_id"]
    else:
        logger.debug("Using input tx = %s", input_tx)
        tx = input_tx
    src_path.attributes["transaction_id"] = tx
   
    if yt.exists(dst_path):
        logger.debug("Output path %s already exists, removing it", dst_path)
        yt.remove(dst_path)

    with yt.Transaction(transaction_id=tx):
        schema = yt.get(src_path + "/@schema")
    logger.debug("Original schema was: %s", schema)
    yt.create("table", dst_path, attributes={"schema": schema})

    spec_builder = yt.spec_builders.MergeSpecBuilder() \
        .input_table_paths(src_path) \
        .output_table_path(dst_path) \
        .mode("ordered")
    return spec_builder

def main(args):
    dst_dir = args.destination_dir
    if dst_dir.endswith("/"):
        dst_dir = dst_dir[:-1]

    op_id = args.operation_id
    renumerate_tables = args.renumerate_tables

    if not args.dry_run and not yt.exists(dst_dir):
        logger.info("Creating destination directory")
        yt.create("map_node", dst_dir)
    
    if yt.exists(dst_dir) and yt.get(dst_dir + "/@count") > 0:
        logger.warn("Destination directory exists and is not empty; output tables and spec may override some of the existing nodes")
    logger.info("Destination directory is %s", dst_dir)
    
    op = yt.get_operation(op_id)
    input_tx = op.get("input_transaction_id")
    logger.info("Input transaction id is %s", input_tx)
    spec = op["spec"]
    input_paths = spec["input_table_paths"]

    if len(set(map(get_table_name, input_paths))) != len(input_paths):
        logger.warn("Input table names collide, forcing table renumeration")
        renumerate_tables = True

    with yt.OperationsTrackerPool(args.pool_size, poll_period=args.pool_poll_period) as tracker:
        for i, path in enumerate(input_paths):
            if renumerate_tables:
                dst_name = "%03d".format(i)
            else:
                dst_name = get_table_name(path)
            dst_path = dst_dir + "/" + dst_name
            spec_builder = get_dump_table_spec_builder(path, dst_path, input_tx)

            if not args.dry_run:
                tracker.add(spec_builder)
            
            rewritten_src_path = path
            rewritten_src_path.attributes.pop("transaction_id", None)
            logger.debug("Rewritten path for %s is %s", path, rewritten_src_path)
            spec["input_table_paths"][i] = rewritten_src_path

    if args.print_spec:
        print yson.dumps(spec, yson_format="pretty")

    if not args.dry_run:
        if yt.exists(dst_dir + "/spec"):
            yt.remove(dst_dir + "/spec")
        yt.create("document", dst_dir + "/spec", attributes={"value": spec})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script dumps input tables of the operation to the given directory and produces document with rewritten operation spec.")
    parser.add_argument("operation_id", type=str, metavar="operation-id", help="Id of the operation")
    parser.add_argument("destination_dir", type=str, metavar="destination-dir", help="Destination directory in which all operations and the resulting spec will reside")
    parser.add_argument("--renumerate-tables", action="store_true", 
                        help="Instead of keeping original table names, renumerate them as table000, table001, ... which is useful when input table names collide")
    parser.add_argument("--pool-size", type=int, default=10, help="Maximum number of concurrently running merge operations")
    parser.add_argument("--pool-poll-period", type=int, default=1000, help="Poll period in ms")
    parser.add_argument("--dry-run", action="store_true", help="Only log what is going to happen")
    parser.add_argument("--print-spec", action="store_true", help="Print resulting spec to stdout")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print lots of debugging information")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)


    main(args)

