#!/usr/bin/python2.7

import argparse
import logging
import yt.wrapper as yt
import yt.wrapper.operation_commands as operation_commands
import yt.yson as yson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)

def start_clique(bin_path, clique_type, prev_operation_id=None):
    attr_keys = yt.get(bin_path + "/@user_attribute_keys")
    attrs = yt.get(bin_path + "/@", attributes=attr_keys)
    attrs["previous_operation_id"] = prev_operation_id
    attrs["previous_operation_url"] = yson.to_yson_type(operation_commands.get_operation_url(prev_operation_id), attributes={"_type_tag": "url"})
    alias = "*ch_" + clique_type
    yt.start_clickhouse_clique(
        16,
        cpu_limit=8,
        enable_monitoring=True,
        enable_query_log=True,
        cypress_geodata_path="//sys/clickhouse/geodata/geodata.tgz",
        cypress_ytserver_clickhouse_path=bin_path,
        spec={
            "acl": [{
                "subjects": ["yandex"],
                "permissions": ["read"],
                "action": "allow"
            }],
            "title": clique_type.capitalize() + " clique",
            "max_failed_job_count": 10 * 1000,
            "pool": "chyt",
            "alias": alias,
            "description": attrs,
        },
        clickhouse_config={
            "profiling_tags": {"operation_alias": alias}
        })

def main():
    parser = argparse.ArgumentParser(description="Restart public (or its equivalent, for example, prestable) clique")
    parser.add_argument("-b", "--bin", default="//sys/clickhouse/bin/ytserver-clickhouse", help="ytserver-clickhouse binary to use")
    parser.add_argument("-t", "--type", default="public", help="Clique type; for example 'public' or 'prestable'")
    args = parser.parse_args()

    try:
        logger.info("Starting clique")
        start_clique(args.bin, args.type)
        logger.info("Clique started successfully")
        return
    except yt.YtError as err:
        logger.info("Caught an exception")
        operation_id = None
        try:
            operation_id = err.inner_errors[0]["inner_errors"][0]["attributes"]["operation_id"]
        except:
            pass
        if operation_id is None:
            logger.info("Exception is unrelated to alias being already used")
            raise err
    logger.info("Alias is already used by operation with id %s, aborting it", operation_id)
    yt.abort_operation(operation_id)
    logger.info("Previous operation aborted, starting new clique mentioning previous operation in the description")
    start_clique(args.bin, args.type, prev_operation_id=operation_id)

if __name__ == "__main__": 
    main()

