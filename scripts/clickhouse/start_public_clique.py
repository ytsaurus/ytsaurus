#!/usr/bin/python2.7

import argparse
import logging
import yt.wrapper as yt
import yt.yson as yson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)

def format_url(url):
    return yson.to_yson_type(url, attributes={"_type_tag": "url"})

def start_clique(bin_path, clique_type, prev_operation_id=None):
    alias = "*ch_" + clique_type
    
    attr_keys = yt.get(bin_path + "/@user_attribute_keys")
    description = yt.get(bin_path + "/@", attributes=attr_keys)
    description["previous_operation_id"] = prev_operation_id
    description["previous_operation_url"] = format_url(yt.operation_commands.get_operation_url(prev_operation_id))

    proxy_url = yt.http_helpers.get_proxy_url(required=False)
    default_suffix = yt.config.get_config(None)["proxy"]["default_suffix"]
    if proxy_url is not None and proxy_url.endswith(default_suffix):
        cluster_name = proxy_url[:-len(default_suffix)]
        description["monitoring_url"] = format_url("https://solomon.yandex-team.ru/?project=yt&cluster={}&service=yt_clickhouse&operation_alias={}".format(cluster_name, alias))
        description["yql_url"] = format_url("https://yql.yandex-team.ru/?query=use%20chyt.{}/{}%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE".format(cluster_name, alias[1:]))

    yt.start_clickhouse_clique(
        16,
        cpu_limit=8,
        enable_monitoring=True,
        enable_query_log=True,
        cypress_geodata_path="//sys/clickhouse/geodata/geodata.tgz",
        core_table_path="//home/max42/core_table_" + clique_type,
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
            "description": description,
        },
        clickhouse_config={
            "profile_manager": {
                "global_tags": {
                    "operation_alias": alias,
                },
            },
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

