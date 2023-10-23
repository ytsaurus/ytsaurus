#!/usr/bin/env python

import logging
import glob
import getpass
import os
import pyspark
import pyspark.sql
import spyt.client
import spyt.enabler
import spyt.standalone
import spyt.utils
import sys

from yt.wrapper import YtClient
from yt.wrapper.http_helpers import get_user_name

def error_exit(message):
    logging.critical(message)
    sys.exit(1)

def find_hive_jars():
    path = os.path.join(sys.modules['pyspark'].__path__[0], 'jars')
    jars = list(glob.glob(os.path.join(path, '*hive*.jar')))
    if not jars:
        error_exit("Unable to find pyspark jar dependencies for Hive in {}.\n" \
                   "Check your pyspark installation. \n"
                   "Alternatively, run $ import.py --add_hive_jars false "
                   "--jars /path/to/hive/jars/.*.jar".format(path))
    return jars

def start_spyt_cluster(yt_client, args):
    spyt.standalone.start_spark_cluster(worker_cores = args.cores_per_executor,
                                        worker_memory = args.ram_per_core * args.cores_per_executor,
                                        worker_num = args.num_executors,
                                        worker_timeout = args.executor_timeout,
                                        discovery_path = args.discovery_path,
                                        tmpfs_limit = args.executor_tmpfs_limit,
                                        pool = args.pool or get_user_name(client=yt_client),
                                        enablers = spyt.enabler.SpytEnablers(enable_byop=False),
                                        spark_cluster_version=args.spark_cluster_version,
                                        params=spyt.standalone.SparkDefaultArguments.get_params(),
                                        client=yt_client)

def open_spark_session(yt_client, args):
    conf = spyt.client._build_spark_conf(discovery_path=args.discovery_path,
                                         num_executors=args.num_executors,
                                         cores_per_executor=args.cores_per_executor,
                                         driver_memory="1G",
                                         executor_memory_per_core=args.ram_per_core,
                                         client=yt_client)

    if args.metastore:
        conf.set('hive.metastore.uris', 'thrift://%s' % args.metastore)
        conf.set('spark.sql.warehouse.dir', args.warehouse_dir)


    if args.extra_conf:
        for keyvalue in args.extra_conf.split(','):
            conf.set(keyvalue.split('=')[0], keyvalue.split('=')[1])

    jar_list = []

    if args.metastore and args.add_hive_jars:
        jar_list = find_hive_jars()

    for pattern in args.jars:
        jar_list += list(glob.glob(pattern))

    conf.set('spark.jars', ','.join(jar_list))

    classpath = ':'.join(jar_list)
    spark_current_cp = os.environ.get('SPARK_DIST_CLASSPATH', '')
    if spark_current_cp:
        classpath = spark_current_cp + ':' + classpath

    os.environ['SPARK_DIST_CLASSPATH'] = classpath

    builder = pyspark.sql.SparkSession.builder
    if args.metastore:
        builder = builder.enableHiveSupport()
    return builder.config(conf=conf).getOrCreate()

def use_hive_db(spark, db):
    spark.sql("USE {}".format(db)).collect()

def read_jdbc(args, spark, db):
    ret = spark.read.format("jdbc") \
        .option('url', 'jdbc:%s://%s/%s' % (args.jdbc, args.jdbc_server, db))

    if args.jdbc_user:
        ret = ret.option('user', args.jdbc_user)

    if args.jdbc_password is not None:
        if args.jdbc_password:
            password = args.jdbc_password
        else:
            password = getpass.getpass('Database password:')

        ret = ret.option('password', password)

    return ret

def split_by(inp, sep):
    sep_pos = inp.find(sep)
    if sep_pos < 0:
        return None, inp
    return inp[:sep_pos], inp[sep_pos+1:]

def extract_fmt_path(input_path):
    fmt, path = split_by(input_path, ':')
    if not fmt:
        error_exit("--input must be in <source>:<path> format")
    return fmt, path

def validate_args(args, input_path, output_path):
    fmt, path = extract_fmt_path(input_path)
    if fmt == "hive" or fmt == "hive_sql":
        if not args.metastore:
            error_exit("--metastore should must host:port for Hive metastore")
        if not args.warehouse_dir:
            error_exit("--warehouse_dir must provide path to Hive warehouse")
    elif fmt == "jdbc" or fmt == "jdbc_sql":
        if not args.jdbc_server:
            error_exit("--jdbc_server must provide host:port for JDBC server")
    elif fmt == "text" or fmt == "orc" or fmt == "parquet" or fmt == "local_parquet":
        pass
    else:
        error_exit("Unsupported input format {}".format(fmt))

    mode, out = split_by(output_path, ':')
    if mode and mode != "overwrite" and mode != "append":
        error_exit("output write mode must be one of: overwrite, append")

def read_input(args, spark, input_path):
    fmt, path = extract_fmt_path(input_path)

    if fmt == "hive":
        return spark.read.table(path)
    elif fmt == "hive_sql":
        db, sql = split_by(path, ':')
        use_hive_db(spark, db)
        return spark.sql(sql)
    elif fmt == "jdbc":
        db, table = split_by(path, '.')
        return read_jdbc(args, spark, db).option('dbtable', table).load()
    elif fmt == "jdbc_sql":
        db, sql = split_by(path, ':')
        return read_jdbc(args, spark, db).option('dbtable', '({}) as r'.format(sql)).load()
    elif fmt == "text":
        return spark.read.text(path)
    elif fmt == "orc":
        return spark.read.orc(path)
    elif fmt == "parquet":
        return spark.read.parquet(path)
    elif fmt == "local_parquet":
        import pandas as pd
        return spark.createDataFrame(pd.read_parquet(path, engine='pyarrow'))

def write_output(data, output_path):
    mode, out = split_by(output_path, ':')

    data_write = data.write

    if mode:
        data_write = data_write.mode(mode)

    data_write.yt(out)


def main():
    parser = spyt.utils.get_default_arg_parser(prog="import.py")

    parser.add_argument("--start-spyt", required=False, default=False,
                        help="If true, start the SPYT cluster")

    parser.add_argument("--metastore", required=False,
                        help="host:port for Hive Metastore thrift service")
    parser.add_argument("--warehouse-dir", required=False,
                        help="Path to Hive warehouse in HDFS")
    parser.add_argument("--add_hive_jars", required=False, default=True,
                        help="If true, run SPYT operations with jar libraries for Hive. pyspark with these " \
                        "libraries must be installed.")

    parser.add_argument("--pool", help="If starting SPYT cluster, YT pool to run in")
    parser.add_argument("--num-executors", required=False, type=int, default=1)
    parser.add_argument("--cores-per-executor", required=False, type=int, default=1)
    parser.add_argument("--ram-per-core", required=False, default="2GB")

    parser.add_argument("--spark-cluster-version", required=False,
                        help="Spark cluster version, when starting SPYT cluster")

    parser.add_argument("--executor-timeout", required=False, type=str, default="1h",
                        help="Timeout for SPYT node, when starting a cluster")
    parser.add_argument("--executor-tmpfs-limit", required=False, type=str, default="2GB",
                        help="Tmpfs limit for SPYT node, when starting a cluster")

    parser.add_argument("--input", action='append', default=[],
                        help="Identifier for the imported object. " \
                        "Refer to documentation in import.md on how to describe imported data.")
    parser.add_argument("--output", action='append', default=[],
                        help="Path in YT to store imported data")

    parser.add_argument("--jdbc", required=False, help="JDBC driver type")
    parser.add_argument("--jdbc-server", required=False)
    parser.add_argument("--jdbc-user", required=False, default='')
    parser.add_argument("--jdbc-password", required=False)
    parser.add_argument("--extra-conf", required=False)

    parser.add_argument('--jars',
                        nargs='*',
                        default=[os.path.join(os.path.dirname(__file__), 'target/dependency/*.jar')],
                        help="Additional jar files to provide to the SPYT operation")

    args = parser.parse_args()

    if not args.discovery_path:
        error_exit("--discovery-path is required to identify SPYT cluster. " \
                   "It it is not running, provide --start-spyt true to start the cluster")

    if not args.input:
        logging.warning('--input argument is not provided, exiting.')
        return

    if len(args.input) != len(args.output):
        error_exit("Expected exactly one --output for every input --input")

    for (in_table, out_table) in zip(args.input, args.output):
        validate_args(args, in_table, out_table)

    yt_client = YtClient(proxy=args.proxy, token=spyt.utils.default_token())
    if args.start_spyt:
        start_spyt_cluster(yt_client, args)

    spark = open_spark_session(yt_client, args)

    try:
        for (in_table, out_table) in zip(args.input, args.output):
            input_data = read_input(args, spark, in_table)
            write_output(input_data, out_table)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
