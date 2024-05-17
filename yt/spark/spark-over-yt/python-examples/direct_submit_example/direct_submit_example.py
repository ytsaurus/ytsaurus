import argparse
import spyt

from pyspark.sql.functions import col
from pyspark import SparkConf


def run_job(yt_proxy: str, spyt_version: str = None):
    print(f"SPYT Direct submit example.\nSPYT version: {spyt_version}\n"
          "This job reads the table at //sys/spark/examples/example_1,\n"
          "groups by id mod 11, calculates the count in each group\n"
          "and shows the result in console\n")

    conf = SparkConf()
    conf.set("spark.app.name", "Direct submit example")
    if spyt_version:
        conf.set("spark.ytsaurus.spyt.version", spyt_version)
        if spyt_version.endswith("-SNAPSHOT"):
            conf.set("spark.ytsaurus.config.releases.path", "//home/spark/conf/snapshots")
            conf.set("spark.ytsaurus.spyt.releases.path", "//home/spark/spyt/snapshots")

    with spyt.direct_spark_session(yt_proxy, conf) as spark:
        df = spark.read.yt("//sys/spark/examples/example_1")
        df.groupBy((col("id") % 11).alias("rem")).count().show(11, False)
    print("Direct Spark session was successfully stopped, the result is printed to the console.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--proxy', required=True, help="YT proxy address")
    parser.add_argument('--spyt-version', help="Custom SPYT version")
    args, _ = parser.parse_known_args()
    run_job(args.proxy, args.spyt_version)


if __name__ == '__main__':
    main()
