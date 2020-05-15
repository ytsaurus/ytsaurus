import argparse
import json
import spyt
from yt.wrapper import write_file, YtClient

parser = argparse.ArgumentParser()
parser.add_argument("--path", required=True)
args, unknown_args = parser.parse_known_args()

with spyt.spark_session() as spark:
    spark.read.yt("//sys/spark/examples/example_1").show()
    conf_dict = dict(spark.sparkContext.getConf().getAll())
    conf_dict["spark.hadoop.yt.token"] = "xxxxx"
    client = YtClient(token=spark.conf.get("spark.hadoop.yt.token"), proxy=spark.conf.get("spark.hadoop.yt.proxy"))
    write_file(args.path, json.dumps(conf_dict).encode("utf-8"), client=client)
