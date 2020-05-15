import argparse
import json
import spyt
from yt.wrapper import write_file, YtClient

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--discovery-path", required=True)
    parser.add_argument("--conf", required=False, action='append',nargs=1)
    args, unknown_args = parser.parse_known_args()

    spark_conf = dict([x[0].split("=", 2) for x in (args.conf or [])])
    spark_conf["spark.driver.bindAddress"] = "0.0.0.0"
    spark_conf["spark.driver.host"] = "sashbel-dev.man.yp-c.yandex.net"

    spark = spyt.connect(
        yt_proxy="hume",
        discovery_path=args.discovery_path,
        spark_conf_args=spark_conf
    )
    spark.read.yt("//sys/spark/examples/example_1").show()

    conf_dict = dict(spark.sparkContext.getConf().getAll())
    conf_dict["spark.hadoop.yt.token"] = "xxxxx"
    client = YtClient(token=spark.conf.get("spark.hadoop.yt.token"), proxy=spark.conf.get("spark.hadoop.yt.proxy"))
    write_file(args.path, json.dumps(conf_dict).encode("utf-8"), client=client)