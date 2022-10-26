from typing import List
from typing import Optional
from typing import Set
from typing import Union
import sys

from pyspark.sql import DataFrame, functions as f, Window, types as t, Column
from spyt import spark_session

input_path = sys.argv[1]
output_path = sys.argv[2]

with spark_session() as spark:
    data = spark.read.option("header", "true").csv(input_path[1:] + "/data")

    data.write.mode("overwrite").optimize_for("scan").yt("ytTable:/" + output_path)
