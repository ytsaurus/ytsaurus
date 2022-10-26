import sys

from spyt import spark_session
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, ArrayType

input_path = sys.argv[1]
output_path = sys.argv[2]

with spark_session() as spark:
    eda_user_appsession = (spark.read
                           .schema_hint({'request_id_list': ArrayType(StringType())})
                           .yt(input_path + "/eda_user_appsession")
                           .filter((f.col("utc_session_start_dttm") >= "2021-12-19 00:00:00") &
                                   (f.col("utc_session_start_dttm") < "2021-12-20 00:00:00")))

    eats_layout_constructor_log = (spark.read
                                   .yt(input_path + "/eats_layout_constructor_log")
                                   .filter((f.col("utc_created_dttm") >= "2021-12-19 00:00:00") &
                                           (f.col("utc_created_dttm") < "2021-12-20 00:00:00")))

    eda_user_appsession_w_request_id = (
        eda_user_appsession
            .withColumn(
            'request_id', f.explode(f.col('request_id_list'))
        )
            .select(
            'request_id',
            'appsession_id'
        )

    )

    link_eda_user_appsession_request_id = (
        eats_layout_constructor_log
            .select(
            'request_id',
            'utc_created_dttm'
        )
            .distinct()
            .join(eda_user_appsession_w_request_id, on='request_id', how='inner')
            .select(
            'appsession_id', 'utc_created_dttm', 'request_id',
        )
    )

    (link_eda_user_appsession_request_id
     .sort("appsession_id")
     .coalesce(1)
     .write
     .mode("overwrite")
     .optimize_for("scan")
     .sorted_by("appsession_id")
     .yt(output_path))
