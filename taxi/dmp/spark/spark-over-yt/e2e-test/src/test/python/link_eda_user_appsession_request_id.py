from spyt import spark_session
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, ArrayType

test_name = "link_eda_user_appsession_request_id"
input_path = "//home/spark/e2e/{}/input".format(test_name)
output_path = "//home/spark/e2e/{}/output".format(test_name)
print(test_name)
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

    link_eda_user_appsession_request_id.write.mode("overwrite").optimize_for("scan").yt(output_path)
