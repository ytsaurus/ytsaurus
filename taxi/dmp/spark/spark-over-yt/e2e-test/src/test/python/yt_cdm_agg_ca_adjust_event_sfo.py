from pyspark.sql import functions as f, types as t
from spyt import spark_session

test_name = "yt_cdm_agg_ca_adjust_event_sfo"
input_path = "//home/spark/e2e/{}/input".format(test_name)
output_path = "//home/spark/e2e/{}/output".format(test_name)
print(test_name)

SPARK_DATE_FORMAT = 'yyyy-MM-dd'

with spark_session() as spark:
    ods_success_order_log = spark.read.yt(input_path + "/ods_success_order_log/1",
                                          input_path + "/ods_success_order_log/2")
    link_adjust_id_user_phone_id_hist = spark.read.yt(input_path + "/link_adjust_id_user_phone_id_hist")
    user_phone = spark.read.yt(input_path + "/user_phone")
    ltv_act_ltv = spark.read.yt(input_path + "/ltv_act_ltv")

    ltv_act_ltv = ltv_act_ltv.select(
        'phone_pd_id',
        'service_name',
        'ltv_26_gmv_adj'
    )

    eventlog_w_ltv = ods_success_order_log.join(
        link_adjust_id_user_phone_id_hist,
        ['adjust_id', 'service'],
        'left'
    ).filter(
        f.col('moscow_event_dttm').between(f.col('moscow_effective_from_dttm'), f.col('moscow_effective_to_dttm'))
        | f.col('moscow_effective_to_dttm').isNull()
    ).join(
        user_phone,
        'user_phone_id',
        'left'
    ).withColumn(
        ##сервис для джоина с лтв
        'service_name',
        f.when(
            f.col('service').isin('uber az', 'uber by', 'uber kz', 'uber ru'), 'Uber'
        ).when(
            f.col('service') == 'yango', 'Yango'
        ).when(
            f.col('service') == 'yandex taxi', 'Яндекс.Такси'
        ).otherwise(f.col('service'))
    ).join(
        ltv_act_ltv,
        ['service_name', 'phone_pd_id'],
        'left'
    ).select(
        'brand',
        'platform',
        'utm_source',
        'utm_campaign',
        'utm_content',
        'utm_term',
        'country',
        'event_type',
        'moscow_click_time_dttm',
        'moscow_installed_at_dttm',
        'moscow_reattribitted_at_dttm',
        'moscow_event_dttm',
        f.col('performance_city').alias('city'),
        f.date_format(f.col('moscow_event_dttm').cast(t.TimestampType()), SPARK_DATE_FORMAT).alias('moscow_dt'),
        f.coalesce('ltv_26_gmv_adj', f.lit(0)).alias('ltv_26_gmv_adj'),
        f.lit(1).alias('event_cnt'),
        f.lit(None).alias('ltv_26_gmv_adj_attr'),
        f.lit(None).alias('event_cnt_attr'),
    )

    key_columns = ["moscow_dt", "brand", "platform", "utm_source", "utm_campaign",
                   "utm_content", "utm_term", "city", "country", "event_type"]
    df_attr = eventlog_w_ltv.select(
        f.date_format(
            f.coalesce(
                f.col('moscow_click_time_dttm'),
                f.col('moscow_installed_at_dttm'),
                f.col('moscow_reattribitted_at_dttm'),
                f.col('moscow_event_dttm'),
            ).cast(t.TimestampType()), SPARK_DATE_FORMAT
        ).alias('moscow_dt'),
        'brand',
        'platform',
        'utm_source',
        'utm_campaign',
        'utm_content',
        'utm_term',
        'country',
        'event_type',
        'moscow_click_time_dttm',
        'moscow_installed_at_dttm',
        'moscow_reattribitted_at_dttm',
        'moscow_event_dttm',
        'city',
        f.col('ltv_26_gmv_adj').alias('ltv_26_gmv_adj_attr'),
        f.col('event_cnt').alias('event_cnt_attr'),
        f.lit(None).alias('ltv_26_gmv_adj'),
        f.lit(None).alias('event_cnt')
    )

    res = eventlog_w_ltv.unionByName(df_attr).groupBy(
        key_columns
    ).agg(
        f.sum('ltv_26_gmv_adj').alias('ltv_26_gmv_adj'),
        f.sum('ltv_26_gmv_adj_attr').alias('ltv_26_gmv_adj_attr'),
        f.sum('event_cnt').alias('event_cnt'),
        f.sum('event_cnt_attr').alias('event_cnt_attr'),
    )

    res.write.mode("overwrite").optimize_for("scan").yt(output_path)
