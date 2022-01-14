from spyt import spark_session
from pyspark.sql import DataFrame, functions as f, Window

test_name = "fct_extreme_user_order_act"
input_path = "//home/spark/e2e/{}/input".format(test_name)
output_path = "//home/spark/e2e/{}/output".format(test_name)
print(test_name)

UNKNOWN_BRAND_LITERAL = 'unknown'
ALL_BRAND_LITERAL = '__all__'
TEMP_FAKE_BRAND_LITERAL = '__none__'

with spark_session() as spark:
    order = (spark.read
             .yt(input_path + "/order")
             .filter((f.col("utc_order_created_dt") >= "2022-01-10") &
                     (f.col("utc_order_created_dt") <= "2022-01-11")))
    uber_order = (spark.read
                  .yt(input_path + "/uber_order")
                  .filter((f.col("utc_order_created_dt") >= "2022-01-10") &
                          (f.col("utc_order_created_dt") <= "2022-01-11")))
    current_extreme_user_order = (spark.read.yt(input_path + "/current_extreme_user_order"))

    order_processed = order.union(uber_order).select(
        'brand',
        'user_phone_pd_id',
        'order_id',
        'utc_order_created_dttm',
        'success_order_flg',
    ).withColumnRenamed(
        'user_phone_pd_id', 'phone_pd_id'
    ).withColumnRenamed(
        'brand', 'brand_name'
    ).filter(
        (f.col('success_order_flg') == f.lit(True)) &
        (f.col('brand_name').isNotNull()) &
        (f.col('user_phone_pd_id').isNotNull())
    ).drop(
        'success_order_flg'
    )

    current_user_order = current_extreme_user_order.select(
        'brand_name',
        'phone_pd_id',
        f.explode(f.create_map(
            f.col('first_order_id'), f.col('utc_first_order_dttm'),
            f.col('last_order_id'), f.col('utc_last_order_dttm'),
        )).alias('order_id', 'utc_order_created_dttm'),
    ) # mapKeyDedupPolicy нужен для случаев, когда первый и последний заказы совпадают

    all_order = order_processed.union(
        current_user_order
    )

    window_w_brand = Window.partitionBy(
        'phone_pd_id', 'brand_name'
    ).orderBy(
        'utc_order_created_dttm', 'order_id'
    ).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    first_order_w_brand = all_order.select(
        'phone_pd_id',
        'order_id',
        'utc_order_created_dttm',
        f.when(
            (f.col('brand_name') != f.lit(ALL_BRAND_LITERAL)) &
            (f.col('brand_name') != f.lit(UNKNOWN_BRAND_LITERAL)),
            f.col('brand_name')
        ).otherwise(TEMP_FAKE_BRAND_LITERAL).alias('brand_name'),
    ).select(
        'phone_pd_id',
        f.first('order_id').over(window_w_brand).alias('first_order_id'),
        f.first('utc_order_created_dttm').over(window_w_brand).alias('utc_first_order_dttm'),
        f.last('order_id').over(window_w_brand).alias('last_order_id'),
        f.last('utc_order_created_dttm').over(window_w_brand).alias('utc_last_order_dttm'),
        'brand_name',
    ).dropDuplicates(
        ['phone_pd_id', 'brand_name']
    )

    window_wo_brand_first = Window.partitionBy(
        'phone_pd_id'
    ).orderBy(
        'utc_first_order_dttm', 'first_order_id'
    ).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    window_wo_brand_last = Window.partitionBy(
        'phone_pd_id'
    ).orderBy(
        'utc_last_order_dttm', 'last_order_id'
    ).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    first_order_wo_brand = first_order_w_brand.select(
        'phone_pd_id',
        f.first('first_order_id').over(window_wo_brand_first).alias('first_order_id'),
        f.first('utc_first_order_dttm').over(window_wo_brand_first).alias('utc_first_order_dttm'),
        f.last('last_order_id').over(window_wo_brand_last).alias('last_order_id'),
        f.last('utc_last_order_dttm').over(window_wo_brand_last).alias('utc_last_order_dttm'),
    ).dropDuplicates(
        ['phone_pd_id']
    ).withColumn(
        'brand_name', f.lit(ALL_BRAND_LITERAL)
    )

    res = first_order_w_brand.union(
        first_order_wo_brand
    ).filter(
        (f.col('brand_name') != f.lit(TEMP_FAKE_BRAND_LITERAL))
    )

    res.write.mode("overwrite").optimize_for("scan").yt(output_path)
