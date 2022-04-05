from typing import List
from typing import Optional
from typing import Set
from typing import Union
import sys

from pyspark.sql import DataFrame, functions as f, Window, types as t, Column
from spyt import spark_session

input_path = sys.argv[1]
output_path = sys.argv[2]

# минимальное число заказов, чтобы определять водителя как хейтера
MIN_ORDER_CNT = 50

with spark_session() as spark:
    taximeter_feedback = (spark.read
                          .yt(input_path + "/taximeter_feedback")
                          .filter((f.col("utc_event_dttm") >= "2022-01-10 00:00:00") &
                                  (f.col("utc_event_dttm") < "2022-01-13 00:00:00")))

    agg_executor_hateometer = (spark.read
                               .yt(input_path + "/agg_executor_hateometer")
                               .filter((f.col("utc_business_dt") >= "2022-01-10 00:00:00") &
                                       (f.col("utc_business_dt") < "2022-01-11 00:00:00")))

    order = (spark.read
             .yt(input_path + "/order")
             .filter((f.col("utc_order_created_dt") >= "2022-01-10 00:00:00") &
                     (f.col("utc_order_created_dt") < "2022-01-11 00:00:00")))

    app_mapping = spark.read.yt(input_path + "/app_mapping")

    executor_hateometer = agg_executor_hateometer.select(
        'utc_business_dt',
        'park_taximeter_id',
        'executor_profile_id',
        'order_cnt',
        'bad_score_cnt',
    ).where(
        # order_cnt имеет тип UInt, мы в явном виде преобразовываем его в Long, иначе фейлится сравнение int vs bigint
        f.col('order_cnt').cast(t.LongType()) >= f.lit(MIN_ORDER_CNT).cast(t.LongType()),
        ).withColumn(
        'driver_bad_feedback_shr', f.col('bad_score_cnt').cast(t.LongType()) / f.col('order_cnt').cast(t.LongType())
    ).drop(
        'order_cnt', 'bad_score_cnt',
    )

    order_score = taximeter_feedback.select(
        'utc_event_dttm',
        'taximeter_order_id',
        'score',
    ).withColumnRenamed(
        'score',
        'rating',
    ).where(
        (f.col('taximeter_order_id').isNotNull()) &
        (f.col('rating').isNotNull()) &
        (f.col('rating') != f.lit(0)),
        ).withColumn(
        'row_num',
        f.row_number().over(
            # для одного заказа может быть несколько отзывов
            Window.partitionBy(
                'taximeter_order_id',
            ).orderBy(
                f.col('utc_event_dttm'),
            ),
        ),
    ).where(
        f.col('row_num') == f.lit(1),
        ).drop(
        'utc_event_dttm',
        'row_num',
    )

    res = order.select(
        'user_uid',
        'application',
        'taximeter_order_id',
        'utc_order_created_dttm',
        'success_order_flg',
        'taximeter_park_id',
        'driver_uuid',
        'driver_tariff_class',
    ).withColumnRenamed(
        'taximeter_park_id',
        'park_taximeter_id',
    ).withColumnRenamed(
        'driver_uuid',
        'executor_profile_id',
    ).withColumnRenamed(
        'utc_order_created_dttm',
        'utc_order_dttm',
    ).withColumnRenamed(
        'application',
        'application_platform',
    ).where(
        (f.col('park_taximeter_id').isNotNull()) &
        (f.col('executor_profile_id').isNotNull()) &
        (f.col('application_platform').isNotNull()) &
        (f.col('taximeter_order_id').isNotNull()) &
        (f.col('user_uid').isNotNull()) &
        (f.col('user_uid') != f.lit('')) &
        (f.col('success_order_flg') == f.lit(True)) &
        (~f.col('driver_tariff_class').isin({'eda', 'lavka', 'drive'})),
        ).drop(
        'success_order_flg',
        'driver_tariff_class',
    ).join(
        app_mapping,
        on='application_platform',
        how='inner',
    ).withColumn(
        'brand',
        f.when(
            f.col('app_name').contains('yango'),
            'yango',
        )
            .when(
            f.col('app_name').contains('uber'),
            'yauber',
        )
            .otherwise(
            'yataxi',
        ),
            ).drop(
        'application_platform',
        'app_name',
    ).join(
        order_score,
        on='taximeter_order_id',
        how='left',
    ).withColumn(
        'utc_business_dt',
        f.col('utc_order_dttm').substr(1, 10),
    ).join(
        executor_hateometer,
        on=['utc_business_dt', 'park_taximeter_id', 'executor_profile_id'],
        how='left',
    ).drop(
        'utc_business_dt',
        'park_taximeter_id',
        'executor_profile_id',
    )

    res.write.mode("overwrite").optimize_for("scan").yt(output_path)
