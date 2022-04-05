from typing import List
from typing import Optional
from typing import Set
from typing import Union
import sys

from pyspark.sql import DataFrame, functions as f, Window, types as t, Column
from spyt import spark_session

input_path = sys.argv[1]
output_path = sys.argv[2]

SPARK_MILLISECONDS_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'


# mypy: ignore-errors
def union_merging_schemas(*dfs: DataFrame) -> Optional[DataFrame]:
    """
    Делает union всех датафреймов из списка, добавляя недостающие колонки и заполняя их null'ами.
    Union в семантике Spark это аналог UNION ALL из SQL.
    Если надо сделать UNION прост, после вызова функции добавьте .distinct().

    Если список датафреймов пуст, возвращает None

    Пример использования:
    >>> united_df = union_merging_schemas(one_df, another_df, third_df)

    :param dfs: Список датафреймов
    :return: Объединение датафреймов.
    """
    result_df: Optional[DataFrame] = None
    known_columns_set: Set[str] = set()
    for df in dfs:
        if result_df is None:
            result_df = df
            known_columns_set.update(result_df.columns)
            continue

        current_df_columns = set(df.columns)
        current_df_known_columns: List[Union[Column, str]] = [
            df[x]
            if x in current_df_columns
            else f.lit(None).alias(x)
            for x in result_df.columns
        ]
        current_df_new_columns = [
            df[x]
            for x in df.columns
            if x not in known_columns_set
        ]

        result_df_new_columns = [
            f.lit(None).alias(x)
            for x in df.columns
            if x not in known_columns_set
        ]

        current_df = df.select(current_df_known_columns + current_df_new_columns)

        result_df = result_df.select(result_df.columns + result_df_new_columns)
        known_columns_set.update(result_df.columns)

        result_df = result_df.union(current_df)

    return result_df


with spark_session() as spark:
    commutation = (spark.read
                   .yt(input_path + "/commutation")
                   .filter((f.col("utc_created_dttm") >= "2021-12-06 23:00:00") &
                           (f.col("utc_created_dttm") < "2021-12-14 01:00:00")))

    operator_status_log = (spark.read
                           .yt(input_path + "/operator_status_log")
                           .filter((f.col("utc_created_dttm") >= "2021-12-06 00:00:00") &
                                   (f.col("utc_created_dttm") < "2021-12-14 00:00:00")))

    period_start = "2021-12-07 00:00:00"
    period_end = "2021-12-14 00:00:00"

    operator_status_events_df = (
        operator_status_log
            .withColumn('utc_event_dttm', f.col('utc_created_dttm').cast(t.TimestampType()))
            .withColumn('event_type', f.lit('operator_status_log'))
            .withColumnRenamed('operator_status_log_id', 'event_id')
            .withColumnRenamed('asterisk_login', 'staff_login')
            .withColumnRenamed('queue_code_list', 'queues_enabled_list')
            .withColumn('work_substatus_code', f.coalesce(f.col('work_substatus_code'), f.lit('')))
            .select(
            'agent_id',
            'utc_event_dttm',
            'event_type',
            'event_id',
            'work_status_code',
            'work_substatus_code',
            'staff_login',
            'queues_enabled_list',
        )
    )

    commutation_events_df = (
        commutation
            .withColumn('utc_answered_dttm', f.col('utc_answered_dttm').cast(t.TimestampType()))
            .withColumn('utc_completed_dttm', f.col('utc_completed_dttm').cast(t.TimestampType()))

            # Это всякие ненужные строки типа потерянных звонков и звонков из stub_queue (что это, кстати?)
            .filter(f.col('commutation_end_reason_type') != f.lit('abandoned'))
            .filter(f.col('call_id').isNotNull() & (f.col('call_id') != ''))

            .withColumnRenamed('commutation_id', 'event_id')
            .withColumnRenamed('commutation_queue_code', 'queue_code')
            .withColumnRenamed('utc_answered_dttm', 'utc_event_dttm')
            .withColumnRenamed('utc_completed_dttm', 'utc_commutation_end_dttm')
            .select(
            'agent_id',
            'event_id',
            'call_id',
            'callcenter_phone_number',
            'commutation_end_reason_type',
            'queue_code',
            'phone_pd_id',
            'transfer_destination_code',
            # Добавляем по строчке на каждый конец коммутации, чтобы не терять
            # дальше waiting между коммутациями в случаях когда не было смены статуса у оператора
            f.explode(f.create_map(
                f.lit('commutation_started'), f.col('utc_event_dttm'),
                f.lit('commutation_finished'), f.col('utc_commutation_end_dttm'),
            )).alias('event_type', 'utc_event_dttm'),
            'utc_commutation_end_dttm',
        )
            # Сбрасываем utc_commutation_end_dttm у событий окончания коммутации
            # (чтобы дальше не было проблем от него)
            .withColumn('utc_commutation_end_dttm', f.expr('''
            CASE WHEN event_type = 'commutation_started' THEN utc_commutation_end_dttm END
        '''))
            .withColumn("is_commutating", f.when(f.col("event_type") == f.lit("commutation_started"), f.lit("True"))
                        .otherwise(f.lit("False")))
    )

    all_events_df = union_merging_schemas(
        operator_status_events_df,
        commutation_events_df,
    )

    agent_window = Window.partitionBy('agent_id').orderBy('utc_event_dttm')


    def sift(col_name: str) -> Column:
        return f.last(col_name, True).over(agent_window).alias(col_name)


    event_cols = [
        'agent_id',
        'utc_event_dttm',
        'event_type',
        'event_id',
        'call_id',

        sift('staff_login'),
        sift('work_status_code'),
        sift('work_substatus_code'),
        sift('queues_enabled_list'),
        sift('is_commutating'),
        'callcenter_phone_number',
        'phone_pd_id',
        'queue_code',
        'transfer_destination_code',
        'utc_commutation_end_dttm',
    ]

    result_df = (
        all_events_df
            .select(event_cols)
            .withColumn('next_event_type', f.lead('event_type').over(agent_window))
            .withColumn('prev_event_type', f.lag('event_type').over(agent_window))
            .withColumn('next_work_status_code', f.lead('work_status_code').over(agent_window))
            .withColumn('operator_state', f.expr('''
            CASE
            WHEN is_commutating = 'True' THEN 'talking'
            WHEN event_type = 'operator_status_log' OR event_type = 'commutation_finished' THEN
                CASE
                WHEN work_status_code = 'connected' THEN
                    CASE
                    -- В work_substatus_code лежит уточнение про postcall, например
                    WHEN work_substatus_code != '' THEN work_substatus_code
                    -- а когда ничего не лежит, значит оператор просто ждет
                    ELSE 'waiting'
                    END
                ELSE work_status_code
                END
            END
        '''))
            # Убираем события вида коннект-дисконнект во время звонков
            .filter(~((f.col('is_commutating') == f.lit('True')) & (f.col('call_id').isNull())))

            .withColumn('utc_next_event_dttm', f.lead('utc_event_dttm').over(agent_window))
            .withColumn(
            'state_dur_sec',
            f.col('utc_next_event_dttm').cast(t.DoubleType()) - f.col('utc_event_dttm').cast(t.DoubleType())
        )
            .filter(f.col('state_dur_sec') > 0)

            .drop('utc_answered_dttm')
            .withColumn('utc_event_dttm', f.date_format(f.col('utc_event_dttm'), SPARK_MILLISECONDS_FORMAT))
            .withColumn('utc_next_event_dttm', f.date_format(f.col('utc_next_event_dttm'), SPARK_MILLISECONDS_FORMAT))

            .withColumnRenamed('utc_event_dttm', 'utc_valid_from_dttm')
            .withColumnRenamed('utc_next_event_dttm', 'utc_valid_to_dttm')
            .withColumnRenamed('operator_state', 'operator_state_code')

            # Тестовые коммутации могут идти для agent_id, который не учитывается в бекенде КЦ как оператор
            # Для них не будет сущностей в operator_status_log, а значит, не будет work_status_code и login
            .filter(f.col('work_status_code').isNotNull())

            # Здесь я мог накосячить, и при нескольких прогонах могут дублироваться суррогатные состояния на границе
            # суток.
            # Найти проблемы при дебаге мне не удалось.
            # Если они и есть, то их вклад в пределах 0.05 * 3/86400 от общего сапплая.
            # Это можно будет заменить на обычный сессионный подход после https://st.yandex-team.ru/TAXIBACKEND-30166

            .filter(f.col('utc_valid_from_dttm') >= f.lit(period_start))
            .filter(f.col('utc_valid_from_dttm') < f.lit(period_end))
            .withColumn('agent_id', f.col('agent_id').cast(t.LongType()))
            .select(
            'agent_id',
            'utc_valid_from_dttm',
            'utc_valid_to_dttm',
            'state_dur_sec',
            'operator_state_code',
            'work_status_code',
            'work_substatus_code',
            'queue_code',
            'queues_enabled_list',
            'event_type',
            'event_id',
            'call_id',
            'staff_login',
            'callcenter_phone_number',
            'phone_pd_id',
            'transfer_destination_code',
        )
    )

    res = result_df

    res.write.mode("overwrite").optimize_for("scan").yt(output_path)
