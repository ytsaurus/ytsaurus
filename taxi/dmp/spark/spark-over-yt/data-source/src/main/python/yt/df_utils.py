from pyspark.sql import DataFrame


def get_df_top(df, group_by, top_by, partitions):
    return DataFrame(
        df._sc._jvm.ru.yandex.spark.yt.utils.DataFrameUtils.getDataFrameTop(df._jdf, group_by, top_by, partitions),
        df.sql_ctx
    )

