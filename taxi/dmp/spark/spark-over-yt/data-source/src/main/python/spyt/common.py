from pyspark.sql.column import _to_java_column, Column
from pyspark.sql import DataFrame
from pyspark.sql.column import _to_java_column
from pyspark.sql.functions import col, lit, broadcast
from pyspark.sql import SparkSession


def join_with_hot_key_null(right, key, joinType, condition=lit(True)):
    def join(left):
        java_condition = _to_java_column(condition)
        return DataFrame(
            left._sc._jvm.ru.yandex.spark.yt.common.utils.DataFrameUtils.joinWithHotKeyNull(
                left._jdf, right._jdf, key, joinType, java_condition
            ),
            left.sql_ctx
        )
    return join


def get_top(schema, top_col_names, select_col_names):
    spark = SparkSession.builder.getOrCreate()
    jschema = spark._jsparkSession.parseDataType(schema.json())
    jc = spark._jvm.ru.yandex.spark.yt.common.utils.TopUdaf.top(jschema, top_col_names, select_col_names)
    return Column(jc)
