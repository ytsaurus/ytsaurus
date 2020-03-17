from pyspark import SparkContext
from pyspark.sql.column import _to_java_column, Column
import pyspark.sql.functions as F


def contains(s_col, sub):
    sc = SparkContext._active_spark_context
    cols = sc._gateway.new_array(sc._jvm.Column, 2)
    cols[0] = _to_java_column(s_col)
    cols[1] = _to_java_column(F.lit(sub))
    jc = sc._jvm.ru.yandex.spark.example.UdfToPythonExample.contains.apply(cols)
    return Column(jc)


def parse_string(s_col):
    sc = SparkContext._active_spark_context
    cols = sc._gateway.new_array(sc._jvm.Column, 1)
    cols[0] = _to_java_column(s_col)
    jc = sc._jvm.ru.yandex.spark.example.UdfToPythonExample.parseString.apply(cols)
    return Column(jc)
