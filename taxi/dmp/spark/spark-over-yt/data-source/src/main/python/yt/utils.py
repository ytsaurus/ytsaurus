from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import json


def restart_spark(spark):
    conf = spark._sc._conf
    scala_conf = spark._jsparkSession.conf()
    spark.stop()
    scala_spark = spark._jvm.ru.yandex.spark.yt.PythonUtils.startSparkWithExtensions(scala_conf)
    sc = SparkContext(conf=conf, jsc=spark._jvm.JavaSparkContext(scala_spark.sparkContext()))
    spark = SparkSession(sc, scala_spark)
    sqlContext = SQLContext(sc, spark, scala_spark.sqlContext)
    return spark, sqlContext, sc


def set_path_files_count(spark, path, files_count):
    spark._jvm.ru.yandex.spark.yt.PythonUtils.setPathFilesCount(path, files_count)


def string_type(type):
    if type == StringType():
        return "string"
    if type == IntegerType():
        return "int64"
    if type == LongType():
        return "int64"
    if type == DoubleType():
        return "double"
    if type == BooleanType():
        return "boolean"
    if isinstance(type, ArrayType):
        return "a#" + string_type(type.elementType)
    if isinstance(type, StructType):
        return "s#" + json.dumps([(f.name, string_type(f.dataType)) for f in type.fields])
    if isinstance(type, MapType):
        return "m#" + string_type(type.valuetype)
