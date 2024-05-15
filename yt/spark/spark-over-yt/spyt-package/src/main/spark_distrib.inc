DEFAULT(SPARK_VERSION 3.2.2)
MESSAGE(Compiling SPYT using $SPARK_VERSION Spark version)

SET(HADOOP_VERSION 3)

IF (SPARK_VERSION == "3.2.2")
    SET(SPARK_SANDBOX 6299738196)
    SET(HADOOP_VERSION 3.2)
#ELSEIF (SPARK_VERSION == "3.2.3")
#    SET(SPARK_SANDBOX {insert version here})
ELSE()
    MESSAGE(FATAL_ERROR Spark $SPARK_VERSION is not supported)
ENDIF()

FROM_SANDBOX(
    ${SPARK_SANDBOX}
    OUT_NOAUTO
    spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
)

RESOURCE_FILES(
    PREFIX yt/spark/spark-over-yt/spyt-package/src/main/
    spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
)
