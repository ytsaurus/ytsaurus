from spyt import spark_session

with spark_session() as spark:
    df = spark.read.yt('//tmp/t_in')
    df.write.yt('//tmp/t_out')
