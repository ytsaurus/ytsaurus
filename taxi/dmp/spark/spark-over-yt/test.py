from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    for i in range(1000):
        spark.read.yt("//sys/spark/examples/test_data").show()

