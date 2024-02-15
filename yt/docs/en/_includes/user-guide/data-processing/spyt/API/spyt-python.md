# SPYT in Python

## Steps to launch { #how-to }

1. Write some code.
2. Post your code and dependencies to {{product-name}}. The main `.py` file and the dependencies in `.py`, `.zip`, or `.egg`.
3. Build a binary file and post it to {{product-name}} (Spark 3.2.2+).
4. Run `spark-submit-yt` for submitting to inner standalone cluster or `spark-submit` for submitting directly to {{product-name}} (available from version 1.76.0)

## Differences for submitting directly to {{product-name}} { #submit }

All of the examples below are written for using with inner Spark standalone cluster. There's some differences for creating SparkSession object when submitting directly to {{product-name}}. Instead of using `with spark_session()` or `spyt.connect()` functions the object should be created explicitly according to Spark recommendations:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My Application').getOrCreate()

... # Application code

spark.stop()
```

## Running with no dependencies { #simple }

### Main file code

```python
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.read.yt("path_to_file").show()

```

### Launch

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my_discovery_path \
  --deploy-mode cluster \
  YT_path_to_file
```

## Running with dependencies { #dependencies }

### Main file code

```python
from my_lib import plus5
from pyspark.sql.functions import col
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.read.yt("path_to_file")\
	.withColumn("value", plus5(col("value")))\
	.write.mode("overwrite").yt("path_to_file")

```

### Dependency

`my_lib.zip` containing `my_lib.py`:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

plus5 = udf(lambda x: x + 5, LongType())

```

### Code and dependencies in {{product-name}}

To be posted to {{product-name}}:

- A file, such as `deps_example.py`.

- Dependencies, such as `my_lib.zip`.

### Launch

```bash
spark-submit-yt \
    --proxy <cluster-name> \
    --discovery-path //my_discovery_path \
    --deploy-mode cluster \
    --py-files YT_path_to_lib \
    YT_path_to_file

```
## Running with configurations

Spark includes many useful configurations that you can specify when running a job. Such as [spark.sql.shuffle.partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html). There is a way to control the amount of resources per job via `spark.cores.max` and `spark.executor.memory`. For more information, please see the [Spark documentation](https://spark.apache.org/docs/latest/configuration.html#application-properties).

{% note warning "Attention!" %}

In [Standalone](../../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) mode, the `num-executors` and the `spark.executor.instances` settings are non-functional while the number of executors depends on the `spark.cores.max` parameter.

{% endnote %}

### Main file code

```python
from pyspark_yt import spark_session
from pyspark.sql.functions import col
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.read.yt("path_to_file")\
	.withColumn("id_mod_10", col("id") % 10)\
	.groupBy("id_mod_10")\
	.count()\
	.write.mode("overwrite").yt("path_to_file")

```

### Code in {{product-name}}

You need to post this file to {{product-name}}.

### Launch

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --id test \
  --discovery-dir //my_discovery_path \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.cores.max=1 \
  --conf spark.executor.cores=1 \
  YT_path_to_file
```





## Other examples

You can find additional examples in [SPYT in Jupyter](../../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).

In regular jobs, you can create the `spark` object the same way as in Jupyter via a call to `connect`or, alternatively, via `with spark_session` as in the example. The difference between the options is minimal. Jupyter allows you to pass resource settings, while for regular jobs, you would normally do this at launch to avoid reposting code if, for instance, the amount of data had increased.

Example job start and output verification:

```python
import spyt
import time
from spyt.submit import java_gateway, SparkSubmissionClient, SubmissionStatus

user = "user_name"
token = spyt.utils.default_token()
yt_proxy = "cluster_name"
discovery_path = "//my_discovery_path"
spyt_version = "1.4.1"

with java_gateway() as gateway:
    client = SparkSubmissionClient(gateway, yt_proxy, discovery_path, spyt_version, user, token)
    launcher = (
        client
            .new_launcher()
            .set_app_resource("yt:///sys/spark/examples/smoke_test.py")
            .set_conf("spark.pyspark.python", "/opt/python3.7/bin/python3.7")
    )
    submission_id = client.submit(launcher)
    status = client.get_status(submission_id)
    while not SubmissionStatus.is_final(status):
        status = client.get_status(submission_id)
        time.sleep(10)
    SubmissionStatus.is_success(status)
    SubmissionStatus.is_failure(status)

```

1. The client operates via `Py4J` calling the required `RestSubmissionClient` methods. The string `with java_gateway() as gateway` will bring the `JVM` up with the right `Classpath` and exit without error.

2. You can use the `launch_gateway` and the `shutdown_gateway` methods to control the creation of a `JVM` manually.

3. `SparkSubmissionClient` is a client to submit jobs to a specific cluster. The client finds the location of the master with `discovery_path` and communicates with it.

4. The parameters of the job to be started are described in the `launcher` object. A complete listing of the object's methods is available in the code. They match the `spark-submit` parameters.

5. After launch, you can check job status using the `client.get_status` method. The example shows the timeout to get the final output.

