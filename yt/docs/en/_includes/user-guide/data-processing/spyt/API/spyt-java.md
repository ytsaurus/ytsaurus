# SPYT in Java

## Project configuration, dependencies, building { #prepare }

All the examples in this section have been collected with `maven`.

- All the dependencies required to run jobs in a cluster are specified in the Vanilla operation at cluster startup. That is why the example shows all the dependencies as having been added in the `Provided` `scope`. They will be used for compilation only and will not be included in the `.jar` or run.
- Since the release of Spark 3.0, the project has been using Java 11.
- The code has been built using the `mvn clean compile assembly:assembly` command and posted to {{product-name}}: `//sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar`. You must always upload your code to {{product-name}} as Spark does not distribute user files to [workers](../../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode).


## Running examples { #run-examples }

Each of the examples includes a single class inherited from `SparkAppJava`.`SparkJavaApp` initializes Sparks and exits without errors. This method is suitable for working with [inner Spark standalone cluster](../../../../../user-guide/data-processing/spyt/launch.md#standalone). For launching Spark applications using[{{product-name}} scheduler](../../../../../user-guide/data-processing/spyt/launch.md#submit) you should refer to standard Spark recommendations for creating `SparkSession`:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

class MySparkApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        SparkSession spark = SparkSession.builder.config(conf).getOrCreate();

        try {
            // Application code
        } finally {
            spark.stop();
        }
    }
}
```

## Reading data from {{product-name}} { #read-data }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/SmokeTest.java' lang='java' %}

1. The job class is inherited from `SparkAppJava`. The auxiliary `run` method initializes `SparkSession` and calls the `doRun` abstract method. In `main`, we call this method from inside an instance.

2. `SparkSession` is the entry point used to communicate with the framework. The most frequently called method is `read` that reads data from {{product-name}}.

3. `spark.read().format("yt").load("/sys/spark/examples/test_data")`: Read data in `yt` format from `//sys/spark/examples/test_data`.

The read operation returns a `Dataset` object aliased as `DataFrame` in Scala. DataFrames are Spark's internal representations of schematized tables. You can filter and join these, as well as add new columns. By applying operations like these to a DataFrame, you create a new DataFrame for subsequent use. A DataFrame includes several partitions that you can process individually. A DataFrame read from {{product-name}} has as many partitions as the {{product-name}} table had [chunks](../../../../../user-guide/storage/chunks.md) .

The example here calls the DataFrame's `show()` method. The output is the first few rows of the DataFrame (20 by default) displayed to `stdout`. In the logs, the output will look like this:

```
+-----+
|value|
+-----+
|    1|
|    2|
|    3|
|    4|
|    5|
|    6|
|    7|
|    8|
|    9|
|   10|
+-----+
```

Start is accomplished with the `spark-submit-yt` command. The arguments must include Spark cluster location information, the `main class`, and the `.jar` executable:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.SmokeTest \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```


## Using UDF { #use-udf }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/UdfExample.java' lang='java' %}

1. The job class is inherited from `SparkAppJava` as in the previous example.
2. Read a DataFrame from `//sys/spark/examples/example_1`.

3. You need to write a UDF to parse the `uuid` and obtain a substring.

   - Create a parsing lambda function: `(String s) -> s.split("-")[1]`.
   - Place it in an object wrapper using `functions.udf`.
   - Tell Spark that its output value is of type `DataTypes.StringType`.

4. Filter a DataFrame on column `id > 5`.
5. Apply the `udf` to the `uuid` column and call the new column `value`.
6. Save the output to {{product-name}}. By default, Spark does not overwrite existing tables but throws an exception instead. To overwrite data, use `.mode(SaveMode.Overwrite)`.

Output: `//sys/spark/examples/example_1_map`.

Start is accomplished with the `spark-submit-yt` command:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.UdfExample \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```


## Aggregations { #agg }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/GroupingExample.java' lang='java' %}

1. The job class is inherited from `SparkAppJava` as in the previous example.
2. Read a DataFrame from `//sys/spark/examples/example_1`.
3. Read a DataFrame from `//sys/spark/examples/example_dict`.
4. Join on the `uuid` column. In this example, Spark will evaluate the size of the right DataFrame and decide that a `map-side join` is feasible. In Spark terminology, this situation is called a `broadcast`.
5. Group the output by `count` to find the greatest `id` for each `count`. For groupings, Spark re-shuffles a DataFrame to place records with identical keys in the same partition. The number of partitions following a shuffle is defined by `spark.sql.shuffle.partitions`. By default, there are `200` partitions. You can change this value when you start a job.

6. Save the output to {{product-name}}. The saved table will have as many chunks as the DataFrame being written had partitions. There were many partitions (200) because of the grouping that had been made. To avoid creating many small chunks in a write, aggregate them into a single partition with `repartition(1)` before a write.

Output: `//sys/spark/examples/example_1_agg`.

Start is accomplished with the `spark-submit-yt` command. You also need to specify `spark.sql.shuffle.partitions`:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --conf spark.sql.shuffle.partitions=8 \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.GroupingExample \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```

## Other examples

[Example](https://github.com/ytsaurus/ytsaurus/blob/main/yt/spark/spark-over-yt/java-submit-example/src/main/java/tech/ytsaurus/spyt/example/submit/SubmitExample.java) job start and output verification.



{% note warning "Attention!" %}

You need to add `spark-yt-submit-client_2.12` to the dependencies. The client, in turn, has `runtime` dependencies on Spark classes. You do not need to add these dependencies to the build for jobs that run in the cluster. The Spark dependency must have the `Provided` value since the Spark classes are already available in the cluster. This dependency is only used to run jobs.

{% endnote %}

1. `SubmissionClient` is a client to submit jobs to a specific cluster. The client finds the location of the master with `discovery_path` and communicates with it.
2. The parameters of the job to be started are described in the `launcher` object. See the source code for a complete listing of the object's methods. The methods match the `spark-submit` parameters.
3. Once you have submitted a job to a cluster, you can check its status with the `client.getStatus` method. The example shows the timeout to get the final output but it is optional.
