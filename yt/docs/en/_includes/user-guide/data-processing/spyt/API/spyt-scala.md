# SPYT in Scala

## Requirements { #requirements }

SPYT works with Java 11 and Scala 2.12.

## Dependencies { #dependencies }

All the dependencies are installed in SPYT automatically; therefore the `scope` value is `Provided`. They will be used for compilation only and will not be included in the `.jar` or run.

All possible values for `spytVersion` can be found [here](https://github.com/ytsaurus/ytsaurus/tags) under the spyt/* or spyt-spark/* tag.

```scala

val sparkVersion = "3.2.2"
val spytVersion = "1.75.2"

libraryDependencies ++= Seq(
    // Spark dependencies
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    // SPYT library
    "tech.ytsaurus" %% "spark-yt-data-source" % spytVersion % Provided
)
```

## Build { #build }

The example code has been built using the `sbt assembly` command and posted to {{product-name}}: `//home/spark/examples/scala-examples-assembly-0.1.jar`.

## Starting jobs { #start }

Before running jobs from the example, you need to launch your SPYT cluster or find out the `discovery-path` for an already running cluster.

Jobs are started via the `spark-submit-yt` utility.

```bash
spark-submit-yt \
  --proxy ${YT_PROXY} \
  --discovery-path ${SPYT_DISCOVERY_PATH} \
  --deploy-mode cluster \
  --class tech.ytsaurus.spyt.examples.GroupingExample \
  yt:///home/spark/examples/scala-examples-assembly-0.1.jar

```


