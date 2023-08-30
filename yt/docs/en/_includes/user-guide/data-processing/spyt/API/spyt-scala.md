# SPYT in Scala

## Requirements { #requirements }

SPYT works with Java 11 and Scala 2.12.

## Dependencies { #dependencies }

All the dependencies are installed in SPYT automatically; therefore the `scope` value is `Provided`. They will be used for compilation only and will not be included in the `.jar` or run.

```java
libraryDependencies ++= Seq(
    // Spark dependencies
    "org.apache.spark" %% "spark-core" % "3.0.1" % Provided,
    "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided,
    // SPYT library
    "tech.ytsaurus" %% "spark-yt-data-source" % "1.50.0" % Provided
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


