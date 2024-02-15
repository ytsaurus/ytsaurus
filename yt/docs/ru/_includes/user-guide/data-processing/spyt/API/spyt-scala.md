# SPYT в Scala

## Требования { #requirements }

SPYT работает с Java 11 и Scala 2.12.

## Зависимости { #dependencies }

Все зависимости ставятся в SPYT автоматически, поэтому  значение для `scope` – `Provided`. Они будут использоваться только для компиляции и не будут включены в `.jar` для запуска.

Все допустимые значения для `spytVersion` можно найти [здесь](https://github.com/ytsaurus/ytsaurus/tags) под тегами spyt/* или spyt-spark/*.

```scala

val sparkVersion = "3.2.2"
val spytVersion = "1.76.1"

libraryDependencies ++= Seq(
    // зависимости от Spark
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    // библиотека SPYT
    "tech.ytsaurus" %% "spark-yt-data-source" % spytVersion % Provided
)
```

## Сборка { #build }

Код с примерами собран командой `sbt assembly`  и выложен в {{product-name}}: `//home/spark/examples/scala-examples-assembly-0.1.jar`.

## Запуск джобов { #start }

Перед запуском джобов из примера необходимо запустить кластер SPYT или узнать `discovery-path` уже запущенного кластера.

Запуск джобов производится с помощью утилиты `spark-submit-yt`.

```bash
spark-submit-yt \
  --proxy ${YT_PROXY} \
  --discovery-path ${SPYT_DISCOVERY_PATH} \
  --deploy-mode cluster \
  --class tech.ytsaurus.spyt.examples.GroupingExample \
  yt:///home/spark/examples/scala-examples-assembly-0.1.jar

```

## Особенности запуска задач напрямую в {{product-name}} (с версии 1.76.0) { #submit }

В случае использования [запуска задач с использованием планировщика {{product-name}}](../../../../../user-guide/data-processing/spyt/launch.md#submit), для создания `SparkSession` нужно воспользоваться стандартными рекомендациями Spark:

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MySparkApplication {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val spark = SparkSession.builder.config(conf).getOrCreate()

        try {
            // Application code
        } finally {
            spark.stop()
        }
    }
}
```

В этом случае запуск выполняется при помощи команды `spark-submit`:

```bash
$ spark-submit \
  --master ytsaurus://${YT_PROXY} \
  --deploy-mode cluster \
  --class tech.ytsaurus.spyt.examples.GroupingExample \
  yt:///home/spark/examples/scala-examples-assembly-0.1.jar

```


