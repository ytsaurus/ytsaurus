# SPYT в Scala

## Требования { #requirements }

SPYT работает с Java 11 и Scala 2.12.

## Зависимости { #dependencies }

Все зависимости ставятся в SPYT автоматически, поэтому  значение для `scope` – `Provided`. Они будут использоваться только для компиляции и не будут включены в `.jar` для запуска.

Все допустимые значения для `spytVersion` можно найти [здесь](https://github.com/ytsaurus/ytsaurus/tags) под тегами spyt/* или spyt-spark/*.

```scala

val sparkVersion = "3.2.2"
val spytVersion = "1.75.2"

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


