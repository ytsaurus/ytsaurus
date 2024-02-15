# SPYT в Python

##  Шаги для запуска { #how-to }

1. Напишите код.
2. Выложите код и зависимости в {{product-name}}. Основной файл `.py` и зависимости в `.py`, `.zip` или `.egg`.
3. Соберите испольняемый бинарный файл и выложите его в {{product-name}} (Spark 3.2.2+).
4. Запустите команду `spark-submit-yt` (для запуска во внутреннем standalone кластере) или `spark-submit` (для запуска напрямую в {{product-name}}, доступно с версии SPYT 1.76.0).

## Особенности запуска задач напрямую в {{product-name}} { #submit }

Все примеры, приведённые ниже, написаны для запуска во внутреннем standalone кластере. При запуске задач напрямую в {{product-name}} есть отличия в способе создания объекта SparkSession. Вместо использования функций `with spark_session()` или `spyt.connect()` нужно создавать его напрямую, согласно рекомендациям Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('My Application').getOrCreate()

... # Application code

spark.stop()
```

Это связано с тем, что функции `with spark_session()` и `spyt.connect()` обращаются к discovery-path, который создаётся в Кипарисе при запуске standalone кластера. В данном способе discovery-path не используется, так как внутренний кластер не создаётся.

## Запуск без зависимостей  { #simple }

### Код основного файла

```python
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.read.yt("path_to_file").show()

```

### Запуск

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my_discovery_path \
  --deploy-mode cluster \
  YTsaurus_path_to_file
```

## Запуск с зависимостями  { #dependencies }

### Код основного файла

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

### Зависимость

 Файл `my_lib.zip`, в котором находится `my_lib.py`:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

plus5 = udf(lambda x: x + 5, LongType())

```

### Код и зависимости в {{product-name}}

В {{product-name}} должны находиться:

- файл, например `deps_example.py`;

- зависимости, например `my_lib.zip`.

### Запуск

```bash
spark-submit-yt \
    --proxy <cluster-name> \
    --discovery-path //my_discovery_path \
    --deploy-mode cluster \
    --py-files YTsaurus_path_to_lib \
    YTsaurus_path_to_file

```
## Запуск с конфигурациями

В Spark много полезных конфигураций, которые можно указывать при запуске джоба. Например [spark.sql.shuffle.partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html). Существует возможность регулировать количество ресурсов на джоб через `spark.cores.max`, `spark.executor.memory`, подробнее можно узнать в [документации Spark](https://spark.apache.org/docs/latest/configuration.html#application-properties).

{% note warning "Внимание" %}

В режиме [Standalone](../../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) не работают настройки `num-executors` и `spark.executor.instances`, количество экзекьюторов определяется настройкой `spark.cores.max`.

{% endnote %}

### Код основного файла

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

  ### Код в {{product-name}}

 Файл необходимо загрузить в {{product-name}}.

  ### Запуск

  ```bash
spark-submit-yt \
    --proxy <cluster-name> \
    --id test \
    --discovery-dir //my_discovery_path \
    --deploy-mode cluster \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.cores.max=1 \
    --conf spark.executor.cores=1 \
    YTsaurus_path_to_file
  ```





## Другие примеры

Дополнительные примеры содержатся в разделе [SPYT в Jupyter](../../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).

В регулярных джобах объект `spark` можно создавать так же, как в Jupyter, с помощью вызова `connect`, а можно через `with spark_session`, как показано в примере. Разница между данными вариантами минимальна. В метод для Jupyter можно передавать настройки по ресурсам, а в регулярных джобах это обычно делается при запуске, чтобы не перевыкладывать код, если данных, например, стало больше.

Пример запуска и проверки результатов джоба:

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

1. Клиент работает через `Py4J`, вызывает нужные методы у `RestSubmissionClient`. Строка `with java_gateway() as gateway`  поднимает `JVM` с нужным `Classpath` и корректно завершает работу.

2. Можно использовать методы `launch_gateway` и `shutdown_gateway` и управлять созданием `JVM` самостоятельно.

3. `SparkSubmissionClient` — это клиент для запуска джобов на конкретном кластере. Клиент находит координаты мастера по `discovery_path` и взаимодействует с ним.

4. Параметры запускаемого джоба описываются в объекте `launcher`. Полный список методов объекта можно посмотреть в коде, методы соответствуют параметрам `spark-submit`.

5. Статус джоба после запуска можно проверять с помощью метода `client.get_status`. В примере указано время ожидания финального результата.

