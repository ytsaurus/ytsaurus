# SPYT в Java

## Настройка проекта, зависимости, сборка { #prepare }

Все примеры в данном разделе собраны с помощью `maven`. См. [POM файл](https://github.com/ytsaurus/ytsaurus/tree/main/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example).

- Все зависимости, которые нужны для запуска джобов в кластере, указываются в Vanilla-операции при старте кластера. Поэтому в приведённом примере все зависимости добавлены в `scope` – `Provided`. Зависимости будут использоваться только для компиляции и не будут включены в `.jar` для запуска.
- с момента выхода Spark 3.0 проект использует 11 версию Java;
- код собран командой `mvn clean compile assembly:assembly` и выложен в {{product-name}}: `//sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar`. Код нужно обязательно выкладывать в {{product-name}}, Spark не распределяет пользовательские файлы по [воркерам](../../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode).


## Запуск примеров { #run-examples }

Каждый пример состоит из одного класса, который унаследован от `SparkAppJava`.`SparkJavaApp` инициализирует Spark и правильно завершает работу. Данный способ нужно использовать в случае работы с [внутренним standalone Spark кластером](../../../../../user-guide/data-processing/spyt/launch.md#standalone). В случае, если предполагается использовать [запуск задач с использованием планировщика {{product-name}}](../../../../../user-guide/data-processing/spyt/launch.md#submit), для создания `SparkSession` нужно воспользоваться стандартными рекомендациями Spark:

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

## Чтение данных из {{product-name}} { #read-data }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/SmokeTest.java' lang='java' %}

<!--  Just in case: См. [примеры](https://github.com/ytsaurus/ytsaurus/tree/main/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example).-->

1. Класс джоба унаследован от `SparkAppJava`. Вспомогательный метод метод `run` инициализирует `SparkSession` и вызывает абстрактный метод `doRun`. В методе `main` данный метод вызывается у инстанса.

2. `SparkSession` — точка входа, через которую происходит взаимодействие с фреймворком. Самый часто используемый метод — `read`, позволяющий читать данные из {{product-name}}.

3. `spark.read().format("yt").load("/sys/spark/examples/test_data")` — чтение данных в формате `yt` по пути `//sys/spark/examples/test_data`.

Результатом чтения является объект `Dataset`, в Scala для него существует алиас `DataFrame`. Датафреймы — это внутреннее представление схематизированных таблиц в Spark. Их можно фильтровать, джойнить между собой, добавлять новые колонки. После применения таких операций к датафрейму получается новый датафрейм и его можно использовать в дальнейшей работе. Датафрейм состоит из нескольких партиций, каждая из которых может обрабатываться отдельно. У датафрейма, который прочитан из {{product-name}}, столько же партиций, сколько [чанков](../../../../../user-guide/storage/chunks.md) было у таблицы в {{product-name}}.

В приведённом примере у датафрейма вызывается метод `show()`. В результате в `stdout` выводится несколько первых строк датафрейма (по умолчанию 20). Результат в логах будет выглядеть следующим образом:

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

Запуск осуществляется командой `spark-submit-yt`. В аргументах необходимо указать координаты кластера Spark, `main class` и запускаемый файл `.jar`:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.SmokeTest \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```


## Использование UDF { #use-udf }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/UdfExample.java' lang='java' %}

1. Класс джоба унаследован от `SparkAppJava`,  как в предыдущем примере.
2. Чтение датафрейма из `//sys/spark/examples/example_1`.

3. Для парсинга `uuid` и получения подстроки, необходимо написать UDF:

   - Создать лямбда-функцию для парсинга: `(String s) -> s.split("-")[1]`.
   - Завернуть её в объект с помощью `functions.udf`.
   - Сообщить Spark тип выходного значения `DataTypes.StringType`.

4. Фильтрация датафрейма по колонке `id > 5`.
5. Применение `udf` к колонке `uuid`, новая колонка называется `value`.
6. Сохранение результата в {{product-name}}. По умолчанию Spark не перезаписывает существующие таблицы, а выкидывает исключение. Для перезаписи данных используйте `.mode(SaveMode.Overwrite)`.

Результат: `//sys/spark/examples/example_1_map`.

Запуск осуществляется командой `spark-submit-yt`:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.UdfExample \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```


## Агрегации { #agg }

{% code '/yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/GroupingExample.java' lang='java' %}

1. Класс джоба унаследован от `SparkAppJava`, как в предыдущем примере.
2. Чтение датафрейма из `//sys/spark/examples/example_1`.
3. Чтение датафрейма из `//sys/spark/examples/example_dict`.
4. Джойн по колонке `uuid`. В данном примере Spark оценит размер правого датафрейма и решит, что можно сделать `map-side join`. В терминологии Spark такая ситуация называется `broadcast`.
5. Группировка результата по `count`, найден максимальный `id` для каждого `count`. Для группировки Spark сделает перетасовку (shuffle) датафрейма, чтобы записи с одинаковыми ключами оказались в одной партиции. Количество партиций после перетасовки определяется параметром `spark.sql.shuffle.partitions`. По умолчанию число партиций равно `200`, значение можно изменить при запуске джоба.

6. Сохранение результата в {{product-name}}. Количество чанков в сохраненной таблице будет равно количеству партиций у записываемого датафрейма. Партиций образовалось много (200), так как была сделана группировка. Для того чтобы при записи не порождать много мелких чанков, перед записью они собираются в одну партицию с помощью `repartition(1)`

Результат: `//sys/spark/examples/example_1_agg`.

Запуск осуществляется командой `spark-submit-yt`. Дополнительно указывается `spark.sql.shuffle.partitions`:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path //my/discovery/path \
  --conf spark.sql.shuffle.partitions=8 \
  --deploy-mode cluster \
  --class tech.ytsaurus.spark.example.GroupingExample \
  yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar
```


## Другие примеры


[Пример](https://github.com/ytsaurus/ytsaurus/blob/main/yt/spark/spark-over-yt/java-submit-example/src/main/java/tech/ytsaurus/spyt/example/submit/SubmitExample.java) запуска джоба и проверки выходных данных.

{% note warning "Внимание" %}

В зависимости необходимо добавить клиент `spark-yt-submit-client_2.12`. В свою очередь у клиента есть зависимости на классы Spark классы в `runtime`. Не нужно добавлять данные зависимости в сборку джобов, которые запускаются в кластере. Зависимость на Spark должна быть в значении `Provided`, поскольку классы Spark уже присутствуют в кластере. Данная зависимость используется только для запуска джобов.

{% endnote %}

1. `SubmissionClient` — клиент для отправки джобов на конкретный кластер. Клиент находит координаты мастера по `discovery_path` и взаимодействует с ним.
2. Параметры запускаемого джоба описываются в объекте `launcher`. Полный список методов объекта содержится в исходном коде. Методы соответствуют параметрам `spark-submit`.
3. После отправки джоба на кластер можно проверять его статус при помощи метода `client.getStatus`. В примере указано время ожидания финального результата, но это необязательно.
