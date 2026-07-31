# Импорт данных из Hive, S3, MongoDB и других систем

Импорт данных из внешних систем в {{product-name}} реализован через SPYT.

На данной странице приведена инструкция по импорту данных с помощью скрипта [import.py](https://github.com/ytsaurus/ytsaurus/blob/main/connectors/import.py). Этот скрипт позволяет импортировать данные из Hive, Hadoop, S3, а также из СУБД, поддерживающих протокол JDBC. Для импорта из других систем хранения данных &mdash; например, из MongoDB &mdash; вы можете использовать SPYT напрямую, обратившись к этой системе с помощью соответствующего Spark Datasource.


## Настройка подключения через Kerberos

Для работы с платформой Hadoop может использоваться протокол сетевой аутентификации Kerberos. Для настройки взаимодействия необходимо:

1. Создать на стороне Hadoop токен следующей командой:
```bash
$ fetchdt --renewer null /tmp/token
```
2. Скопировать токен в Кипарис:
```bash
$ cat /tmp/token | yt write-file //home/spark/conf/token
```
3. Задать при запуске приложения путь к токену в Кипарисе с помощью опции `--files`

#### Клиентский режим

При запуске в режиме `--deploy-mode client` для драйвера необходимо указать путь к локальному 
файлу с токеном, так как процесс драйвера запускается не в кластере {{product-name}}. 
Для экзекьюторов указывается параметр `HADOOP_TOKEN_FILE_LOCATION=token````bash
```bash
$ spark-submit \
  --master ytsaurus:// \
  --deploy-mode client \
  --files yt://home/spark/conf/token \
  --conf spark.executorEnv.HADOOP_TOKEN_FILE_LOCATION=/tmp/token \
 ./script.py
```
#### Кластерный режим

Для запуска в режиме `--deploy-mode cluster` путь к файлу с токеном указывается через конфигурационный параметр `spark.ytsaurus.config.global.path`. 

Дополнительно для драйвера необходимо добавить параметры `kdc` и `realm`. Пример команды:

```bash
$ spark-submit \
  --master ytsaurus:// \
  --deploy-mode cluster \
  --files yt://home/spark/conf/token \
  --conf spark.ytsaurus.config.global.path=//home/spark/conf/global \
  --conf spark.driver.extraJavaOption="-Djava.security.krb5.kdc=... -Djava.secutirykrb5.realm=..."  \
 ./script.py
```

## Установите зависимости { #dependencies }

jar-зависимости для чтения из Hive предоставляются вместе с пакетом `pyspark`. Пакеты для работы с SPYT (включая `pyspark`), должны быть установлены на системе, из которой вызывается импорт.

Для чтения из СУБД, которая поддерживает JDBC-протокол, нужно скачать JDBC-драйвер этой СУБД.

Файл `connectors/pom.xml` содержит конфиг для Maven, в котором указаны JDBC-драйвера для MySQL и PostgreSQL. Эти драйвера можно скачать следующим образом:

```bash
~/yt/connectors$ mvn dependency:copy-dependencies
```

`mvn` скачает jar-файлы с драйверами для PostgreSQL и MySQL в директорию `target/dependency`.

Если необходимо импортировать данные из другой СУБД, добавьте JDBC-драйвер этой СУБД в качестве зависимости в `pom.xml`, и запустите команду `$ mvn dependency:copy-dependencies`

{% note warning "JDBC-драйвер должен быть виден процессу-драйверу" %}

JDBC-драйвер должен быть доступен не только экзекьюторам (через `--jars`), но и на classpath **процесса-драйвера**. В режиме `--deploy-mode client` Spark не подхватывает драйвер из `--jars` на стороне драйвера автоматически, и чтение падает с `java.sql.SQLException: No suitable driver`. Положите jar в каталог `jars/` установленного `pyspark` (`$SPARK_HOME/jars/`) либо задайте `export SPARK_DIST_CLASSPATH=/path/to/driver.jar` перед запуском.

{% endnote %}

## Запустите импорт данных { #how-to-import }

Для импорта данных следует запустить скрипт `import.py`:

```bash
$ ./import.py \
   # Опции импорта.
   # ...
```

Опции должны идентифицировать:
- источник данных;
- путь к импортируемым данным;
- путь в {{product-name}}, куда должны быть записаны импортируемые данные.

Список всех опций приведён в конце этого раздела, в секции [Опции](#script-options). Ниже приведены примеры импорта данных из разных систем.

### Клиентский и кластерный режим { #run-modes }

`import.py` можно запускать в двух режимах:

- **Клиентский (по умолчанию)** — `python import.py ...`: драйвер Spark стартует локально на машине, откуда запущен скрипт, а экзекьюторы — в кластере {{product-name}}.
- **Кластерный** — `import.py` запускается как Spark-приложение через `spark-submit ... --deploy-mode cluster`, при этом драйвер тоже стартует внутри кластера {{product-name}}. Режим определяется автоматически (по факту запуска через `spark-submit`), отдельный флаг не нужен:

  ```bash
  $ spark-submit \
      --master ytsaurus://<proxy> \
      --deploy-mode cluster \
      --jars yt:///path/to/jars/driver.jar \
      import.py \
      --jdbc postgresql --jdbc-server pg_host:5432 --jdbc-user user --jdbc-password '<password>' \
      --input jdbc:database_name.table_name \
      --output //path/in/yt/table
  ```

Кластерный режим удобен при сабмите с **внешней машины** (например, вне Kubernetes-кластера, в котором развёрнут {{product-name}}): драйвер уходит внутрь кластера, и не требуется сетевая доступность драйвер↔экзекьюторы с внешней машины.

#### Сетевое взаимодействие внутри Kubernetes { #cluster-proxy }

Если сабмит идёт с внешней машины, а {{product-name}} развёрнут в Kubernetes, снаружи обычно доступен только внешний прокси, а внутрикластерный трафик (драйвер и экзекьюторы ↔ {{product-name}}) должен идти через внутренний прокси. Чтобы это обеспечить, задайте параметр `spark.hadoop.yt.clusterProxy` — адрес прокси, доступного изнутри кластера; при этом `--master` указывает на внешний адрес, по которому выполняется сабмит:

```bash
$ spark-submit \
    --master ytsaurus://<внешний proxy> \
    --deploy-mode cluster \
    --conf spark.hadoop.yt.clusterProxy=<внутренний proxy в k8s> \
    import.py ...
```

В клиентском режиме тот же параметр передаётся через `--extra-conf spark.hadoop.yt.clusterProxy=...`.

### Hive

Чтобы импортировать данные из Hive:

1. Загрузите на Кипарис файлы `hadoop-aws-*.jar` и `aws-java-sdk-bundle-*.jar` из локальной директории `target/dependency`.
2. Запустите импорт данных следующей командой:
   ```bash
   $ ./import.py \
     --metastore master_host:9083 \
     --warehouse-dir /path/to/hive/warehouse \
     --input hive:database_name.table_name \
     --output //path/in/yt/table \
     --proxy https://ytsaurus.company.net \
     --num-executors 5 \
     --executor-memory 4G \
     --executor-memory-overhead 2G \
     --jars yt:///path/to/jars/aws-java-sdk-bundle-1.11.901.jar \
            yt:///path/to/jars/hadoop-aws-3.3.1.jar
   ```
   [Перейти к описанию опций](#script-options).

Альтернативно, можно передать в Hive SQL-запрос для исполнения и импортировать результат этого запроса. Для этого используйте префикс `hive_sql` для описания исходных данных:

```bash
$ ./import.py \
    ...
    --input hive_sql:database_name:SELECT * FROM action_log WHERE action_date > '2023-01-01' \
    ...
```

### HDFS

Для импорта файла из HDFS используйте спецификатор с указанием формата этого файла, а также адрес HDFS NameNode:

```bash
$ ./import.py \
    ...
    --input text:hdfs://namenode/path/to/text/file
    ...
```

`import.py` также поддерживает форматы parquet, orc.

### СУБД, поддерживающие JDBC-протокол

Чтобы импортировать данные из JDBC-совместимых СУБД (например, из PostgreSQL), запустите команду:
```bash
$ ./import.py \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \  # Получить пароль из консольного ввода
    --input jdbc:database_name.table_name \
    --output //path/in/yt/table
```
[Перейти к описанию опций](#script-options).

Чтобы передать в СУБД запрос для исполнения и импортировать результат, можно использовать префикс `jdbc_sql`:

```bash
$ ./import.py \
    ...
    --input jdbc_sql:database_name:SELECT * FROM users WHERE signup_date > '2023-01-01' \
    ...
```

#### Параллельное чтение больших таблиц

По умолчанию `import.py` читает JDBC-источник в один поток (одним запросом). Для больших таблиц это медленно и может приводить к ошибкам долгих запросов на стороне СУБД (например, `ORA-01555: snapshot too old` в Oracle). Чтобы читать таблицу параллельно, задайте числовую или дата-колонку разбиения и её диапазон:

```bash
$ ./import.py \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \
    --input jdbc:database_name.table_name \
    --jdbc-partition-column id \
    --jdbc-lower-bound 1 \
    --jdbc-upper-bound 1000000 \
    --jdbc-num-partitions 16 \
    --output //path/in/yt/table
```

SPYT разобьёт чтение на `--jdbc-num-partitions` диапазонов вида `column >= a AND column < b` и прочитает их параллельно. Границы `--jdbc-lower-bound`/`--jdbc-upper-bound` задают только шаг разбиения — данные они не фильтруют (значения за пределами попадают в крайние партиции). Тип колонки должен быть числовым или датой; желательно проиндексированной или являющейся ключом секционирования таблицы, чтобы каждый запрос читал только свою часть.

Опции партиционирования глобальные — они применяются **одинаково ко всем** таблицам, перечисленным через `--input`. Если за один запуск импортируется несколько таблиц, колонка разбиения и границы должны подходить каждой из них; иначе запускайте `import.py` отдельно под каждую таблицу.

### S3

Чтобы импортировать данные из S3:

1. Загрузите на Кипарис файлы `hadoop-aws-*.jar` и `aws-java-sdk-bundle-*.jar` из локальной директории `target/dependency`.
2. Запустите импорт данных следующей командой:

   ```bash
   ./import.py \
    --input parquet:s3a://bucket/path/to/data/sample-parquet \
    --output //home/tables/import_from_s3 \
    --s3-access-key <S3 Access Key> \
    --s3-secret-key <S3 Secret Key> \
    --s3-endpoint <S3 endpoint> \
    --proxy https://ytsaurus.company.net \
    --num-executors 5 \
    --executor-memory 4G \
    --executor-memory-overhead 2G \
    --jars yt:///path/to/jars/aws-java-sdk-bundle-1.11.901.jar \
         yt:///path/to/jars/hadoop-aws-3.3.1.jar
   ```
   Описание опций приведено ниже.

## Опции { #script-options }

`import.py` поддерживает следующие опции:

| **Опция** | **Описание** |
| ----------| --------- |
| `--num-executors` | Число экзекьютеров для операции импорта (по умолчанию 1) |
| `--cores-per-executor` | Резервация для числа CPU-ядер на экзекьютер (по умолчанию 1) |
| `--ram-per-core` | RAM-резервация на каждое ядро (по умолчанию 2GB) |
| `--jdbc` | Тип JDBC-драйвера для СУБД. Например `mysql` или `postgresql` |
| `--jdbc-server` | host:port сервера СУБД |
| `--jdbc-user` | Имя пользователя в СУБД |
| `--jdbc-password` | Пароль для СУБД. Если передан пустой флаг, получить из консоли |
| `--jdbc-partition-column` | Числовая или дата-колонка для параллельного чтения источника (Spark `partitionColumn`). Требует `--jdbc-lower-bound`, `--jdbc-upper-bound`, `--jdbc-num-partitions` |
| `--jdbc-lower-bound` | Нижняя граница диапазона `--jdbc-partition-column` |
| `--jdbc-upper-bound` | Верхняя граница диапазона `--jdbc-partition-column` |
| `--jdbc-num-partitions` | Число параллельных партиций (JDBC-соединений) при чтении |
| `--jars` | Дополнительные JAR-библиотеки. По умолчанию, `target/dependency/jar/*.jar` |
| `--input` | Объект для импорта, можно указывать несколько раз |
| `--output` | Путь для записи в {{product-name}}. Должно быть указано одно значение на каждое значение `--input` |

Если операция импорта данных предусматривает запуск кластера SPYT, следующие опции командной строки могут использоваться для конфигурации этого кластера:

| **Опция** | **Описание** |
| ----------| --------- |
| `--proxy` | Адрес прокси в кластере {{product-name}}, где будет работать SPYT |
| `--pool` | Пул ресурсов в {{product-name}} для кластера SPYT |
| `--executor-timeout` | Таймаут для Spark-экзекьютеров |
| `--executor-tmpfs-limit` | Размер tmpfs-партиции для Spark экзекьютеров |
| `--executor-memory` | Объём оперативной памяти, выделяемой каждому экзекьютеру    |
  `--executor-memory-overhead`  | Дополнительный объём памяти, который выделяется экзекьютерам сверх основной памяти. Например, если основная память 4&nbsp;ГБ и дополнительная 2&nbsp;ГБ, то общий объём памяти, запрашиваемый у кластера, будет 6&nbsp;ГБ  |

В случае импорта данных с S3:

| **Опция** | **Описание** |
| ----------| --------- |
| `--s3-access-key` | Ключ доступа (Access Key ID), который идентифицирует пользователя или приложение в S3 |
| `--s3-secret-key` | Секретный ключ (Secret Access Key), связанный с ключом доступа |
| `--s3-endpoint` | URL-адрес (endpoint) S3-хранилища |

Поддерживаются следующие спецификаторы для импортируемых данных:

| **Спецификатор** | **Описание** |
| ----------| --------- |
| `hive` | Таблица в Hive, вида `db_name.table_name` |
| `hive_sql` | SQL-запрос над Hive, вида `db_name:sql statement` |
| `jdbc` | Таблица в СУБД, вида `db_name.table_name` |
| `jdbc_sql` | SQL-запрос для СУБД, вида `db_name:sql statement' |
| `text` | Текстовый файл в HDFS |
| `parquet` | parquet-файл в HDFS |
| `orc` | orc-файл из в HDFS |

По умолчанию при записи таблицы в {{product-name}} подразумевается, что таблица не существует. Если таблица уже существует, можно её перезаписать или дополнить, используя спецификатор `overwrite` или `append`. Например: `--output overwrite:/path/to/yt`

## Преобразования типов

Импорт сложных типов из внешних систем может поддерживаться лишь частично. Система типов {{product-name}} не имеет взаимно однозначного соответствия с системами типов других систем хранения данных. При импорте SPYT сохранит тип по мере возможности, и преобразует значение в строку в тех случаях, когда не удалось вывести правильный тип. При необходимости используйте SQL для корректного преобразования типов.

Диапазоны значений одного типа могут оказаться разными. Например, {{product-name}} `date` не хранит даты ранее Юникс-эпохи, 1 января 1970 года. Попытка записать в {{product-name}} более ранние даты приведёт к ошибке. Даты раньше Юникс-эпохи в {{product-name}} можно хранить как строки (например, используя преобразование `to_char(date_value, 'YYYY-MM-DD')` в PostgreSQL), или как целые числа (`date_value - '1970-01-01'` в PostgreSQL).

