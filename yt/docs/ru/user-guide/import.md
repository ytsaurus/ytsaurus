# Импорт данных из Hive, S3, MongoDB и других систем

Импорт данных из внешних систем в {{product-name}} реализован через SPYT.

На данной странице приведена инструкция по импорту данных с помощью скрипта [import.py](https://github.com/ytsaurus/ytsaurus/blob/main/connectors/import.py). Этот скрипт позволяет импортировать данные из Hive, Hadoop, S3, а также из СУБД, поддерживающих протокол JDBC. Для импорта из других систем хранения данных &mdash; например, из MongoDB &mdash; вы можете использовать SPYT напрямую, обратившись к этой системе с помощью соответствующего Spark Datasource.

## Установите зависимости { #dependencies }

jar-зависимости для чтения из Hive предоставляются вместе с пакетом `pyspark`. Пакеты для работы с SPYT (включая `pyspark`), должны быть установлены на системе, из которой вызывается импорт.

Для чтения из СУБД, которая поддерживает JDBC-протокол, нужно скачать JDBC-драйвер этой СУБД.

Файл `connectors/pom.xml` содержит конфиг для Maven, в котором указаны JDBC-драйвера для MySQL и PostgreSQL. Эти драйвера можно скачать следующим образом:

```bash
~/yt/connectors$ mvn dependency:copy-dependencies
```

`mvn` скачает jar-файлы с драйверами для PostgreSQL и MySQL в директорию `target/dependency`.

Если необходимо импортировать данные из другой СУБД, добавьте JDBC-драйвер этой СУБД в качестве зависимости в `pom.xml`, и запустите команду `$ mvn dependency:copy-dependencies`

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

