# Импорт данных из Hive, S3, MongoDB и других систем

Импорт данных из внешних систем в {{product-name}} реализован через SPYT.

Ниже на этой странице приведена инструкция для запуска скрипта [import.py](https://github.com/ytsaurus/ytsaurus/blob/main/connectors/import.py). Этот скрипт реализует импорт данных из Hadoop, Hive, а также из СУБД, поддерживающих протокол JDBC.

Для импорта из других систем хранения данных (например, S3 или MongoDB), вы можете использовать SPYT напрямую, обратившись к этой системе с помощью соответствующего Spark Datasource.

## Запуск SPYT

Для запуска SPYT-кластера можно воспользоваться [инструкцией](./data-processing/spyt/launch.md).

Также, `import.py` может запустить кластер перед выполнением операции импорта данных. Для этого, передайте следующие опции командной строки:

```bash
$ ./import.py \
    --start-spyt true \
    --discovery_path //path/to/discovery \
    --proxy yt_proxy_host:port
```

Если флаг `--start-spyt` не был передан, то `import.py` ожидает, что кластер уже запущен.

## Зависимости

jar-зависимости для чтения из Hive предоставляются вместе с пакетом `pyspark`. Пакеты для работы с SPYT (включая `pyspark`), должны быть установлены на системе, из которой вызывается импорт.

Для чтения из СУБД, которая поддерживает JDBC-протокол, нужно скачать JDBC-драйвер этой СУБД.

Файл `connectors/pom.xml` содержит конфиг для Maven, в котором указаны JDBC-драйвера для MySQL и PostgreSQL. Эти драйвера можно скачать следующим образом:

```bash
~/yt/connectors$ mvn dependency:copy-dependencies
```

`mvn` скачает jar-файлы с драйверами для PostgreSQL и MySQL в директорию `target/dependency`.
Если необходимо импортировать данные из другой СУБД, добавьте JDBC-драйвер этой СУБД в качестве зависимости в `pom.xml`, и запустите команду `$ mvn dependency:copy-dependencies`

## import.py

При запуске `import.py` необходимо передать discovery-путь для SPYT-кластера
```bash
$ ./import.py --discovery_path //path/to/discovery \
    ... # остальные опциии
```

Дополнительные опции должны идентифицировать источник данных, путь к импортируемым данным, а также путь в {{product-name}}, куда должны быть записанны импортируемые данные.

Для Hive:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    --metastore master_host:9083 \
    --warehouse-dir /path/to/hive/warehouse \
    --input hive:database_name.table_name \
    --output //path/in/yt/table
```

Альтернативно, можно передать в Hive SQL-запрос для исполнения, и импортировать результат этого запроса.
Для этого, используйте префикс `hive_sql` для описания исходных данных:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input hive_sql:database_name:SELECT * FROM action_log WHERE action_date > '2023-01-01' \
    ...
```

Для СУБД, поддерживающих JDBC-протокол (например, PostgreSQL), используйте следующую команду:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    --jdbc postgresql \
    --jdbc-server pg_host:5432 \
    --jdbc-user user \
    --jdbc-password '' \  # Получить пароль из консольного ввода
    --input jdbc:database_name.table_name \
    --output //path/in/yt/table
```

Чтобы передать в СУБД запрос для исполнения, и импортировать результат,
можно использовать префикс `jdbc_sql`:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input jdbc_sql:database_name:SELECT * FROM users WHERE signup_date > '2023-01-01' \
    ...
```

Для импорта файла из HDFS, используйте спецификатор с указанием формата этого файла, а также
адрес HDFS NameNode:

```bash
$ ./import.py --discovery_path //path/to/discovery \
    ...
    --input text:hdfs://namenode/path/to/text/file
    ...
```

import.py также поддерживает форматы parquet, orc.

## Опции

import.py поддерживает следующие опции:

| **Опция** | Описание |
| ----------| --------- |
| `--discovery-path` | обязательная опция - путь, идентифицирующий SPYT-кластер |
| `--num-executors` | число воркеров для операции импорта (по умолчанию 1) |
| `--cores-per-executor` | резервация для числа CPU-ядер на воркер (по умолчанию 1) |
| `--ram-per-core` | RAM-резервация на каждое ядро (по умолчанию 2GB) |
| `--jdbc` | тип JDBC-драйвера для СУБД. Например `mysql` или `postgresql` |
| `--jdbc-server` | host:port сервера СУБД |
| `--jdbc-user` | имя пользователя в СУБД |
| `--jdbc-password` | пароль для СУБД. Если передан пустой флаг, получить из консоли |
| `--jars` | дополнительные JAR-библиотеки. По умолчанию, `target/dependency/jar/*.jar` |
| `--input` | объект для импорта, можно указывать несколько раз |
| `--output` | путь для записи в {{product-name}}. Должно быть указано одно значение на каждое значение `--input` |

Если операция импорта данных предусматривает запуск кластера SPYT, следующие опции командной строки могут использоваться для конфигурации этого кластера:

| **Argument** | Description |
| ----------| --------- |
| `--start-spyt` | указание для `import.py` запустить кластер SPYT |
| `--proxy` | адрес прокси в кластере {{product-name}}, где будет работать SPYT |
| `--pool` | пул ресурсов в {{product-name}} для кластера SPYT |
| `--spark-cluster-version` | версия SPYT |
| `--executor-timeout` | Таймаут для Spark-воркеров |
| `--executor-tmpfs-limit` | Размер tmpfs-партиции для Spark воркеров |


Поддерживаются следующие спецификаторы для импортируемых данных:

| **Спецификатор** | Описание |
| ----------| --------- |
| `hive` | таблица в Hive, вида `db_name.table_name` |
| `hive_sql` | SQL-запрос над Hive, вида `db_name:sql statement` |
| `jdbc` | таблица в СУБД, вида `db_name.table_name` |
| `jdbc_sql` | SQL-запрос для СУБД, вида `db_name:sql statement' |
| `text` | текстовый файл в HDFS |
| `parquet` | parquet-файл в HDFS |
| `orc` | orc-файл из в HDFS |

По умолчанию при записи таблицы в {{product-name}} подразумевается, что таблица не существует. Если
таблица уже существует, можно ее перезаписать или дополнить, используя спецификатор `overwrite` или
`append`, например: `--output overwrite:/path/to/yt`

## Преобразования типов

Импорт сложных типов из внешних систем может поддерживаться лишь частично. Система типов {{product-name}} не имеет взаимно однозначного соответствия с системами типов других систем хранения данных. При иморте, SPYT сохранит тип по мере возможности, и преобразует значение в строку в тех случаях, когда не удалось вывести правильный тип. При необходимости, используйте SQL для корректного преобразования типов.

Диапазоны значений одного типа могут оказаться разными. Например, {{product-name}} `date` не хранит даты ранее Юникс-эпохи, 1 января 1970 года. Попытка записать в {{product-name}} более ранние даты приведет к ошибке. Даты раньше Юникс-эпохи в {{product-name}} можно хранить как строки (например, используя преобразование `to_char(date_value, 'YYYY-MM-DD')` в PostgreSQL), или как целые числа (`date_value - '1970-01-01'` в PostgreSQL).

