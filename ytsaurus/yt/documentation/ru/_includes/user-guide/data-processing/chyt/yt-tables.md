# Работа с таблицами {{product-name}}

Основная задача, которую решает CHYT — возможность обработки таблиц, уже находящихся в {{product-name}}, без необходимости их копирования в какие-либо сторонние хранилища.

Таким образом, основное отличие CHYT от ClickHouse — отсутствие многообразия движков из обычного ClickHouse. Таблицы хранятся в соответствии с логикой [хранения таблиц в {{product-name}}](../../../../user-guide/storage/static-tables.md). Создание таблиц с использованием стандартных движков ClickHouse запрещено, вместо этого можно читать любую из имеющихся в соответствующем кластере {{product-name}} таблиц. Также можно сохранять результаты в виде таблиц {{product-name}}.

"Под капотом" работа с таблицами {{product-name}} устроена похоже на движок [Distributed](https://clickhouse.com/docs/ru/engines/table-engines/special/distributed) из ClickHouse. Исключение: не приходится думать о ручном или автоматическом распределении данных по шардам — {{product-name}} как сторадж решает эту задачу прозрачным, незаметным и надежным образом, раскладывая данные по нодам.

CHYT работает только со [схематизированными таблицами](../../../../user-guide/storage/static-schema.md). У таблицы должен быть заполнен атрибут `/@schema`, или, что то же самое, в веб-интерфейсе {{product-name}} на вкладке **Schema** должна присутствовать схема (т.е. упомянута хотя бы одна колонка).

Таблицы {{product-name}} обозначаются своими полными путями в Кипарисе, обернутыми в backticks или в двойные кавычки: `` `//path/to/table` `` либо `"//path/to/table"`:

```sql
SELECT * FROM `//tmp/sample_table`
```

CHYT умеет читать и писать статические и сортированные динамически таблицы, но стоит учитывать, что из-за особенностей хранения данных чтение из [динамических таблиц](../../../../user-guide/dynamic-tables/overview.md) может занимать в несколько раз больше времени по сравнению со статическими.

## Чтение нескольких таблиц { #many }

- Конкатенация нескольких таблиц {{product-name}}: `concatYtTables(table1, [table2, [...]])`;

  Пример:
  ```sql
  SELECT * FROM concatYtTables("//tmp/sample_table", "//tmp/sample_table2")
  ```

- Конкатенация всех таблиц по пути `cypressPath` (без слеша на конце): `concatYtTablesRange(cypressPath, [from, [to]])`.

  Пример:
  ```sql
  SELECT * FROM concatYtTablesRange("//tmp/sample_tables")
  SELECT * FROM concatYtTablesRange('//tmp/sample_tables','2019-01-01')
  SELECT * FROM concatYtTablesRange('//tmp/sample_tables', '2019-08-13T11:00:00')
  ```

## Сохранение результатов { #save }

CHYT умеет создавать статические таблицы, вставлять в них данные и сохранять результаты запросов в {{product-name}}. Ниже представлен список поддерживаемых запросов:

* `INSERT INTO … VALUES`;
* `INSERT INTO … SELECT`;
* `CREATE TABLE`;
* `CREATE TABLE AS SELECT`.

Примеры:

```sql
-- Вставка данных в таблицу
INSERT INTO `//tmp/sample_table`(a) VALUES (10), (11);
```

```sql
-- Вставка данных из другой таблицы
INSERT INTO `//tmp/sample_table`
SELECT a+100 FROM `//tmp/sample_table` ORDER BY a;
```

```sql
-- Создание таблицы
CREATE TABLE IF NOT EXISTS `//tmp/sample_table`
(
   a String,
   b Int32
) ENGINE = YtTable();
```

```sql
-- Создание таблицы с сортировкой по ключу b
CREATE TABLE IF NOT EXISTS `//tmp/sample_table_with_pk`
(
   a String,
   b Int32
) ENGINE = YtTable() order by b;
```

```sql
-- Создание таблицы на основе select запроса
CREATE TABLE `//tmp/sample_table` ENGINE = YtTable()
AS SELECT a FROM `//tmp/sample_table` ORDER BY a;
```

```sql
-- Создание таблицы на основе select запроса с сортировкой по колонке b
CREATE TABLE `//tmp/sample_table_with_pk` ENGINE = YtTable() order by b
AS SELECT b,a FROM `//tmp/sample_table` order by b;
```

Для перезаписи существующих данных достаточно добавить опцию `<append=%false>` перед путем:

Пример:

```sql
INSERT INTO `<append=%false>//tmp/sample_table`
SELECT a+100 FROM `//tmp/sample_table` ORDER BY a;
```

При создании таблицы можно указывать дополнительные атрибуты, для этого их достаточно передать в Engine в виде [YSON](../../../../user-guide/storage/data-types.md#yson).

Пример:

``` sql
CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{compression_codec=snappy}');

CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{optimize_for=lookup}');

CREATE TABLE `//tmp/sample_table`(i Int64) engine = YtTable('{primary_medium=ssd_blobs}');
```

При сохранении результатов существуют ограничения: значения по умолчанию, выражения для TTL, форматы сжатия для колонок игнорируются.

## Запросы для манипуляции таблицами { #ddl }

CHYT поддерживает следующие операции над таблицами:

* `TRUNCATE TABLE [IF EXISTS]`;
* `RENAME TABLE … TO …`;
* `EXCHANGE TABLES … AND …`;
* `DROP TABLE [IF EXISTS]`.

Примеры:

```sql
-- Удаление всех данных из таблицы
TRUNCATE TABLE `//tmp/sample_table`;
TRUNCATE TABLE IF EXISTS `//tmp/sample_table`;

-- Переименование таблицы
RENAME TABLE `//tmp/sample_table` TO `//tmp/sample_table_renamed`;

-- Обмен имен двух таблиц
EXCHANGE TABLES `//tmp/sample_table` AND `//tmp/other_sample_table`;

-- Удаление таблицы
DROP TABLE `//tmp/sample_table`;
DROP TABLE IF EXISTS `//tmp/sample_table`;
```

## Виртуальные колонки { #virtual_columns }

У каждой {{product-name}}-таблицы есть виртуальные колонки. Такие колонки не видны в запросах вида `DESCRIBE TABLE` и `SELECT * FROM`, получить их можно только явно обратившись к ним.

На текущий момент для каждой таблицы доступны 3 виртуальные колонки:
- `$table_index Int64` — номер таблицы в порядке перечисления в `concatYtTables` (или в порядке сортировки в `concatYtTablesRange`). При чтении одной таблицы индекс всегда равен нулю.
- `$table_path String` — полный путь до таблицы в {{product-name}}.
- `$table_name String` — имя таблицы, то есть последний литерал пути таблицы.

Входные таблицы можно достаточно эффективно фильтровать по описанным виртуальным колонкам, так как их значения известны до чтения каких-либо данных из самих таблиц. Данное свойство можно использовать для чтения практически любого подмножества таблиц из директории.

Однако стоит учитывать, что хоть сами таблицы прочитаны не будут, *метаинформация таблиц* (например, схема) все равно будет загружена. Кроме того, даже если таблица не попала под условие и не будет считана, все равно будет проведена проверка прав доступа пользователя ко всем таблицам указанным в `concatYtTables/concatYtTablesRange`. Поэтому для директорий с большим количеством таблиц рекомендуется указывать ограничения с помощью аргументов функции `concatYtTablesRange`.

Примеры запросов:

```sql
SELECT $table_name FROM "//home/dev/username/t1";
-- Result
-- #	$table_name
-- 1	"t1"
```

```sql
SELECT * FROM "//home/dev/username/t1";
--Result
--#	a	b	c
--1	0	1	1
```

```sql
SELECT *, $table_name, $table_path, $table_index
FROM concatYtTables("//home/dev/username/t0", "//home/dev/username/t1");
-- Result
--#	a	b	c	$table_name	$table_path	$table_index
--1	0	0	0	"t0"	"//home/dev/username/t0"	0
--2	0	1	1	"t0"	"//home/dev/username/t0"	0
--3	0	1	1	"t1"	"//home/dev/username/t1"	1
```

## Работа с динамическими таблицами { #dynamic }

Из CHYT можно читать [динамические](../../../../user-guide/dynamic-tables/overview.md) таблицы, включая dynamic stores (свежие данные в памяти) будет ссылка.

{% note warning "Внимание" %}

Чтобы данные в dynamic stores были видны при чтении из CHYT и Map-Reduce, таблицу следует перемонтировать с выставлением атрибута `enable_dynamic_store_read = %true`.

{% endnote %}

По умолчанию, если попробовать прочитать динамическую таблицу без атрибута `enable_dynamic_store_read = %true`, запрос завершится ошибкой. Если действительно нужны данные только из chunk stores (с отставанием на десятки минут), то можно установить настройку `chyt.dynamic_table.enable_dynamic_store_read` в значение `0`, после чего CHYT форсированно начнет читать только chunk stores (смотрите страницу с [настройками запросов](../../../../user-guide/data-processing/chyt/reference/settings.md).
