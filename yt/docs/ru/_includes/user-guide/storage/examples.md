# Примеры работы с объектами Кипариса

В данном разделе приведены примеры работы с объектами Кипариса.

## Статические таблицы { #static_tables }

### Создание { #create_table }

Для создания таблицы используется команда `create`.

```bash
yt create table //home/dev/test_table
1282c-1ed72c-3fe0191-443cf2ee
```

### Удаление { #remove_table }

Для удаления таблицы используется команда `remove`.

```bash
yt remove //home/dev/test_table
```

### Чтение { #read_table }

Чтобы прочитать существующую таблицу `<table>`, используйте команду `read-table`.

```bash
yt read-table [--format FORMAT]
                [--table-reader TABLE_READER]
                <table>
```

Опция `--format` определяет формат выходных данных. Поддерживаются форматы `json`, `yson`, `dsv` и `schemaful_dsv`. Подробнее можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md).

```bash
yt read-table --format dsv //home/dev/test_table
day=monday	time=10
day=wednesday	time=20
day=friday	time=30
```

Опция `--table-reader` позволяет изменять параметры чтения из таблицы.

#### Семплирование { #sampling_rate }

Атрибут `sampling_rate` задает процент данных, которые необходимо прочесть из входной таблицы.

```bash
yt read-table --format dsv --table-reader '{"sampling_rate"=0.3}' //home/dev/test_table
day=monday	time=10
day=friday	time=30
```

В данном примере команда `read-table` вернет 30% всех строк из входной таблицы.

Атрибут `sampling_seed` управляет генератором случайных чисел, который отбирает строки. Гарантируется, что при одинаковом `sampling_seed` и том же наборе входных чанков будет сгенерирован одинаковый выход. Если атрибут `sampling_seed` не был указан, то он будет случайным.

```bash
yt read-table --format dsv --table-reader '{"sampling_seed"=42;"sampling_rate"=0.3}' //home/dev/test_table
```

Чтобы поместить результат семплирования в другую таблицу, запустите `map`:

```bash
yt map cat --src //home/dev/input --dst //home/dev/output --spec '{job_io = {table_reader = {sampling_rate = 0.001}}}' --format yson
```

{% note info "Примечание" %}

При использовании семплирования независимо от указанного атрибута `sampling_rate` все данные читаются с диска.

{% endnote %}


### Перезапись { #write_table }

Команда `write-table` перезаписывает существующую таблицу `<table>` переданными данными.

```bash
yt write-table [--format FORMAT]
                 [--table-writer TABLE_WRITER]
                 <table>
```

Опция `--format` определяет формат входных данных. Поддерживаются форматы `json`, `yson`, `dsv` и `schemaful_dsv`. Подробнее можно прочитать в разделе [Форматы](../../../user-guide/storage/formats.md).

```bash
yt write-table --format dsv //home/dev/test_table
time=10	day=monday
time=20	day=wednesday
time=30 day=friday
^D
```

Чтобы добавить записи в таблицу, используйте опцию `<append=true>` перед путем таблицы.

```bash
cat test_table.json
{"time":"10","day":"monday"}
{"time":"20","day":"wednesday"}
{"time":"30","day":"friday"}
cat test_table.json | yt write-table --format json "<append=true>"//home/dev/test_table
```

Опция `--table-writer` позволяет изменять параметры записи в таблицу:

* [ограничение на размер строки в таблице;](#max_row_weight)
* [размер чанка;](#desired_chunk_size)
* [коэффициент репликации.](#replication_factor)

##### Ограничение на размеры строк в таблице { #max_row_weight }

При записи табличных данных система {{product-name}} проверяет их размер. Запись завершится ошибкой, если размер превысит максимально допустимое значение.
По умолчанию максимальный размер строки — 16 МБ. Чтобы изменить это значение, используйте опцию `--table-writer`, параметр `max_row_weight`. Укажите значение в байтах.

```bash
cat test_table.json | yt write-table --format json --table-writer {"max_row_weight"=33554432} //home/dev/test_table
```

{% note info "Примечание" %}

Значение параметра `max_row_weight` не может превышать 128 МБ.

{% endnote %}

#### Размер чанка { #desired_chunk_size }

Атрибут `desired_chunk_size` определяет размер чанка в байтах.

```bash
cat test_table.json | yt write-table --format json --table-writer {"desired_chunk_size"=1024} //home/dev/test_table
```

#### Коэффициент репликации { #replication_factor }

Количество реплик чанков таблицы задается атрибутом `replication_factor`. По умолчанию оно равно 3.
```bash
yt get //home/dev/test_table/@replication_factor
3
```

Коэффициент репликации можно менять:

```bash
$ yt set //home/dev/test_table/@replication_factor 5
```

В этом случае преобразование произойдет через некоторое время в фоновом режиме.

Управление коэффициентом репликации *новых* чанков таблицы осуществляется опциями `min_upload_replication_factor` и  `upload_replication_factor`. Опция `upload_replication_factor` задает количество реплик, создаваемых синхронно во время записи новых данных в таблицу. Опция `min_upload_replication_factor` задает минимальное количество реплик, которые должны быть успешно записаны, чтобы запись чанка считалась успешной. Значение по умолчанию обоих атрибутов — 2, максимальное значение — 10.

```bash
cat test_table.json | yt write-table --format json --table-writer '{"upload_replication_factor"=5;"min_upload_replication_factor"=3}' //home/dev/test_table
```

Новые чанки, добавляемые в таблицу, будут получать коэффициент репликации, установленный в опции `upload_replication_factor`; далее фоновый процесс будет асинхронно реплицировать их для достижения установленного на таблице коэффициента репликации.

### Медиум { #medium }

Чтобы переместить таблицу в другой медиум, измените значение атрибута `primary_medium`.
Новые данные, записываемые в таблицу, будут сразу попадать на новый медиум, старые данные будут перемещены в фоновом режиме.
Чтобы форсированно переместить данные в новый медиум, запустите операцию Merge:

```bash
yt set //home/dev/test_table/@primary_medium ssd_blobs
yt merge --mode auto --spec '{"force_transform"=true;}' --src //home/dev/test_table --dst //home/dev/test_table
```

Чтобы проверить, изменился ли медиум, в котором находится таблица, выполните команду.

```bash
yt get //home/dev/test_table/@resource_usage
```
```
{
    "tablet_count" = 0;
    "disk_space_per_medium" = {
        "ssd_blobs" = 930;
    };
    "tablet_static_memory" = 0;
    "disk_space" = 930;
    "node_count" = 1;
    "chunk_count" = 1;
}
```
Объем указан в байтах.

