# Экспорт в статические таблицы

`TProcessFunctionStaticTableExportSink` выгружает очередь {{product-name}} в неизменяемые статические таблицы, по одной таблице на временное окно. Каждую строку очереди разбирает per-message process function из параметра `processing_function`: она получает исходную строку и выдаёт ноль или несколько строк таблицы в колонках схемы `schema`. Без `processing_function` колонки `schema` берутся из строки очереди по имени. Каждая опубликованная таблица хранит в `@_queue_metadata` смежный диапазон оффсетов (`$row_index`) каждой партиции очереди, и каждая строка экспортируется ровно один раз.

## Как это работает {#how-it-works}

- Окно строки определяется по времени её записи в очередь (`$timestamp`). Окна партиции не идут назад: если время записи уменьшилось или окно уже опубликовано, строка попадает в последнее окно партиции или в первое неопубликованное.
- Process function выполняется на пуле джоба источника внутри ресурса `TStaticTableExportWriterResource`, а не в вычислении, поэтому все строки одного оффсета попадают в одно окно. Функция не может использовать состояние, таймеры, вотермарки и троттлеры, а `GetCurrentTimestamp` возвращает текущее время; строка, для которой она ничего не выдала, считается выгруженной. Ошибка функции роняет джоб партиции, и он перезапускается.
- Ресурс `TStaticTableExportWriterResource` воркера дописывает строки всех партиций в общую служебную таблицу окна. Транзакция окна дописывает строки и продвигает курсоры партиций, а источник получает подтверждение только после её коммита.
- Окно закрыто, когда сохранённый вотермарк выходного стрима достиг `window_end + closure_delta`; простаивающие партиции его не держат. Контроллер публикует закрытые окна по одному, пустые окна тоже публикуются.
- Писатели дописывают только в полосу из `max_unpublished_window_count` окон после последнего опубликованного, поэтому остановившаяся публикация ограничивает объём неопубликованных данных.
- Дрейн завершается после публикации окон, в которые писали все партиции; партиция конечного источника переходит в completed только после публикации своих окон.

## Настройка {#configuration}

Подключите `yt/yt/flow/extensions/static_table_export/process_function` к бинарнику пайплайна. За основу возьмите [пример]({{source-root}}/yt/yt/flow/examples/cpp/queue_static_table_export/pipeline.yson).

Вычисление `TSwiftPassthroughOrderedSourceComputation` содержит один `TQueueSource`, один выходной стрим и один `TProcessFunctionStaticTableExportSink`. Источнику нужны `write_time_column`, `try_parse_flow_queue_meta = %false`, кластер в `queue_path` и отсутствие `partition_filter`; skip-выражения запрещены. Схема стрима содержит `$row_index` типа `int64` и колонку `write_time_column` типа `uint64`; без `processing_function` в ней также есть все колонки `schema` синка с теми же типами.

Стратегия вотермарков проверяется; `column` совпадает с `write_time_column` источника:

```yson
watermark_strategy = {
    event_timestamp_assigner = {column = write_time_ms; format = milli_seconds; limit_by_system_timestamp = %false;};
    watermark_generator = {
        use_source_watermark = %false;
        out_of_orderness_bound = 0;
        idle_partitions = {duration = "10s"; max_ratio = 1.0;};
    };
    watermark_alignment = {group_name = queue-export; drift_bound = "6m";};
};
```

`drift_bound` должен превышать `window_period + closure_delta`, а полоса должна вмещать выравнивание: `max_unpublished_window_count * window_period > drift_bound + closure_delta + 3 * window_period`.

Параметры синка:

- `output_directory` — директория таблиц; состояние экспорта хранится в `_export_state` внутри неё.
- `schema` — несортированная схема таблиц без системных колонок.
- `processing_function` и `processing_function_parameters` — имя process function с методом `ProcessMessage` и её статические параметры. Функция строит строки через `context->MakeOutputMessageBuilder()`.
- `window_period` (по умолчанию 5 минут, целое число секунд), `closure_delta` (по умолчанию 30 секунд), `max_unpublished_window_count` (по умолчанию 16).
- `table_name_utc_offset` (по умолчанию `+03:00`) — смещение от UTC, в котором таблицы называются по началу окна.
- `writer_resource_id` — ресурс писателя, нужен только на воркерах.
- Динамические: `publication_period`, `retry_backoff`, `table_ttl`.

Атрибут `@_queue_metadata` опубликованной таблицы:

```yson
{
    cluster = primary;
    queue_path = "//path/to/queue";
    partitions = [{partition = 0; first_offset = 0; next_offset = 50;}; {partition = 1; first_offset = 0; next_offset = 50;}];
}
```

Состояние привязано к пайплайну, вычислению, очереди, периоду, размеру полосы и смещению имён таблиц; при смене любого из них нужна новая выходная директория. Число партиций очереди фиксируется при старте экспорта. Совместимое изменение `schema`, например новая необязательная колонка, применяется к неопубликованным окнам через `alter_table`.
