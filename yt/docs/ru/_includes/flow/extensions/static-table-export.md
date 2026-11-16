# Экспорт в статические таблицы

`TProcessFunctionStaticTableExportSink` выгружает упорядоченный источник в неизменяемые статические таблицы, по одной таблице на временное окно. Источником может быть, например, очередь {{product-name}} (`TQueueSource`).

Каждое сообщение источника разбирает per-message process function из параметра `processing_function`: она получает исходное сообщение и выдаёт ноль или несколько строк таблицы в колонках схемы `schema`. Без `processing_function` колонки `schema` берутся из сообщения по имени. Каждая опубликованная таблица хранит в атрибуте метаданных смежный диапазон оффсетов каждой партиции источника, и каждое сообщение экспортируется ровно один раз.

## Как это работает {#how-it-works}

- Окно сообщения определяется по времени его записи в источник, а не по event time внутри данных. Окна партиции не идут назад: если время записи уменьшилось или окно уже опубликовано, сообщение попадает в последнее окно партиции или в первое неопубликованное.
- Джоб источника не разбирает сообщения. Синк передаёт их ресурсу `TStaticTableExportWriterResource` своего воркера, который вызывает process function на пуле джоба источника, поэтому CPU учитывается в его партиции, а все строки одного оффсета попадают в одно окно. Функция не может использовать состояние, таймеры, вотермарки и троттлеры, а `GetCurrentTimestamp` возвращает текущее время; сообщение, для которого она ничего не выдала, считается выгруженным. Ошибка функции роняет джоб партиции, и он перезапускается.
- Все писатели дописывают строки в одну общую служебную таблицу окна. Транзакция окна дописывает строки и продвигает курсоры партиций, а источник получает подтверждение только после её коммита. Переигранный префикс, оффсеты которого уже сохранены, подтверждается без разбора.
- Окно закрыто, когда сохранённый вотермарк выходного стрима достиг `window_end + closure_delta`; простаивающие партиции его не держат. Контроллер публикует закрытые окна по одному, пустые окна тоже публикуются.
- Экспорт начинается с самого раннего времени записи первых сообщений всех партиций. Для партиции, которая не получила ни одного сообщения за 30 секунд работы джоба вне дрейна, берётся время начала этих 30 секунд: дрейнящийся джоб не читает, поэтому его молчание не говорит об отставании. Поэтому экспорт пустого источника может начаться с пустых окон.
- Писатели дописывают только в полосу из `max_unpublished_window_count` окон после последнего опубликованного, поэтому остановившаяся публикация ограничивает объём неопубликованных данных.
- При плавном переносе джоба писатель сразу коммитит окна переносимой партиции, поэтому преемник забирает партицию без ожидания.
- Дрейн завершается после публикации окон, в которые писали все партиции; партиция конечного источника переходит в completed только после публикации своих окон.

## Настройка {#configuration}

Подключите `yt/yt/flow/extensions/static_table_export/process_function` и библиотеку с process function к бинарнику пайплайна. За основу возьмите [пример]({{source-root}}/yt/yt/flow/examples/cpp/queue_static_table_export).

Вычисление `TSwiftPassthroughOrderedSourceComputation` содержит один источник, один выходной стрим и один `TProcessFunctionStaticTableExportSink`. Источнику нужны `write_time_column` — колонка с временем записи в миллисекундах, `try_parse_flow_queue_meta = %false` и отсутствие `partition_filter`; skip-выражения запрещены. Схема стрима содержит колонку `write_time_column` типа `uint64`; без `processing_function` в ней также есть все колонки `schema` синка с теми же типами.

### Очередь {{product-name}} {#queue-source}

`queue_path` содержит кластер, а схема стрима — колонку `$row_index` типа `int64`: это оффсет строки. `write_time_column` заполняется из `$timestamp` строки.

### Вотермарки {#watermarks}

Стратегия вотермарков проверяется; `column` совпадает с `write_time_column` источника:

```yson
watermark_strategy = {
    event_timestamp_assigner = {column = write_time_ms; format = milli_seconds; limit_by_system_timestamp = %false;};
    watermark_generator = {
        use_source_watermark = %false;
        out_of_orderness_bound = 0;
        idle_partitions = {duration = "1m"; max_ratio = 1.0;};
    };
    watermark_alignment = {group_name = static-export; drift_bound = "40m";};
};
```

`max_ratio = 1.0` позволяет тихому источнику опубликовать последние окна. `drift_bound` должен превышать `window_period + closure_delta`, а полоса должна вмещать выравнивание: `max_unpublished_window_count * window_period > drift_bound + closure_delta + 3 * window_period`. Пока экспорт догоняет отставание, партиции расходятся на весь `drift_bound`, поэтому неопубликованные таблицы содержат примерно `drift_bound` входного потока: подбирайте его так, чтобы этот объём с фактором репликации 3 помещался в дисковую квоту.

### Синк {#sink}

```yson
export = {
    sink_class_name = "NYT::NFlow::TProcessFunctionStaticTableExportSink";
    input_stream_ids = [rows];
    parameters = {
        output_directory = "//home/service/export/5min";
        writer_resource_id = "ExportWriter";
        processing_function = "NYT::NFlow::NService::TParseRecord";
        schema = [{name = key; type = string;}; {name = value; type = int64;}];
    };
};
```

Функция строит строки через `context->MakeOutputMessageBuilder()` и добавляет их в `output`.

- `output_directory` — директория таблиц; состояние экспорта хранится в `_export_state` внутри неё.
- `schema` — несортированная схема таблиц без системных колонок.
- `processing_function` и `processing_function_parameters` — имя process function с методом `ProcessMessage` и её статические параметры.
- `window_period` (по умолчанию 5 минут, целое число секунд), `closure_delta` (по умолчанию 30 секунд), `max_unpublished_window_count` (по умолчанию 16).
- `table_name_utc_offset` (по умолчанию `+03:00`) — смещение от UTC, в котором таблицы называются по началу окна; часовые пояса регионов не принимаются.
- `writer_resource_id` — ресурс писателя.
- Динамические: `publication_period`, `retry_backoff`, `table_ttl`.

### Ресурс писателя {#writer}

Ресурс объявляется в спеке пайплайна и требуется вычислению только на воркерах:

```yson
resources = {
    ExportWriter = {resource_class_name = "NYT::NFlow::TStaticTableExportWriterResource"; parameters = {};};
};
```

```yson
required_resource_ids = {
    ExportWriter = {worker = %true; controller = %false;};
};
```

Параметры: `max_raw_data_weight` ограничивает доставленные, но не подтверждённые сообщения, `max_parsed_data_weight` — разобранные, но не записанные строки, `max_unwritten_data_weight` — принятые в окна, но ещё не записанные сообщения. `table_writer_count` (по умолчанию 4) — наибольшее число table writer'ов, которые параллельно дописывают в транзакции одного окна: writer добавляется, только пока приём упирается в `max_unwritten_data_weight`. `idle_flush_period` и `max_stream_age` закрывают потоки окон, а `max_stream_age + commit_timeout` — жёсткий дедлайн транзакции. Таблицы пишутся с фактором репликации 3, минимум с двумя репликами, без слияния чанков.

## Метаданные таблиц {#metadata}

Для очереди — атрибут `@_queue_metadata`:

```yson
{
    cluster = primary;
    queue_path = "//path/to/queue";
    partitions = [{partition = 0; first_offset = 0; next_offset = 50;}; {partition = 1; first_offset = 0; next_offset = 50;}];
}
```

Диапазоны партиции начинаются с её первого выгруженного оффсета.

## Эксплуатация {#operations}

Нагрузка на мастера постоянна на писателя и окно, плюс одна блокировка по ключу на партицию в каждом коммите.

Сенсоры: `raw_bytes`, `open_windows`, `durable_bytes`, `rejected_records`, `stream_retries`, `fenced_partitions`, `resets` и `commit_duration` у писателя; `closed_through`, `publication_duration` и `publication_retries` у контроллера синка. Остановившаяся публикация сообщает окно в ошибке `publish`.

Состояние привязано к пайплайну, вычислению, источнику, периоду, размеру полосы и смещению имён таблиц; при смене любого из них нужна новая выходная директория. Добавленные партиции источника экспорт подхватывает сам: их диапазоны начинаются с первого выгруженного оффсета, а до этого в метаданных у них пустые диапазоны с нуля. Уменьшение числа партиций останавливает публикацию с ошибкой `publish`. При смене `schema` контроллер переводит неопубликованные окна на новую схему через `alter_table`; {{product-name}} разрешает только совместимые изменения, например новую необязательную колонку, а при несовместимой нужна новая выходная директория.
