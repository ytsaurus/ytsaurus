# Версионированный формат взаимодействия с динамическими таблицами

В сортированных динамических таблицах для каждого значения колонки хранится timestamp его записи. Эти временные метки могут быть полезны для реализации пользовательской логики, либо же миграций данных, когда необходимо сохранить оригинальные timestamp-ы для правильной работы очистки данных по TTL.

Версионированный формат взаимодействия не несет практически никаких накладных расходов, кроме случаев, когда сама колонка не запрошена, а ее timestamp запрошен, в таком случае будут прочитаны данные самой колонки.

Для всех последующих видов взаимодействия предполагается, что timestamp колонки `value` обозначается, как `$timestamp:value`.

## Версионированный lookup и select

Версионированные lookup и select поддержаны в C++ и Python API. Для добавления колонок с timstamps в вывод необходимо указать `versioned_read_options = {read_mode = latest_timestamp}`.
В select-запросах необходимо дополнительно экранировать timestamp колонки квадратными скобками `[$timestamp:value]`.

В Python API вместо `versioned_read_options`, можно указать опцию `with_timestamps`

### Примеры

```python
yt.lookup_rows("//path/to/table", [{"key": 123}], with_timestamps=True)

# Прочитать все колонки и их timestamp-ы.
yt.select_rows("* from [//path/to/table]", with_timestamps=True)

# Прочитать отдельные колонки и timestamp-ы.
yt.select_rows("col_a, [$timestamp:col_a] as ts_a, [$timestamp:col_b] as ts_b from [//path/to/table]", with_timestamps=True)
```

## Версионированный map-reduce

### Чтение

Для чтения timestamp-колонок в map-reduce операциях необходимо добавить опцию в [rich YPath](../../../user-guide/storage/ypath#rich_ypath) входной таблицы: `<versioned_read_options = {read_mode = latest_timestamp}>`

### Запись

Для записи данных в map-reduce операциях в версионированном формате необходимо добавить в [rich YPath](../../../user-guide/storage/ypath#rich_ypath) выходной таблицы `<versioned_write_options = {read_mode = latest_timestamp}>`. Для колонок, для которых указан timestamp будет записан он, для остальных будет взят commit timestamp транзакции bulk insert. 

{% note warning "Внимание" %}

Версионированный формат записи имеет несколько ограничений:
- Запись не поддержана в sort операции.
- Во время записи не происходит проверка на возможное дублирование timestamp-ов у разных записей одной колонки по одному ключу. 

{% endnote %}
