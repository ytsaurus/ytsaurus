# Опции стриминга

В разделе перечислены опции SPYT для Structured Streaming, служебные колонки и матрица совместимости версий. Параметры Spark-сессии (включая `spark.yt.streaming.transactional`) — на странице [Конфигурационные параметры](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md).

## Опции источника и приёмника `yt` { #options }

Передаются через `.option(...)` при чтении или записи стримингового датафрейма. Для входной очереди параметр `path` обычно передаётся через `.load(...)`.

| **Опция** | **Описание** | **Обязательный** | **Значение по умолчанию** | **С какой версии** |
| --------- | ------------ | ---------------- | ------------------------- | ------------------ |
| `consumer_path` | Путь к таблице-консьюмеру для чтения из очереди | да, при чтении | — | 1.77.0 |
| `path` | Путь к входной очереди при чтении или к выходной таблице при записи | да | — | 1.77.0 |
| `include_service_columns` | Добавить служебные колонки `__spyt_streaming_src_tablet_index` и `__spyt_streaming_src_row_index` в датафрейм (соответствуют `$tablet_index` и `$row_index` строки в исходной очереди) | нет | `false` | 2.6.0 |
| `max_rows_per_partition` | Максимальное количество строк, читаемых из одной партиции очереди в рамках одного батча | нет | `∞` | 2.6.0 |
| `parsing_type_v3` | Читать композитные типы с сохранением типа. Если опция не указана, используется `spark.yt.read.typeV3.enabled` | нет | `spark.yt.read.typeV3.enabled` | 2.6.0 |
| `write_type_v3` | Писать композитные типы с сохранением типа. Если опция не указана, используется `spark.yt.write.typeV3.enabled` | нет | `spark.yt.write.typeV3.enabled` | 2.6.0 |

## Параметры Spark Structured Streaming { #spark-streaming-options }

`checkpointLocation` — это параметр Spark Structured Streaming, а не специфичная опция источника или приёмника `yt`.

| **Параметр** | **Описание** | **Когда указывать** | **С какой версии** |
| ------------ | ------------ | ------------------- | ------------------ |
| `checkpointLocation` | Путь к директории с чекпоинт-файлами streaming query. Spark хранит здесь промежуточное состояние stateful-операций: например, groupBy, windowing или join.

Для хранения в {{product-name}} укажите путь вида `yt:///...` | Всегда — это обязательное требование Spark Structured Streaming | 1.77.0 |

## Служебные колонки { #service-columns }

При `include_service_columns = true` в стриминговый датафрейм добавляются колонки:

| **Колонка** | **Описание** |
| ----------- | ------------ |
| `__spyt_streaming_src_tablet_index` | Значение `$tablet_index` строки в исходной очереди |
| `__spyt_streaming_src_row_index` | Значение `$row_index` строки в исходной очереди |

## Матрица совместимости { #compatibility-matrix }

| **Функциональность** | **Минимальная версия SPYT** |
| -------------------- | --------------------------- |
| Хранение чекпоинтов на {{product-name}} | 1.77.0 |
| Structured Streaming поверх {{product-name}} Queue API | 1.77.0 |
| Поддержка композитных типов данных | 2.6.0 |
| Опция `max_rows_per_partition` | 2.6.0 |
| Опция `include_service_columns` | 2.6.0 |
| Параметр `spark.yt.write.dynBatchSize` | с 2.6.5 стал конфигурируем для стриминга (ранее был жёстко задан и равен 50000) |
| Транзакционный режим (exactly-once) | 2.10 |

## См. также

- [Structured Streaming](../../../../../user-guide/data-processing/spyt/structured-streaming/index.md) — обзор и основные сценарии
- [Гарантия exactly-once](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/index.md) — выбор подхода к гарантиям
- [Транзакционный режим](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/transactional-mode.md) — инструкция по включению exactly-once
- [Конфигурационные параметры](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md) — параметры Spark-сессии, включая `spark.yt.streaming.transactional` и `spark.yt.write.dynBatchSize`
