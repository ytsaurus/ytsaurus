# Обзор

FLYT — это проект по интеграции [Apache Flink](https://flink.apache.org/) и {{product-name}}. С его помощью можно использовать Flink для потоковой и пакетной обработки данных, читая их из {{product-name}} и записывая обратно в реальном времени.

## Компоненты {#components}

- [flink-connector-ytsaurus](../../../../user-guide/data-processing/flyt/flink-connector-ytsaurus.md) — коннектор Apache Flink к сортированным динамическим таблицам {{product-name}}; поддерживает запись, чтение ограниченных потоков и Lookup-операции;
- [flink-yson](../../../../user-guide/data-processing/flyt/flink-yson.md) — форматтер для работы с [YSON](../../../../user-guide/storage/yson.md) в задачах Flink.

## С чего начать {#getting-started}

Если вы впервые работаете с FLYT, начните с раздела [Быстрый старт](../../../../user-guide/data-processing/flyt/flink-connector-ytsaurus.md#quick-start-guide) в документации коннектора.
