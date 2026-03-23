# Обзор

## Что такое Flink

[Apache Flink](https://flink.apache.org/) — это фреймворк и механизм распределенной обработки для вычислений с отслеживанием состояния в потоках данных с неограниченным и ограниченным объемом. Flink разработан для работы во всех распространенных кластерных средах, выполнения вычислений с использованием оперативной памяти и в любом масштабе.

## Что такое FLYT?

FLYT - это проект по интеграции Apache Flink и {{product-name}}.

## Какие компоненты предоставляет FLYT?

- [flink-connector-ytsaurus](../../../../user-guide/data-processing/flyt/flink-connector-ytsaurus.md) - Apache Flink коннектор к сортированным динамическим таблицам {{product-name}};

- [flink-yson](../../../../user-guide/data-processing/flyt/flink-yson.md) - Apache Flink форматер для работы с [YSON](../../../../user-guide/storage/yson.md) форматом.