
## Совместимость версий SPYT с версиями Apache Spark{ #spyt-compatibility }, Java, Scala, Python

#|
|| **Версия SPYT** | **Версия Spark** | **Java** | **Scala** | **Python** ||
|| 1.x.x, 2.0.x | 3.2.2 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.1.x, 2.2.x | 3.2.2 - 3.2.4 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.3.x, 2.4.x | 3.2.2 - 3.3.4 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.5.0 | 3.2.2 - 3.5.3 | 11 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|| 2.6.0 | 3.2.2 - 3.5.5 | 11, 17 | 2.12 | 3.8, 3.9, 3.11, 3.12 ||
|#


## Когда использовать SPYT { #what-to-do }

SPYT оптимален в следующих случаев:
- разработка на Java с использованием MapReduce в {{product-name}};
- оптимизация производительности пайплайна на {{product-name}} с двумя и более джойнами или группировками;
- настройка интеграционных ETL пайплайнов из других систем;
- ad-hoc аналитика в интерактивном режиме с использованием `Jupyter`, `pyspark`, `spark-shell` или встроенного в UI компонента [Query Tracker](../../../../user-guide/query-tracker/about.md).

SPYT не стоит выбирать, если:
- существует необходимость в обработке более 10 ТБ данных в одной транзакции;
- процессинг сводится к единичным Map или MapReduce.

## Способы запуска расчетов на Spark в {{product-name}} { #submit }

- Отдельные запуски расчётов напрямую в {{product-name}}, используя команду `spark-submit` [Подробнее](../../../../user-guide/data-processing/spyt/launch.md#submit).
- Создание Standalone Spark кластера как постоянного ресурса внутри {{product-name}} при помощи Vanilla операции [Подробнее](../../../../user-guide/data-processing/spyt/launch.md#standalone).

## На каких языках можно писать { #lang }

Spark поддерживает следующие языки и среды разработки:

* [Jupyter](../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).
* [Python](../../../../user-guide/data-processing/spyt/API/spyt-python.md).
* [Java](../../../../user-guide/data-processing/spyt/API/spyt-java.md).
* [Scala](../../../../user-guide/data-processing/spyt/API/spyt-scala.md).

