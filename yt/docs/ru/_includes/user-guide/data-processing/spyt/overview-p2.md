
## Когда использовать SPYT { #what-to-do }

SPYT оптимален в следующих случаев:
- разработка на Java с использованием MapReduce в {{product-name}};
- оптимизация производительности пайплайна на {{product-name}} с двумя и более джойнами или группировками;
- настройка интеграционных ETL пайплайнов из других систем;
- ad-hoc аналитика в интерактивном режиме с использованием `Jupyter`, `pyspark` или `spark-shell`.

SPYT не стоит выбирать, если:
- существует необходимость в обработке более 10 ТБ данных в одной транзакции;
- процессинг сводится к единичным Map или MapReduce.

## Способы запуска расчетов на Spark в {{product-name}} { #submit }

- Запуск расчёта напрямую в {{product-name}}, используя команду `spark-submit` [Подробнее](../../../../user-guide/data-processing/spyt/launch.md#submit).
- Создание Standalone Spark кластера внутри {{product-name}} при помощи Vanilla операции [Подробнее](../../../../user-guide/data-processing/spyt/launch.md#standalone).

## На каких языках можно писать { #lang }

Spark поддерживает следующие языки и среды разработки:

* [Jupyter](../../../../user-guide/data-processing/spyt/API/spyt-jupyter.md).
* [Python](../../../../user-guide/data-processing/spyt/API/spyt-python.md).
* [Java](../../../../user-guide/data-processing/spyt/API/spyt-java.md).
* [Scala](../../../../user-guide/data-processing/spyt/API/spyt-scala.md).
