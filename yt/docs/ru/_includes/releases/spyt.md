## SPYT


Выпускается в виде docker-образа.




**Релизы:**

{% cut "**2.11.0**" %}

**Дата релиза:** 2026-07-31


**Страница релиза:** [2.11.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.11.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.11.0](https://github.com/orgs/ytsaurus/packages/container/spyt/1085278761?tag=2.11.0)


Основной релиз, добавляющий поддержку Spark 4.2.x и возможность отправлять джобы во внутренний кластер без JVM.

- Поддержка Spark 4.2.x и Java 25
- Поддержка Java 21 для Spark 4.0.x и новее
- Рефакторинг отправки во внутренний standalone-кластер: теперь используется реализация на чистом Python без JVM
- Поддержка включения сервиса перемешивания YTsaurus для каждого приложения на standalone-кластерах
- Push-режим для сервиса перемешивания YTsaurus, включается с помощью `spark.ytsaurus.shuffle.push.enabled`
- Асинхронное создание задач чтения для ytPartitioning, ограниченное новой опцией `spark.ytsaurus.throttling.maxConcurrency`
- Префикс `spark.ytsaurus.*` стал основным для конфигурации, `spark.yt.*` сохранён как запасной алиас
- Экспорт логов в Monium, включается с помощью `spark.ytsaurus.logs.export.enabled`
- Spark Connect driver использует временный токен своей ванильной операции, опция `spark.ytsaurus.connect.token.refresh.period` удалена
- Исправлены зависания driver при распределённой записи в случае ошибок
- Исправлены дубликаты ключей конфигурации Spark, повторно внедряемых из окружения
- Прочие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.10.0**" %}

**Дата релиза:** 2026-05-27


**Страница релиза:** [2.10.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.10.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.10.0](https://github.com/orgs/ytsaurus/packages/container/spyt/894673032?tag=2.10.0)


Основной релиз, добавляющий поддержку Spark 4.0.x и 4.1.x.

- Добавлена поддержка Scala 2.13 наряду с Scala 2.12
- Поддержка Python-пакета pyspark-client для написания клиентской логики с использованием протокола Spark Connect без JVM
- Транзакционный стриминг
- Прекращена поддержка Spark 3.2.x
- Прекращена поддержка Java 11, все JVM-классы компилируются с Java 17
- Прекращена поддержка Python 3.8, 3.9 и 3.10
- Прекращена поддержка Livy для интеграции с Query Tracker

{% endcut %}


{% cut "**2.9.3**" %}

**Дата релиза:** 2026-07-13


**Страница релиза:** [2.9.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.3)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.3](https://github.com/orgs/ytsaurus/packages/container/spyt/1028609465?tag=2.9.3)


Сопроводительный релиз с незначительными улучшениями

- Сервис перемешивания YTsaurus включается для каждого приложения, а не глобально на standalone-кластерах
- Исправлена совместимость operation_alias и enable_multi_operation_mode
- Исправлены зависания driver при распределённой записи в случае ошибок
- Прочие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.9.2**" %}

**Дата релиза:** 2026-05-22


**Страница релиза:** [2.9.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.2](https://github.com/orgs/ytsaurus/packages/container/spyt/884346974?tag=2.9.2)


Сопроводительный релиз с незначительными улучшениями

- Поддержка runtime-фильтров (динамическое отсечение партиций) для dataframe API
- Исправлен stacktrace при отключённых метриках
- Метод wait_for_spark_connect_endpoint перенесён в spyt.connect
- Распространение nullable в pushStructMetadata при обрезке колонок
- Исправлена запись nullable-значений составных колонок в динамические таблицы
- Прочие мелкие исправления и улучшения


{% endcut %}


{% cut "**2.9.1**" %}

**Дата релиза:** 2026-05-08


**Страница релиза:** [2.9.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.1)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.1](https://github.com/orgs/ytsaurus/packages/container/spyt/851298038?tag=2.9.1)


Сопроводительный релиз с незначительными улучшениями

- Ускорение блокировки нескольких таблиц при транзакционном чтении за счёт асинхронных запросов на блокировку
- Исправлено применение pushdown-фильтров для Spark SQL API
- Поддержка указания пользовательских атрибутов при создании таблицы
- Добавлено ytPartitioning по сжатому размеру таблицы YT вместо веса данных. Может улучшить производительность для lookup-таблиц. Отключено по умолчанию; включите с помощью `spark.yt.read.ytPartitioning.compressedSize.enable=true`.
- Исправлена запись вложенных беззнаковых типов в динамические таблицы
- Прочие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.9.0**" %}

**Дата релиза:** 2026-03-30


**Страница релиза:** [2.9.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.0](https://github.com/orgs/ytsaurus/packages/container/spyt/765852936?tag=2.9.0)


- Поддержка Spark Connect во внутреннем Spark Standalone-кластере
- Поддержка безопасности на уровне строк и колонок (RLS/CLS)
- Обновлённая статистика чтения и записи
- Исправления производительности и стабильности

{% endcut %}


{% cut "**2.8.2**" %}

**Дата релиза:** 2025-12-23


**Страница релиза:** [2.8.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.8.2](https://github.com/orgs/ytsaurus/packages/container/spyt/621174080?tag=2.8.2)


Сопроводительный релиз с незначительными улучшениями

- Повышение стабильности поддержки распределённого API записи и чтения
- Прочие мелкие исправления

{% endcut %}


{% cut "**2.8.0**" %}

**Дата релиза:** 2025-11-27


**Страница релиза:** [2.8.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.8.0](https://github.com/orgs/ytsaurus/packages/container/spyt/591865107?tag=2.8.0)


- Поддержка динамического выделения ресурсов в сценариях прямого запуска
- Поддержка распределённого API чтения и записи YTsaurus
- Автоматическое завершение работы driver при сбоях executor
- Улучшения интеграции Spark Connect

{% endcut %}


{% cut "**2.7.5**" %}

**Дата релиза:** 2025-11-05


**Страница релиза:** [2.7.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.5)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.5](https://github.com/orgs/ytsaurus/packages/container/spyt/566520656?tag=2.7.5)


Сопроводительный релиз с незначительными улучшениями

- Корректное чтение беззнаковых типов (uint8, uint16, uint32) в форматах arrow и wire

{% endcut %}


{% cut "**2.7.4**" %}

**Дата релиза:** 2025-10-07


**Страница релиза:** [2.7.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.4)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.4](https://github.com/orgs/ytsaurus/packages/container/spyt/536915303?tag=2.7.4)


Сопроводительный релиз с незначительными улучшениями

- Более надёжная обработка стриминговых офсетов

{% endcut %}


{% cut "**2.7.3**" %}

**Дата релиза:** 2025-09-08


**Страница релиза:** [2.7.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.3)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.3](https://github.com/orgs/ytsaurus/packages/container/spyt/508561375?tag=2.7.3)


Сопроводительный релиз с незначительными улучшениями

- Рефакторинг записи и чтения данных перемешивания
- Улучшения метрик

{% endcut %}

{% cut "**2.7.2**" %}

**Дата релиза:** 2025-09-01


**Страница релиза:** [2.7.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.2](https://github.com/orgs/ytsaurus/packages/container/spyt/501679957?tag=2.7.2)


Релиз поддержки с небольшими улучшениями

- Улучшение интеграции с сервисом перемешивания YTsaurus
- Обёртка Spark connect server для SPYT


{% endcut %}


{% cut "**2.7.1**" %}

**Дата релиза:** 2025-08-15


**Страница релиза:** [2.7.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.1)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.1](https://github.com/orgs/ytsaurus/packages/container/spyt/487987719?tag=2.7.1)


Релиз поддержки с небольшими улучшениями

- Оптимизация количества запросов к мастеру в сценариях массового чтения
- Скрытие конфиденциальной информации из командной строки драйвера и передача её через защищённое хранилище
- Исправление hostname исполнителей в сетевом проекте
- Отображение id операции исполнителя в описании операции драйвера
- Исправление экранирования java-свойств
- Включение контекста парсинга в исключения парсинга
- Исправление joins по колонкам uint64
- Поддержка настройки защищённого хранилища в сценариях direct submit


{% endcut %}


{% cut "**2.7.0**" %}

**Дата релиза:** 2025-07-24


**Страница релиза:** [2.7.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.0](https://github.com/orgs/ytsaurus/packages/container/spyt/469733902?tag=2.7.0)


- Поддержка сервиса перемешивания YTsaurus
- Рефакторинг метрик для режимов внутреннего кластера и direct submit
- Запросы к динамическим таблицам через SQL API больше не требуют явного указания timestamp
- Исправления ошибок и повышение стабильности:
- - Исправление ошибок OutOfMemory для оптимизированных под сканирование отсортированных таблиц
- - Исправление приведения типов к uint64 в codegen
- - Исправление ошибки YT "Manually specified and authenticated users mismatch" в direct submit
- - Прочие мелкие исправления

{% endcut %}


{% cut "**2.6.5**" %}

**Дата релиза:** 2025-06-08


**Страница релиза:** [2.6.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.5)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.6.5](https://github.com/orgs/ytsaurus/packages/container/spyt/433480410?tag=2.6.5)


Релиз поддержки с небольшими улучшениями

- Поддержка Spark 3.5.6
- Небольшие улучшения поддержки Spark Streaming в YTsaurus


{% endcut %}


{% cut "**2.6.4**" %}

**Дата релиза:** 2025-05-16


**Страница релиза:** [2.6.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.4)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.6.4](https://github.com/orgs/ytsaurus/packages/container/spyt/417318819?tag=2.6.4)


Релиз поддержки с небольшими улучшениями и исправлениями ошибок

- Поддержка получения id операции драйвера в сценариях direct submit
- Сокращение количества потоков YTsaurusClient за счёт повторного использования экземпляров клиента
- Исправление JSON-конфигурации для log4j2
- Названия транзакций для транзакций SPYT
- Исправление конфигурации метрик prometeus
- Исправление выделенного режима драйвера для автономного кластера


{% endcut %}


{% cut "**2.6.0**" %}

**Дата релиза:** 2025-04-23


**Страница релиза:** [2.6.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.0)


- Поддержка Java 17
- Поддержка типов YTsaurus UUID и Json
- Поддержка RPC-job proxy в direct submit
- Поддержка дополнительных параметров задач в спецификации операции YTsaurus через Spark config в direct submit
- Поддержка взятия блокировок слепков во время чтения
- Явный флаг для усечённого результата запросов Query Tracker
- Исправление совместимости со Spark 3.5.4 и 3.5.5
- Исправление date- и timestamp SQL-функций через Query Tracker
- Множество исправлений стабильности и других ошибок

{% endcut %}


{% cut "**2.5.0**" %}

**Дата релиза:** 2024-12-25


**Страница релиза:** [2.5.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.5.0)


Основной релиз, добавляющий поддержку Spark 3.4.x и 3.5.x.

- Версия Spark на этапе компиляции изменена с 3.2.2 на 3.5.4;
- Начиная с этого релиза версия Spark на этапе компиляции SPYT будет последней доступной поддерживаемой версией;
- Обратная совместимость сохраняется вплоть до Spark 3.2.2;
- Модульные тесты можно запускать с версией Spark, отличной от используемой на этапе компиляции, с помощью флага sbt `-DtestSparkVersion=3.x.x`


{% endcut %}


{% cut "**2.4.4**" %}

**Дата релиза:** 2024-12-20


**Страница релиза:** [2.4.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.4)


Релиз поддержки с исправлениями ошибок:

- Указание сетевого проекта для Livy через аргумент командной строки


{% endcut %}


{% cut "**2.4.3**" %}

**Дата релиза:** 2024-12-16


**Страница релиза:** [2.4.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.3)


Релиз поддержки с исправлениями ошибок:

- Указание сетевого проекта для direct submit и его настройка из Livy
- Исправление чтения и записи для структур со значением float с использованием Dataset API

{% endcut %}


{% cut "**2.4.2**" %}

**Дата релиза:** 2024-12-06


**Страница релиза:** [2.4.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.2)


Релиз поддержки с исправлениями ошибок:

- Автоприведение DatetimeType к TimestampType в spark udf
- Добавлен парсинг spark.executorEnv и spark.ytsaurus.driverEnv и установка SPARK_LOCAL_DIRS
- Исправление параметров worker_disk_limit и worker_disk_account для автономного кластера
- Использование совместимых версий SPYT вместо последних для direct submit
- Разделение роли прокси на клиентскую (spark.hadoop.yt.proxyRole) и кластерную (spark.hadoop.yt.clusterProxyRole)
- Добавлен флаг spark.ytsaurus.driver.watch для отслеживания операции драйвера
- Исправление чтения логов Livy

{% endcut %}


{% cut "**2.4.1**" %}

**Дата релиза:** 2024-11-12


**Страница релиза:** [2.4.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.1)


Релиз поддержки с исправлениями ошибок:

- Исправление создания таблиц через Spark SQL без явного указания схемы ytTable
- Исправление сериализации и десериализации вложенных временных типов
- Исправление приведения NULL во вложенных структурах данных

{% endcut %}


{% cut "**2.4.0**" %}

**Дата релиза:** 2024-10-31


**Страница релиза:** [2.4.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.0)


* Поддержка запуска локальных файлов и их зависимостей в режиме direct submit с загрузкой в кэш YTsaurus
* Поддержка отправки скомпилированных python-бинарников в качестве spark-приложений через direct submit
* Подсказки схемы при записи Dataframe
* Исправления ошибок:
* * Запись во внешний S3 из YTsaurus
* * Чтение значений float из вложенных структур
* * Чтение в колоночном формате для Spark 3.3.x
* * Чтение произвольных файлов из Кипариса при использовании Spark 3.3.x

{% endcut %}

{% cut "**2.3.0**" %}

**Дата релиза:** 2024-09-11


**Страница релиза:** [2.3.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.3.0)


Основная возможность SPYT 2.3.0 — поддержка Spark 3.3.x. Другие заметные изменения:

* поддержка расширенных типов Datetime, таких как Date32, Datetime32, Timestamp64, Interval64;
* поддержка табличных свойств в Spark SQL;
* поддержка записи с использованием схемы партиционирования Hive;
* поддержка указания случайного порта для Shuffle-сервиса во внутреннем автономном кластере;
* исправление статистики выполнения;
* исправления ошибок для пользовательской схемы и сохранения датафреймов.

{% endcut %}


{% cut "**2.2.0**" %}

**Дата релиза:** 2024-08-14


**Страница релиза:** [2.2.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.2.0)


- поддержка чтения из нескольких кластеров YTsaurus;
- передача аннотаций для операций YTsaurus через параметры конфигурации;
- поддержка указания пользовательской схемы при чтении;
- поддержка параметра `--archives` в spark-submit;
- исправление для int8 и int16 как вложенных полей;
- исправление транзакционного чтения;
- прочие мелкие исправления.

{% endcut %}


{% cut "**2.1.0**" %}

**Дата релиза:** 2024-06-19


**Страница релиза:** [2.1.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.1.0)


* поддержка запуска приложений с использованием GPU;
* поддержка версий Spark 3.2.2–3.2.4;
* поддержка History Server для сценариев прямого сабмита;
* поддержка https и TCP-прокси в сценариях прямого сабмита;
* прочие мелкие исправления и улучшения.


{% endcut %}


{% cut "**2.0.0**" %}

**Дата релиза:** 2024-05-29


**Страница релиза:** [2.0.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.0.0)


SPYT 2.0.0 — первый релиз в рамках новой схемы релизов и в отдельном репозитории ytsaurus-spyt. Основная особенность этого релиза — мы наконец перешли с форка Apache Spark, использовавшегося в предыдущих релизах, на оригинальный дистрибутив Apache Spark. Релиз SPYT 2.0.0 по-прежнему использует Apache Spark 3.2.2, но в ближайшее время мы планируем поддерживать все релизы Apache Spark 3.x.x.

Другие заметные изменения:
- поддержка прямого сабмита с использованием Livy через Query Tracker;
- разделение модуля data-source на data-source-base, использующий стандартные типы Spark для всех типов YTsaurus, и data-source-extended для нашей реализации пользовательских типов YTsaurus, не имеющих прямых соответствий в системе типов Spark;
- поддержка прямого сабмита из Jupyter-ноутбуков;
- пользовательский UDT для типа datetime в YTsaurus.

{% endcut %}
