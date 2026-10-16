## SPYT


Выпущен в виде docker-образа.




**Релизы:**

{% cut "**2.11.1**" %}

**Дата релиза:** 2026-08-25


**Страница релиза:** [2.11.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.11.1)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.11.1](https://github.com/orgs/ytsaurus/packages/container/spyt/1169310265?tag=2.11.1)


Технический релиз с небольшими улучшениями и исправлениями ошибок

- Поддержка методов `schema_hint` в сессиях Spark Connect
- Поддержка UDF для python-бинарников, отправляемых через Spark Connect
- Расширенная поддержка UInt64 для Spark Connect
- Поддержка указания пула для операции драйвера Spark Connect
- Проверка доступности эндпоинта Spark Connect
- Повторное использование конфигурации Hadoop в фабриках чтения
- Исправлено чтение столбцов с точками в названиях из lookup-таблиц
- Другие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.11.0**" %}

**Дата релиза:** 2026-07-31


**Страница релиза:** [2.11.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.11.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.11.0](https://github.com/orgs/ytsaurus/packages/container/spyt/1085278761?tag=2.11.0)


Мажорный релиз, включающий поддержку Spark 4.2.x и отправку джобов во внутренний кластер без JVM.

- Поддержка Spark 4.2.x и Java 25
- Поддержка Java 21 для Spark 4.0.x и выше
- Рефакторинг отправки во внутренний standalone-кластер для использования реализации на чистом Python без JVM
- Поддержка включения сервиса shuffle YTsaurus на уровне отдельного приложения для standalone-кластеров
- Push-based режим для сервиса shuffle YTsaurus, включается с помощью `spark.ytsaurus.shuffle.push.enabled`
- Асинхронное создание задач чтения для ytPartitioning, ограниченное новой опцией `spark.ytsaurus.throttling.maxConcurrency` 
- ``spark.ytsaurus.*` стал основным префиксом конфигурации, `spark.yt.*` сохранён как резервный алиас
- Экспорт логов в Monium, включается с помощью `spark.ytsaurus.logs.export.enabled`
- Драйвер Spark Connect использует временный токен своей vanilla-операции, опция `spark.ytsaurus.connect.token.refresh.period` удалена
- Исправлены зависания драйвера при распределённой записи в случае ошибок
- Исправлены дубликаты ключей конфигурации Spark, повторно инжектированные из окружения
- Другие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.9.3**" %}

**Дата релиза:** 2026-07-13


**Страница релиза:** [2.9.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.3)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.3](https://github.com/orgs/ytsaurus/packages/container/spyt/1028609465?tag=2.9.3)


Технический релиз с небольшими улучшениями

- Включение сервиса shuffle YTsaurus для отдельного приложения вместо глобального включения на standalone-кластерах
- Исправлена совместимость `operation_alias` и `enable_multi_operation_mode`
- Исправлены зависания драйвера при распределённой записи в случае ошибок
- Другие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.10.0**" %}

**Дата релиза:** 2026-05-27


**Страница релиза:** [2.10.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.10.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.10.0](https://github.com/orgs/ytsaurus/packages/container/spyt/894673032?tag=2.10.0)


Мажорный релиз, включающий поддержку Spark 4.0.x и 4.1.x.

- Добавлена поддержка Scala 2.13 наряду с Scala 2.12
- Поддержка python-пакета pyspark-client для написания клиентской логики с использованием протокола Spark Connect без JVM
- Транзакционный Streaming
- Прекращена поддержка Spark 3.2.x
- Прекращена поддержка Java 11, все классы JVM скомпилированы с Java 17
- Прекращена поддержка Python 3.8, 3.9 и 3.10
- Прекращена поддержка Livy для интеграции с Query Tracker

{% endcut %}


{% cut "**2.9.2**" %}

**Дата релиза:** 2026-05-22


**Страница релиза:** [2.9.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.2](https://github.com/orgs/ytsaurus/packages/container/spyt/884346974?tag=2.9.2)


Технический релиз с небольшими улучшениями

- Поддержка функциональности runtime-фильтров (динамическое отсечение партиций) для dataframe API
- Исправлен stacktrace для отключённых метрик
- Метод `wait_for_spark_connect_endpoint` перенесён в `spyt.connect`
- Проброс `nullable` в `pushStructMetadata` при отсечении столбцов
- Исправлена запись nullable-значений составных столбцов в динамических таблицах
- Другие мелкие исправления и улучшения


{% endcut %}


{% cut "**2.9.1**" %}

**Дата релиза:** 2026-05-08


**Страница релиза:** [2.9.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.1)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.1](https://github.com/orgs/ytsaurus/packages/container/spyt/851298038?tag=2.9.1)


Технический релиз с небольшими улучшениями

- Ускорена блокировка нескольких таблиц при транзакционном чтении за счёт использования асинхронных запросов блокировок
- Исправлено применение pushdown-фильтров для Spark SQL API
- Поддержка указания пользовательских атрибутов при создании таблицы
- Добавлен ytPartitioning по сжатому размеру YT-таблицы вместо веса данных. Может улучшить производительность для таблиц поиска. По умолчанию отключено; включите с помощью `spark.yt.read.ytPartitioning.compressedSize.enable=true`.
- Исправлена запись вложенных беззнаковых типов в динамические таблицы
- Другие мелкие исправления и улучшения

{% endcut %}


{% cut "**2.9.0**" %}

**Дата релиза:** 2026-03-30


**Страница релиза:** [2.9.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.9.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.9.0](https://github.com/orgs/ytsaurus/packages/container/spyt/765852936?tag=2.9.0)


- Поддержка Spark Connect во внутреннем кластере Spark Standalone
- Поддержка безопасности на уровне строк и столбцов (RLS/CLS)
- Обновлена статистика чтения и записи
- Исправления производительности и стабильности

{% endcut %}


{% cut "**2.8.2**" %}

**Дата релиза:** 2025-12-23


**Страница релиза:** [2.8.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.8.2](https://github.com/orgs/ytsaurus/packages/container/spyt/621174080?tag=2.8.2)


Технический релиз с небольшими улучшениями

- Повышение стабильности поддержки распределённого API чтения и записи
- Прочие мелкие исправления

{% endcut %}


{% cut "**2.8.0**" %}

**Дата релиза:** 2025-11-27


**Страница релиза:** [2.8.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.8.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.8.0](https://github.com/orgs/ytsaurus/packages/container/spyt/591865107?tag=2.8.0)


- Поддержка динамического выделения ресурсов в сценариях direct submit
- Поддержка распределённого API чтения и записи YTsaurus
- Автоматическое завершение работы драйвера при ошибках executor'ов
- Улучшения интеграции Spark Connect

{% endcut %}


{% cut "**2.7.5**" %}

**Дата релиза:** 2025-11-05


**Страница релиза:** [2.7.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.5)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.5](https://github.com/orgs/ytsaurus/packages/container/spyt/566520656?tag=2.7.5)


Технический релиз с небольшими улучшениями

- Корректное чтение беззнаковых типов (uint8, uint16, uint32) в форматах arrow и wire

{% endcut %}


{% cut "**2.7.4**" %}

**Дата релиза:** 2025-10-07


**Страница релиза:** [2.7.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.4)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.4](https://github.com/orgs/ytsaurus/packages/container/spyt/536915303?tag=2.7.4)


Технический релиз с небольшими улучшениями

- Более надёжная обработка стриминговых смещений

{% endcut %}


{% cut "**2.7.3**" %}

**Дата релиза:** 2025-09-08


**Страница релиза:** [2.7.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.3)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.3](https://github.com/orgs/ytsaurus/packages/container/spyt/508561375?tag=2.7.3)


Технический релиз с небольшими улучшениями

- Рефакторинг записи и чтения данных shuffle
- Улучшения метрик

{% endcut %}


{% cut "**2.7.2**" %}

**Дата релиза:** 2025-09-01


**Страница релиза:** [2.7.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.2)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.2](https://github.com/orgs/ytsaurus/packages/container/spyt/501679957?tag=2.7.2)


Технический релиз с небольшими улучшениями

- Улучшение интеграции с сервисом shuffle YTsaurus
- Обёртка сервера Spark Connect для SPYT


{% endcut %}


{% cut "**2.7.1**" %}

**Дата релиза:** 2025-08-15


**Страница релиза:** [2.7.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.1)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.1](https://github.com/orgs/ytsaurus/packages/container/spyt/487987719?tag=2.7.1)


Технический релиз с небольшими улучшениями

- Оптимизация количества запросов к мастер-серверу в сценариях массового чтения
- Скрытие конфиденциальной информации из командной строки драйвера и передача её через защищённое хранилище
- Исправлены имена хостов executor'ов в сетевом проекте
- Отображение идентификатора операции executor'а в описании операции драйвера
- Исправлено экранирование java-свойств
- Добавление контекста парсинга в исключения парсинга
- Исправлены соединения (joins) по столбцам типа uint64
- Поддержка настройки защищённого хранилища в сценариях direct submit


{% endcut %}


{% cut "**2.7.0**" %}

**Дата релиза:** 2025-07-24


**Страница релиза:** [2.7.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.7.0)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.7.0](https://github.com/orgs/ytsaurus/packages/container/spyt/469733902?tag=2.7.0)


- Поддержка сервиса shuffle YTsaurus
- Рефакторинг метрик для внутреннего кластера и режимов direct submit
- Запросы к динамическим таблицам через SQL API не требуют явного указания timestamp
- Исправления ошибок и стабильности:
- - Исправлены ошибки OutOfMemory для оптимизированных для сканирования отсортированных таблиц
- - Исправлено приведение типов к uint64 в codegen
- - Исправлена ошибка YT "Manually specified and authenticated users mismatch" в direct submit
- - Прочие мелкие исправления

{% endcut %}


{% cut "**2.6.5**" %}

**Дата релиза:** 2025-06-08


**Страница релиза:** [2.6.5](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.5)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.6.5](https://github.com/orgs/ytsaurus/packages/container/spyt/433480410?tag=2.6.5)


Технический релиз с небольшими улучшениями

- Поддержка Spark 3.5.6
- Незначительные улучшения поддержки Spark Streaming в YTsaurus


{% endcut %}


{% cut "**2.6.4**" %}

**Дата релиза:** 2025-05-16


**Страница релиза:** [2.6.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.4)


**Docker-образ:** [ghcr.io/ytsaurus/spyt:2.6.4](https://github.com/orgs/ytsaurus/packages/container/spyt/417318819?tag=2.6.4)


Технический релиз с небольшими улучшениями и исправлениями ошибок

- Поддержка получения идентификатора операции драйвера в сценариях direct submit
- Уменьшение количества потоков YTsaurusClient за счёт переиспользования экземпляров клиента
- Исправлен JSON-формат для log4j2
- Заголовки транзакций для транзакций SPYT
- Исправлена конфигурация метрик prometheus
- Исправлен выделенный режим драйвера для standalone-кластера


{% endcut %}


{% cut "**2.6.0**" %}

**Дата релиза:** 2025-04-23


**Страница релиза:** [2.6.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.6.0)


- Поддержка Java 17
- Поддержка типов YTsaurus UUID и Json
- Поддержка прокси RPC-job в direct submit
- Поддержка дополнительных параметров задачи в спецификации операции YTsaurus через конфигурацию Spark в direct submit
- Поддержка взятия блокировок слепков во время чтения
- Явный флаг для усечённого результата запросов Query Tracker
- Исправлена совместимость со Spark 3.5.4 и 3.5.5
- Исправлены SQL-функции для date и timestamp через Query Tracker
- Множество исправлений ошибок и улучшений стабильности

{% endcut %}


{% cut "**2.5.0**" %}

**Дата релиза:** 2024-12-25


**Страница релиза:** [2.5.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.5.0)


Значимый релиз, добавляющий поддержку Spark 3.4.x и 3.5.x. 

- Версия Spark для компиляции изменена с 3.2.2 на 3.5.4;
- Начиная с этого релиза, версия Spark для компиляции SPYT будет последней из поддерживаемых;
- Обратная совместимость вплоть до Spark 3.2.2 по-прежнему сохраняется;
- Модульные тесты можно запускать на версии Spark, отличной от используемой при компиляции, с помощью флага sbt `-DtestSparkVersion=3.x.x`


{% endcut %}


{% cut "**2.4.4**" %}

**Дата релиза:** 2024-12-20


**Страница релиза:** [2.4.4](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.4)


Технический релиз с исправлениями ошибок:

- Передача сетевого проекта для Livy через аргумент командной строки


{% endcut %}


{% cut "**2.4.3**" %}

**Дата релиза:** 2024-12-16


**Страница релиза:** [2.4.3](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.3)


Технический релиз с исправлениями ошибок:

- Указание сетевого проекта для direct submit и его установка из Livy
- Исправлены чтение и запись для структур со значением float с использованием Dataset API

{% endcut %}


{% cut "**2.4.2**" %}

**Дата релиза:** 2024-12-06


**Страница релиза:** [2.4.2](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.2)


Технический релиз с исправлениями ошибок:

- Автоматическое приведение DatetimeType к TimestampType в spark udf
- Добавлен парсинг spark.executorEnv и spark.ytsaurus.driverEnv и установка SPARK_LOCAL_DIRS
- Исправлены параметры worker_disk_limit и worker_disk_account для standalone-кластера
- Использование совместимых версий SPYT вместо последних для direct submit
- Разделение роли прокси на клиентскую (spark.hadoop.yt.proxyRole) и кластерную (spark.hadoop.yt.clusterProxyRole)
- Добавлен флаг spark.ytsaurus.driver.watch для отслеживания операции драйвера
- Исправлено чтение логов Livy

{% endcut %}


{% cut "**2.4.1**" %}

**Дата релиза:** 2024-11-12


**Страница релиза:** [2.4.1](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.1)


Технический релиз с исправлениями ошибок:

- Исправлено создание таблиц через Spark SQL без явного указания схемы ytTable
- Исправлены сериализация и десериализация вложенных типов time
- Исправлено приведение NULL во вложенных структурах данных

{% endcut %}


{% cut "**2.4.0**" %}

**Дата релиза:** 2024-10-31


**Страница релиза:** [2.4.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.4.0)


* Поддержка запуска локальных файлов и их зависимостей в режиме direct submit путём загрузки в кеш YTsaurus
* Поддержка отправки скомпилированных бинарных файлов Python как spark-приложений через direct submit
* Подсказки схемы при записи Dataframe 
* Исправления ошибок:
* * Запись во внешний S3 из YTsaurus
* * Чтение значений float из вложенных структур
* * Чтение в колоночном формате для Spark 3.3.x
* * Чтение произвольных файлов из Cypress при использовании Spark 3.3.x

{% endcut %}


{% cut "**2.3.0**" %}

**Дата релиза:** 2024-09-11


**Страница релиза:** [2.3.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.3.0)


Главная возможность SPYT 2.3.0 — поддержка Spark 3.3.x. Другие значимые возможности:

* Поддержка расширенных типов Datetime, таких как Date32, Datetime32, Timestamp64, Interval64;
* Поддержка свойств таблиц в Spark SQL;
* Поддержка записи с использованием схемы партиционирования Hive;
* Поддержка указания случайного порта для сервиса Shuffle во внутреннем standalone-кластере;
* Исправление для статистики времени выполнения;
* Исправления ошибок для пользовательской схемы и для персистентности dataframe.

{% endcut %}


{% cut "**2.2.0**" %}

**Дата релиза:** 2024-08-14


**Страница релиза:** [2.2.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.2.0)


- Поддержка чтения из нескольких кластеров YTsaurus
- Передача аннотаций для операций YTsaurus через параметры conf
- Поддержка указания пользовательской схемы при чтении
- Поддержка параметра --archives в spark-submit
- Исправление для int8 и int16 как вложенных полей
- Исправление транзакционного чтения
- Прочие мелкие исправления

{% endcut %}


{% cut "**2.1.0**" %}

**Дата релиза:** 2024-06-19


**Страница релиза:** [2.1.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.1.0)


* Поддержка запуска приложений с использованием GPU
* Поддержка версий Spark 3.2.2-3.2.4
* Поддержка History server для сценариев direct submit
* Поддержка https и TCP-прокси в сценариях direct submit
* Другие мелкие исправления и улучшения


{% endcut %}


{% cut "**2.0.0**" %}

**Дата релиза:** 2024-05-29


**Страница релиза:** [2.0.0](https://github.com/ytsaurus/ytsaurus-spyt/releases/tag/spyt/2.0.0)


SPYT 2.0.0 — первый релиз по новой схеме релизов и в отдельном репозитории ytsaurus-spyt. Главная особенность этого релиза — мы наконец перешли с форка Apache Spark, который использовался в предыдущих релизах, на оригинальный дистрибутив Apache Spark. В релизе SPYT 2.0.0 по-прежнему используется Apache Spark 3.2.2, но мы планируем поддержать все релизы Apache Spark 3.x.x в ближайшем будущем!

Другие заметные изменения:
- Поддержка direct submit при использовании Livy через Query Tracker;
- Модуль data-source разделён на data-source-base, использующий стандартные типы Spark для всех типов YTsaurus, и data-source-extended для нашей реализации пользовательских типов YTsaurus, не имеющих прямых соответствий в системе типов Spark;
- Поддержка direct submit из Jupyter notebooks;
- Пользовательский UDT для типа datetime YTsaurus.

{% endcut %}

