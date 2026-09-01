## Java SDK


Пакеты SDK опубликованы в [Maven Central](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).




**Релизы:**

{% cut "**1.2.16**" %}

**Дата релиза:** 2026-05-25


**Страница релиза:** [1.2.16](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.16)


**Maven Central:** [1.2.16](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.16)


#### Новые возможности:
- Перенесены `YTreeSerializers` в модуль `ytsaurus-client-core` [55bf34b0a413399870c1c576fb634a04ed87b38b]
- Добавлен фильтр атрибутов в `GetJob` [05667697aa625b51df7055d9f737354d98b5055a]
- Приведены `Tuple` и `Variant` к типу `COMPOSITE` для typeV3 [9fd9d56162bb039ede5e1722ed2980a8bbcb92f9]

{% endcut %}


{% cut "**1.2.15**" %}

**Дата релиза:** 2026-04-29


**Страница релиза:** [1.2.15](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.15)


**Maven Central:** [1.2.15](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.15)


#### Новые возможности:
- Добавлена поддержка `AttachTransaction` [30d8fa3bd2fd17ca2277517f2928dec06da15199]
- Реализован `SuppressableAccessTrackingOptions` [45cea080eace9c29a05fe693166abc9250d8f5b1]
- Добавлена поддержка параметра размера сжатых данных в методе `PartitionTables` [212229494209279cdea52254cc1af67369ff08f2]

#### Исправления:
- Исправлен NPE в потоковом writer, когда `onPayload` приходит до `onStartStream` [7e625a51db05eb717f010ce00b33e135de029268]

{% endcut %}


{% cut "**1.2.14**" %}

**Дата релиза:** 2026-03-25


**Страница релиза:** [1.2.14](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.14)


**Maven Central:** [1.2.14](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.14)


#### Новые возможности:
- Добавлена поддержка необязательного `expressionBuilderVersion` в `SelectRowsRequest` [1736128d2d5b83f9881112d71e5b203bdd3c65d0]
- Добавлен метод `GetCurrentUser` [b5f3560971dc1eafe530180df21d7f8eefd91d8b]
- Добавлена поддержка необязательных элементов списка в Entity из схемы таблицы [4635b29bb95c2bfe2f2b4f66fb6f6e45d8c7adb4]
- Добавлена поддержка listener для отслеживания байтов, полученных клиентом [e7c49e711236d3dcf81c27c9877f72deb17fce40]

#### Исправления:
- Исправлено накопление повторяющихся полей в `TableAttachmentProtobufReader` [3d58a41edb1bdfe62533f9ad8c253f2f992cb5ba]
- Исправлены параметры колонок `CheckPermission`, чтобы разрешить null, так как на стороне API возникала ошибка «Cannot specify columns for full_read permission check» [b87a0ade9bf289f89f91b02bb996e00fd3b90b13]
- Добавлена поддержка `MessageLite` для определения колонок `TiType` и `EntitySkiffSerializer` [2675dd884e8779457856c68870c72646533c8c18]

{% endcut %}


{% cut "**1.2.13**" %}

**Дата релиза:** 2026-02-16


**Страница релиза:** [1.2.13](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.13)


**Maven Central:** [1.2.13](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.13)


#### Новые возможности:
- Добавлена поддержка `Short` в `YTreeBuilder`.
- Добавлен `findByCode` в `YTsaurusErrorCode`.

#### Исправления:
- Исправлен размер пустого attachment (1 вместо 0).
- Оптимизировано внутреннее представление `ColumnValueType` для повышения производительности (`Map` заменён на `Array`).
- Предварительное вычисление типа колонки и типа передачи данных в `ColumnSchema`.

{% endcut %}


{% cut "**1.2.12**" %}

**Дата релиза:** 2025-12-11


**Страница релиза:** [1.2.12](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.12)


**Maven Central:** [1.2.12](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.12)


#### Новые возможности
* Добавлены методы lookupRowsV2, versionedLookupRowsV2 и multiLookupRowsV2 с поддержкой частичных результатов.
* Добавлена поддержка флага 'omit\_inaccessible\_rows' в API-вызовах read\_table и read\_table\_partition.

#### Исправления
* Агрегаты статистики запросов стали публичными.
* Обновлены версии log4j и log4j-slf4j для устранения уязвимости в log4j.

{% endcut %}


{% cut "**1.2.11**" %}

**Дата релиза:** 2025-10-23


**Страница релиза:** [1.2.11](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.11)


**Maven Central:** [1.2.11](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.11)


#### Новые возможности
* Добавлен метод `execute` в `MultiYTsaurusClient`.
* Добавлена поддержка пустого корневого элемента `YPath` для спецификации операции.
* Добавлена поддержка метода `PatchOperationSpec`.
* Добавлен API для распределённого чтения: `createTablePartitionReader`.
* Добавлен метод `PullQueue`.
* Реализован режим pretty-print для текстовой сериализации YSON.
* Добавлен API для распределённой записи: `startDistributedWriteSession`, `writeTableFragment`, `finishDistributedWriteSession`.
* Добавлена поддержка listener (`RpcClientListener`) для отслеживания байтов, отправленных клиентом.
* Добавлен флаг `sortOrder` в `ListQueries`.

#### Исправления
* Исправлен `OperationContext.getTableIndex()` при использовании `ReducerWithKey`.

{% endcut %}


{% cut "**1.2.10**" %}

**Дата релиза:** 2025-07-17


**Страница релиза:** [1.2.10](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.10)


**Maven Central:** [1.2.10](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.10)


* Добавлен `range` в `CreateShuffleReader`.
* Добавлены `writerIndex`, `overwriteExistingWriterData` в `CreateShuffleWriter`.
* Незначительные исправления и улучшения сообщений об ошибках.

{% endcut %}


{% cut "**1.2.9**" %}

**Дата релиза:** 2025-04-08


**Страница релиза:** [1.2.9](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.9)


**Maven Central:** [1.2.9](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.9)


* Добавлен `fullResult` в `QueryResult`.
* Добавлен `QueryStatistics` в `SelectRowsResult`.
* Для сложных типов в `UnversionedValue` установлен тип 'composite' вместо 'any'.
* Добавлена поддержка 'sort_by' в ReduceSpec.
* Добавлены методы API для работы с Shuffle Service.
* Добавлена поддержка повторных попыток для copy/move между ячейками.
* Добавлена поддержка дополнительных секретов в `StartQuery`.
* Добавлена повторная попытка для таймаута `YTsaurusError`.
* Устранена ошибка при атрибуте 'cluster' в путях очереди и consumer в listQueueConsumerRegistrations.

{% endcut %}


{% cut "**1.2.8**" %}

**Дата релиза:** 2025-01-23


**Страница релиза:** [1.2.8](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.8)


**Maven Central:** [1.2.8](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.8)


* Обновлён protobuf до версии 3.25.5.
* Добавлена поддержка возможности использования пользовательского `TableRowsSerializer`, передаваемого через `SerializationContext`.
* Изменён тип protobuf параметров `YPath` и `RichYPath`: `string` -> `bytes`.
* Удалена «command» из поля спецификации "started_by".

{% endcut %}


{% cut "**1.2.7**" %}

**Дата релиза:** 2024-11-25


**Страница релиза:** [1.2.7](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.7)


**Maven Central:** [1.2.7](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.7)


* Добавлен интерфейс `RequestMiddleware` для подписки на начало запроса.
* Добавлена поддержка `ListQueueConsumerRegistrations`.
* Добавлен интерфейс callback мониторинга для `MultiYTsaurusClient`.
* Выполнен рефакторинг `MultiYTsaurusClient`.
* Добавлена поддержка `YT_BASE_LAYER`.
* Исправлена утечка ресурсов в `ClientPool`.

{% endcut %}


{% cut "**1.2.6**" %}

**Дата релиза:** 2024-09-05


**Страница релиза:** [1.2.6](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.6)


**Maven Central:** [1.2.6](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.6)


* Выпущен `YsonJsonConverter`.
* Добавлена поддержка типов `Date32`, `Datetime64`, `Timestamp64`, `Interval64`.
* Исправлена ошибка, из-за которой `writeTable` зависал, если схема таблицы не совпадала со схемой, указанной пользователем.

{% endcut %}

{% cut "**1.2.5**" %}

**Дата релиза:** 2024-08-20


**Страница релиза:** [1.2.5](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.5)


**Maven Central:** [1.2.5](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.5)


* Добавлен MultiYTsaurusClient.
* Поддержка запроса MultiLookupRows.
* Исправлена ошибка, из-за которой при сбое подключения происходило бесконечное ожидание обнаружения прокси.
* Исправлена ошибка, из-за которой выходная таблица операции создавалась без указанной пользователем транзакции.

{% endcut %}


{% cut "**1.2.4**" %}

**Дата релиза:** 2024-06-18


**Страница релиза:** [1.2.4](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.4)


**Maven Central:** [1.2.4](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.4)


* Поддержка JPA-аннотаций `@Embedded`/`@Embeddable`.
* Поддержка URL-схемы для определения использования TLS.
* Реализованы методы API YT Query Tracker.


{% endcut %}


{% cut "**1.2.3**" %}

**Дата релиза:** 2024-05-27


**Страница релиза:** [1.2.3](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.3)


**Maven Central:** [1.2.3](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.3)


* Добавлен `DiscoveryClient`.
* В полях `@Entity` поддерживаются следующие типы (для указания типа используйте `@Column(columnDefinition=“...”)`):
    * enum -> utf8/string; 
    * String -> string;
    * Instant -> int64;
    * YsonSerializable -> yson.
* Исправлена ошибка, из-за которой `YTsaurusClient` не завершал работу.

{% endcut %}


{% cut "**1.2.2**" %}

**Дата релиза:** 2024-04-11


**Страница релиза:** [1.2.2](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.2)


**Maven Central:** [1.2.2](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.2)


* Добавлена поддержка значений-плейсхолдеров в SelectRowsRequest.
* Добавлена поддержка указания имени сети прокси.
* Добавлена поддержка set(Input/Output)Format в CommandSpec.
* Исправлена ошибка, из-за которой в SyncTableReader возникало исключение NoSuchElementException.
* Исправлена ошибка, из-за которой таблица пересоздавалась при записи без параметра "append".

{% endcut %}


{% cut "**1.2.1**" %}

**Дата релиза:** 2024-01-29


**Страница релиза:** [1.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.1)


**Maven Central:** [1.2.1](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.1)


* Добавлена поддержка сериализуемого mapper/reducer.
* Добавлен метод completeOperation.
* Реализованы три метода API YT Queues: registerQueueConsumer, advanceConsumer, pullConsumer.
* В MultiTablePartition добавлен AggregateStatistics.
* Прочие небольшие исправления ошибок.

{% endcut %}


{% cut "**1.2.0**" %}

**Дата релиза:** 2023-09-18


**Страница релиза:** [1.2.0](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.0)


**Maven Central:** [1.2.0](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.0)


- Исправлена ошибка, из-за которой внутренние потоки `SyncTableReaderImpl` не завершались.
- В запросе `WriteTable` опция `needRetries` по умолчанию установлена в значение `true`.
- В запросе `WriteTable` теперь есть `builder(Class)`; используя его, можно не указывать `SerializationContext`, если класс отмечен аннотацией `@Entity`, реализует интерфейс `com.google.protobuf.Message` или `tech.ytsaurus.ysontree.YTreeMapNode` (форматы сериализации будут `skiff`, `protobuf` или `wire` соответственно).
- Сеттеры `setPath(String)` в билдерах `WriteTable` и `ReadTable` помечены как `@Deprecated`.
- Изменены интерфейсы билдеров запросов `GetNode` и `ListNode`: в метод `setAttributes` вместо `ColumnFilter` передается `List<String>`, аргумент `null` означает `universal filter` (должны быть возвращены все атрибуты).
- В `YTsaurusClientConfig` добавлен флаг `useTLS`; если он установлен в `true`, для `discover_proxies` будет использоваться `https`.

{% endcut %}


{% cut "**1.1.1**" %}

**Дата релиза:** 2023-07-26


**Страница релиза:** [1.1.1](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.1.1)


**Maven Central:** [1.1.1](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.1.1)


- Исправлена проверка схем `@Entity`: чтение подмножества колонок таблицы, надмножества колонок (если типы дополнительных колонок — `nullable`), запись подмножества колонок (если типы отсутствующих колонок — `nullable`).
- В полях `@Entity` поддерживаются следующие типы:
     - `utf8` -> `String`;
     - `string` -> `byte[]`;
     - `uuid` -> `tech.ytsaurus.core.GUID`;
     - `timestamp` -> `java.time.Instant`.
- Если операция, запущенная `SyncYTsaurusClient`, завершается ошибкой, будет выброшено исключение.
- В `YTsaurusClientConfig` добавлен флаг `ignoreBalancers`, который позволяет игнорировать адреса балансировщиков и находить только адреса rpc-прокси.

{% endcut %}
