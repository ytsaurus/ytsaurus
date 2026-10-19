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
- Исправлен NPE в потоковой записи, когда `onPayload` приходит до `onStartStream` [7e625a51db05eb717f010ce00b33e135de029268]

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


{% cut "**1.2.17**" %}

**Release date:** 2026-07-30


### Features:
- Retry `DirectYTsaurusClient` requests according to the `RetryPolicy` configured in `RpcOptions` [ee088b1a3638d4a5fbac3f639dc7c09c6d068d06]
- Support `diskRequest` in `CommandSpec`, `MapperSpec` and `ReducerSpec` [00ea7fe0dc346a7180c949b801dad6d83f6714f3]
- Support `UnregisterQueueConsumer` [ce1ba0db13680be6bd9992af8c7a096d44488c42]
- Support push-based shuffle API: `setUsePushBasedShuffle` and `setSchema` in `StartShuffle` [ad2c6fde1ee188486068fe4a69a379189128c688]
- Add `pushConfig` option to `StartShuffle` [4f334f912cfc1c685c552835da64640db126ff3e]
- Support prerequisite transactions on `CommitTransaction` [b066ec2b7323a4e5deb7b4aaf64e96119f9814da]
- Add `QUEUE_CONSUMER` and `QUEUE_PRODUCER` to `CypressNodeType` [a024c95be7715d9ddaa18cac5928180d49dc41fa]

### Fixes:
- Fixed per-request timeout being ignored in `FailoverRpcExecutor`; `RpcOptions.globalTimeout` is now a default that a request can override [b099c481764411645ed1aa75945c70a5e259fa85]
- Fixed `TableSchema.toBuilder()` mutating the original schema [700962be850e6822345eb7fae54a6c0cf97255d0]
- Speed up `String` serialization in `YsonBinaryWriter` by avoiding a per-string byte array copy [e19476ee14816744efc6949761490ca9ac0dc14b]
- Fixed possible native memory leak in `ZlibCodec` [f836bf83ae4eb80d34475c11edd4bdfaa46d3112]

{% endcut %}


{% cut "**1.2.16**" %}

**Release date:** 2026-05-25


### Features:
- Move `YTreeSerializers` to `ytsaurus-client-core` module [55bf34b0a413399870c1c576fb634a04ed87b38b]
- Add attributes filter to `GetJob` [05667697aa625b51df7055d9f737354d98b5055a]
- Match `Tuple` and `Variant` to `COMPOSITE` type for typeV3 [9fd9d56162bb039ede5e1722ed2980a8bbcb92f9]

{% endcut %}


{% cut "**1.2.15**" %}

**Release date:** 2026-04-29


### Features:
- Support `AttachTransaction` [30d8fa3bd2fd17ca2277517f2928dec06da15199]
- Implement `SuppressableAccessTrackingOptions` [45cea080eace9c29a05fe693166abc9250d8f5b1]
- Support compressed data size parameter in `PartitionTables` method [212229494209279cdea52254cc1af67369ff08f2]

### Fixes:
- Fixed NPE in stream writer when `onPayload` arrives before `onStartStream` [7e625a51db05eb717f010ce00b33e135de029268]

{% endcut %}


{% cut "**1.2.14**" %}

**Release date:** 2026-03-25


### Features:
- Support optional `expressionBuilderVersion` in `SelectRowsRequest` [1736128d2d5b83f9881112d71e5b203bdd3c65d0]
- Add `GetCurrentUser` method [b5f3560971dc1eafe530180df21d7f8eefd91d8b]
- Support non-optional list elements in Entity from table schema [4635b29bb95c2bfe2f2b4f66fb6f6e45d8c7adb4]
- Support listener for tracking bytes received by the client [e7c49e711236d3dcf81c27c9877f72deb17fce40]

### Fixes:
- Fixed repeated fields accumulation in `TableAttachmentProtobufReader` [3d58a41edb1bdfe62533f9ad8c253f2f992cb5ba]
- Fixed `CheckPermission` column parameters to allow nulls, since it was failing at API side with "Cannot specify columns for full_read permission check" [b87a0ade9bf289f89f91b02bb996e00fd3b90b13]
- Added `MessageLite` support for `TiType` column definition and `EntitySkiffSerializer` [2675dd884e8779457856c68870c72646533c8c18]

{% endcut %}


{% cut "**1.2.13**" %}

**Release date:** 2026-02-16


### Features:
- Support `Short` in `YTreeBuilder`.
- Add `findByCode` to `YTsaurusErrorCode`.

### Fixes:
- Fix empty attachment size (1 instead of 0).
- Optimize `ColumnValueType` internal representation for performance (replace `Map` with `Array`).
- Pre-compute column type and wire type in `ColumnSchema`.

{% endcut %}


{% cut "**1.2.12**" %}

**Release date:** 2025-12-11


### Features
* Add methods lookupRowsV2, versionedLookupRowsV2 and multiLookupRowsV2 with partial result support.
* Support 'omit\_inaccessible\_rows' flag in read\_table and read\_table\_partition API calls.

### Fixes
* Make query statistics aggregates public.
* Update log4j and log4j-slf4j versions in order to fix vulnerability in log4j.

{% endcut %}


{% cut "**1.2.11**" %}

**Release date:** 2025-10-23


### Features
* Add `execute` method to `MultiYTsaurusClient`.
* Support `YPath` empty root designator for operation spec.
* Support `PatchOperationSpec` method.
* Add API for distributed reading: `createTablePartitionReader`.
* Add `PullQueue` method.
* Implement a pretty-print mode for YSON text serialization.
* Add API for distributed writing: `startDistributedWriteSession`, `writeTableFragment`, `finishDistributedWriteSession`.
* Support listener (`RpcClientListener`) for tracking bytes sent by the client.
* Add `sortOrder` flag to `ListQueries`.

### Fixes
* Fix `OperationContext.getTableIndex()` when using `ReducerWithKey`.

{% endcut %}


{% cut "**1.2.10**" %}

**Release date:** 2025-07-17


* Add `range` to `CreateShuffleReader`.
* Add `writerIndex`, `overwriteExistingWriterData` to `CreateShuffleWriter`.
* Minor fixes and improvements to error messages.

{% endcut %}


{% cut "**1.2.9**" %}

**Release date:** 2025-04-08


* Add `fullResult` to `QueryResult`.
* Add `QueryStatistics` to `SelectRowsResult`.
* Set type 'composite' instead of 'any' for complex types in `UnversionedValue`.
* Support 'sort_by' in ReduceSpec.
* Add API methods for working with Shuffle Service.
* Support retries for cross cell copy/move.
* Support additional secrets in `StartQuery`.
* Add retry for timeout `YTsaurusError`.
* Don't fail on 'cluster' attribute in queue and consumer paths in the listQueueConsumerRegistrations.

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


* Добавлен `MultiYTsaurusClient`.
* Поддержка запроса `MultiLookupRows`.
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


* Добавлена поддержка значений-плейсхолдеров в `SelectRowsRequest`.
* Добавлена поддержка указания имени сети прокси.
* Добавлена поддержка set(Input/Output)Format в CommandSpec.
* Исправлена ошибка, из-за которой в SyncTableReader возникало исключение `NoSuchElementException`.
* Исправлена ошибка, из-за которой таблица пересоздавалась при записи без параметра "append".

{% endcut %}


{% cut "**1.2.1**" %}

**Дата релиза:** 2024-01-29


**Страница релиза:** [1.2.1](https://github.com/ytsaurus/ytsaurus/releases/tag/java-sdk/1.2.1)


**Maven Central:** [1.2.1](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.2.1)


* Добавлена поддержка сериализуемого mapper/reducer.
* Добавлен метод `completeOperation`.
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
