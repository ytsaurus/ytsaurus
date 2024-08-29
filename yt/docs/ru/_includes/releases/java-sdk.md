### Java

Is published as packages in [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client).

**Current release:** {{java-sdk-version}}

**All releases:**

{% cut "**1.2.5**" %}

- Added MultiYTsaurusClient.
- Support for MultiLookupRows request.
- Fixed a bug that caused an infinite wait for proxy discovery when the connection failed.
- Fixed a bug that caused the operation output table to be created without a user-specified transaction.

{% endcut %}

{% cut "**1.2.4**" %}

- Support for JPA @Embedded/@Embeddable annotations.
- Support for URL schema to detect the usage of TLS.
- Implemented {{product-name}} Query Tracker API methods.

{% endcut %}

{% cut "**1.2.3**" %}

- Introduced `DiscoveryClient`.
- The following types are supported in `@Entity` fields (use `@Column(columnDefinition=“...”)` to specify type):
  - enum -> utf8/string;
  - String -> string;
  - Instant -> int64;
  - YsonSerializable -> yson.
- Fixed a bug due to which `YTsaurusClient` did not terminate.

{% endcut %}

{% cut "**1.2.1**" %}

29.01.2024

- поддержаны сериализуемые mapper/reducer (реализующие интерфейс `Serializable`);
- добавлен метод `completeOperation`;
- поддержано несколько методов {{product-name}} Queues API: `registerQueueConsumer`, `advanceConsumer`, `pullConsumer`;
- в возвращаемых методом `partitionTables` объектах `MultiTablePartition` теперь есть `AggregateStatistics`.

{% endcut %}

{% cut "**1.2.0**" %}

18.09.2023

- исправлен баг с закрытием внутренних потоков `SyncTableReaderImpl`;
- в запросе `WriteTable` опция `needRetries` установлена в `true` по умолчанию;
- в запроса `WriteTable` появился `builder(Class)`, при его использовании можно не указывать `SerializationContext`, если класс помечен аннотацией `@Entity`, реализует интерфейс `com.google.protobuf.Message` или является `tech.ytsaurus.ysontree.YTreeMapNode` (для них будут выбраны форматы сериализации `skiff`, `protobuf` или `wire` соответственно);
- сеттеры `setPath(String)` в билдерах `WriteTable` и `ReadTable` объявлены `@Deprecated`;
- изменился интерфейс билдеров запросов `GetNode` и `ListNode`: в метод `setAttributes` вместо `ColumnFilter` теперь передаётся `List<String>`, аргумент `null` означает `universal filter` (вернутся все атрибуты);
- в `{{product-name}}ClientConfig` добавлен флаг `useTLS`, при выставлении которого в `true` для `discover_proxies` будет использоваться `https`.

{% endcut %}

{% cut "**1.1.1**" %}

06.09.2023

- исправлена валидация схем `@Entity`: можно читать подмножество колонок таблицы, надмножество колонок (если типы лишних колонок `nullable`), писать подмножество колонок (если типы недостающих колонок `nullable`);
- в `@Entity` полях поддержаны типы:
    - `utf8` -> `String`;
    - `string` -> `byte[]`;
    - `uuid` -> `tech.ytsaurus.core.GUID`;
    - `timestamp` -> `java.time.Instant`.
- при падении операции, запущенной `Sync{{product-name}}Client`, бросается исключение;
- в `{{product-name}}ClientConfig` добавлен флаг `ignoreBalancers`, позволяющий проигнорировать адреса балансеров и найти только адреса rpc проксей.

{% endcut %}