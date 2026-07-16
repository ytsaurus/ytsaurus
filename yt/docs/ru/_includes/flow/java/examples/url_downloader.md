# URL Downloader в {{product-name}} Flow (Java)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) группирует входящие URL по хосту, накапливает их во внутреннем [стейте](../../../../flow/concepts/glossary.md#state) и обрабатывает пакетами по таймеру: через 5 секунд после постановки URL в очередь срабатывает таймер, который эмитирует результаты в выходной [стрим](../../../../flow/concepts/glossary.md#stream-and-computation).

[Исходный код (Java)]({{source-root}}/yt/yt/flow/examples/java/url_downloader)

[Исходный код (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/url_downloader)
## Компоненты

### UrlDownloadFunction

Основная процессная функция, реализующая логику накопления и обработки URL. Метод `onMessage` добавляет URL в внутренний стейт хоста и устанавливает таймер; метод `onTimer` обрабатывает накопленные URL и эмитирует результаты:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.java' lang='java' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloadFunction.kt' lang='kotlin' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endlist %}

### HostState

Модель внутреннего стейта, сериализуемая в YSON. Хранит имя хоста и список URL, ожидающих обработки:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/model/HostState.java' lang='java' lines='[BEGIN host_state]-[END host_state]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/model/HostState.kt' lang='kotlin' lines='[BEGIN host_state]-[END host_state]' keep-indents %}

{% endlist %}

### UrlDownloaderComputationContext

Конфигурация компаньона регистрирует компьютейшен `url_downloader` через `ComputationProvider`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/UrlDownloaderComputationContext.java' lang='java' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/UrlDownloaderComputationContext.kt' lang='kotlin' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

{% endlist %}

### NodeCompanionMain

Точка входа компаньона на основе Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/url_downloader/url_downloader/src/main/java/tech/ytsaurus/flow/examples/urldownloader/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/url_downloader/url_downloader/src/main/kotlin/tech/ytsaurus/flow/examples/urldownloader/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Ключевые паттерны

- **Группировка по ключу** — стейт создаётся отдельно для каждого хоста; Flow автоматически направляет сообщения с одинаковым ключом в один экземпляр компьютейшена.
- **Таймер на основе wall-clock времени** — `output.addTimer(System.currentTimeMillis() / 1000 + 5, 0L)` запускает обработку через 5 секунд после постановки URL в очередь. Несколько вызовов `addTimer` с одинаковым `triggerTimestamp` дедуплицируются.
- **Пакетная обработка в `onTimer`** — все накопленные URL обрабатываются разом при срабатывании таймера, что снижает число обращений к downstream-сервисам.
- **Очистка стейта** — после обработки стейт удаляется через `accessor.clear()`, предотвращая утечку памяти.
- **YsonStateAccessor** — внутренний стейт сериализуется в YSON и хранится на стороне C++ воркера; Java-объект получается через `getOrDefault`.

## См. также

- [Быстрый старт (Java)](../../../../flow/java/getting-started.md)
- [Computation (Java)](../../../../flow/java/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
- [Аналогичный пример на C++](../../../../flow/cpp/examples/url_downloader.md)
