# Статические таблицы в {{product-name}} Flow

Коннектор к [статическим таблицам {{product-name}}](../../../user-guide/storage/static-tables.md).

Код коннектора находится [здесь]({{source-root}}/yt/yt/flow/library/cpp/connectors/static_table).

Статические таблицы &mdash; особый вид источника. В нём изначально нет [партиций](../../../flow/concepts/glossary.md#partition), у строк в них нет таймстемпов записи, сами таблицы как правило иммутабельны и в них не происходит дозаписи. При этом часто читать нужно бесконечную последовательность таблиц. Так же "запись" в этот источник идёт очень крупными блоками, поэтому читать из него необходимо с ограничением скорости, чтобы не отнимать ресурсы у более важных [сорсов](../../../flow/concepts/glossary.md#source) [пайплайна](../../../flow/concepts/glossary.md#pipeline).

В связи с этим основная сложность этого источника лежит в [контроллере](../../../flow/concepts/glossary.md#controller), которому необходимо понять, какие таблицы читать, какие таймстемпы ([SystemTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks), [EventTimestamp](../../../flow/concepts/glossary.md#timestamps-and-watermarks)) для них выбрать, что делать, если таблица неожиданно исчезает и так далее.

### Настройки сорса

Класс сорса: `NYT::NFlow::NStaticTableConnector::TSource`.

##### Статическая спека:

{% include [NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

##### Динамическая спека:

{% include [NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

## Импорт статической таблицы в стейт {#import-into-state}

Типичный сценарий: батч-процесс (YQL{% if audience == "internal" %}, Nirvana{% endif %}, &hellip;) периодически пересобирает справочник в виде статической YT-таблицы, а реалтайм-пайплайн должен джойнить свой поток против свежей версии этого справочника. Коннектор `static_table` поддерживает этот сценарий "из коробки", позволяя загрузить такую таблицу в [стейт](../../../flow/concepts/stateful.md) пайплайна с произвольной бизнес-логикой над каждой строкой перед записью.

### Как это работает

Схема: один computation читает справочник через `static_table`-сорс и пишет его в стейт (loader); второй computation на входном потоке достаёт значение по ключу из того же стейта и эмитит обогащённое сообщение (enricher).

1. `static_table`-сорс потоково отдаёт строки таблицы как обычные сообщения. Контроллер сам определяет, какая версия таблицы свежая, и подаёт её строки в пайплайн в виде отдельного стрима.
2. Loader-computation (stateful) принимает каждую строку, при необходимости валидирует/нормализует её (`trim`, `lower-case`, обогащение из других источников, фильтрация и т. п.) и пишет результат в стейт по ключу.
3. Enricher-computation, висящий на потоке событий, читает значения из того же стейта по ключу события через read-only [joiner](../../../flow/concepts/stateful.md#external-state-joiner) и эмитит обогащённое сообщение в синк.

Главное преимущество этого подхода &mdash; **возможность написать бизнес-логику обработки каждой строки** после сборки таблицы, но до того, как она попала в стейт. Это удобно, когда форма данных на диске не совпадает с тем, что нужно джойну, или когда часть строк надо отфильтровать/обогатить из других сорсов на лету.

### Семантика перезаливки

Каждая новая версия статической таблицы &mdash; **полная перезаливка** всего справочника в стейт. Автоматической очистки удалённых строк у этой схемы нет: если в новой версии строка пропала, её нужно явным образом удалить из стейта &mdash; например, помечая строки версией и периодически удаляя устаревшие через механизм [key visit](../../../flow/concepts/key_visitor.md).

Свежая версия выбирается контроллером по правилам, заданным в `TTableTimestampLocatorSpec` (см. статическую спеку сорса выше) &mdash; чаще всего это самая свежая таблица в директории, имя которой парсится как ISO8601-таймстемп. Появление новой таблицы в директории автоматически инициирует новый прогон.

### Когда выбирать

- Над каждой строкой нужна **бизнес-логика** (нормализация, валидация, обогащение).
- Допустима **полная перезаливка** на каждый ребилд.

Эти условия не жёсткие: подход работает и без бизнес-логики над строками, а сложности полной перезаливки часто обходятся (см. про key visit выше). Тем не менее стоит взвесить и другие варианты &mdash; см. раздел [Альтернативы](#alternatives).

### Конфигурация (набросок)

Спека целиком &mdash; в примерах ниже; здесь только ключевая обвязка пайплайна. Loader пишет в стейт через **manager** (read-write), enricher читает его через **joiner** (read-only).

{% cut "Набросок спеки" %}

```yson
{
    "spec" = {
        "computations" = {
            "reference_reader" = {
                "computation_class_name" = "...";
                "output_stream_ids" = ["reference"];
                "source_streams" = {
                    "reference_table" = {
                        "source_class_name" = "NYT::NFlow::NStaticTableConnector::TSource";
                        "parameters" = { "tables_path" = "<cluster=primary>//path/to/reference"; };
                    };
                };
            };
            "reference_loader" = {
                "computation_class_name" = "...";
                "input_stream_ids" = ["reference"];
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"; required = %true;};
                    {"name" = "key"; "type" = "uint64";};
                ];
                "external_state_managers" = {
                    "/reference_state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = { "path" = "<cluster=primary>//path/to/state"; };
                    };
                };
            };
            "enricher" = {
                "computation_class_name" = "...";
                "input_stream_ids" = ["event"];
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"; required = %true;};
                    {"name" = "key"; "type" = "uint64";};
                ];
                "external_state_joiners" = {
                    "/reference_state" = {
                        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
                        "parameters" = { "path" = "<cluster=primary>//path/to/state"; };
                    };
                };
            };
        };
    };
}
```

{% endcut %}

Полные рабочие конфиги, бинарь, `yt_sync` и интеграционные тесты &mdash; в примерах.

### Примеры

Полностью рабочие пайплайны с тестами, демонстрирующие данный подход:

- C++: [`examples/cpp/static_table_join`]({{source-root}}/yt/yt/flow/examples/cpp/static_table_join)
- Python: [`examples/python/static_table_join`]({{source-root}}/yt/yt/flow/examples/python/static_table_join)
- Java: [`examples/java/static_table_join`]({{source-root}}/yt/yt/flow/examples/java/static_table_join)
- Kotlin: [`examples/kotlin/static_table_join`]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join)

## Альтернативы {#alternatives}

`static_table` расширение &rarr; стейт &mdash; не единственный способ организовать джойн со справочником; альтернативы и их компромиссы &mdash; в таблице ниже.

#|
|| **Подход** | **Когда выбирать** | **Цена** ||
|| [#1 `static_table` расширение &rarr; стейт](#import-into-state) (выше) | нужна бизнес-логика над строкой до записи в стейт; допустима полная перезаливка | стоимость перезаливки внутри пайплайна на каждой версии ||
|| [#2 Конвертация в динамическую таблицу + symlink под external state](#alt-dyntable-symlink) | большой объём, lookup по запросу, нужна атомарная смена версии данных | сетевой lookup в динтаблицу + эксплуатация симлинка ||
|| [#3 Встроенная БД и деплой через Resource](#alt-embedded-db) | ноль сетевых обращений на джойне, объём ограничен памятью/диском воркера | сложная эксплуатация, формат и доставка БД на воркеры ||
{% if audience == "internal" %}|| [#4 Plutonium KV](#alt-plutonium) | большой справочник, высокий RPS лукапов, дёшево в рантайме | сложно эксплуатировать (квота MDS, infractl, метаданные) ||
{% endif %}|#

### Конвертация в динамическую таблицу + symlink под external state {#alt-dyntable-symlink}

В этом подходе роль стейта играет сама динамическая таблица: батч-процесс собирает новую версию справочника как сортированную динамическую таблицу `…/reference.vN`, монтирует её, а пайплайн смотрит на неё через **Cypress-симлинк** `…/current → …/reference.vN`. После сборки следующей версии симлинк атомарно перенаправляется на `…/reference.v(N+1)` (`yt link --force …/reference.v(N+1) …/current` либо `set @target_path`), и пайплайн начинает получать новые значения.

В пайплайне `path` коннектора external-state ссылается на симлинк, а не на конкретную версию. Лукап выполняется read-only [joiner](../../../flow/concepts/stateful.md#external-state-joiner)-ом (`TSimpleExternalStateJoiner`); по умолчанию joiner перечитывает значение из YT на каждый лукап, поэтому переключение симлинка сразу видно пайплайну без рестарта. Подробнее про подготовку динамической таблицы и её схему &mdash; в [sorted-dynamic-table.md](../../../flow/connectors/sorted-dynamic-table.md).

#### Примеры

- C++: [`examples/cpp/external_state_join`]({{source-root}}/yt/yt/flow/examples/cpp/external_state_join)
- Python: [`examples/python/external_state_join`]({{source-root}}/yt/yt/flow/examples/python/external_state_join)
- Java: [`examples/java/external_state_join`]({{source-root}}/yt/yt/flow/examples/java/external_state_join)
- Kotlin: [`examples/kotlin/external_state_join`]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join)

В тестах примеров `yt_sync` сначала собирает `reference.v1` и линкует `current → reference.v1`, пайплайн обогащает событие значением `v1`, затем собирается `reference.v2`, симлинк атомарно перенаправляется на новую версию, и для **того же ключа** пайплайн начинает отдавать значение `v2`.

### Встроенная БД и деплой через Resource {#alt-embedded-db}

Если справочник целиком помещается в память или на локальный SSD воркера, и доступ к нему нужен совсем без сетевых походов, имеет смысл собирать его как **встроенную БД**: батч-процесс выкатывает готовый файл/директорию, а пайплайн поднимает её как локальный лукап-движок прямо внутри процесса джоба.

{% if audience == "internal" %}Один из таких форматов в Аркадии &mdash; **vinyl**. Пример сборки и использования есть в [`ads/core/library/cpp/vinyl`]({{source-root}}/ads/core/library/cpp/vinyl): отдельный батч-процесс собирает на YT файл vinyl, выкатывает в нужное место, а пайплайн получает свежую версию файла через механизм [Resource](../../../flow/concepts/spec.md) (на каждый воркер) и читает из него на каждом сообщении.{% endif %}

Достоинства &mdash; **нулевые сетевые обращения** на джойне, минимальная задержка лукапа (только локальный диск/память), полная независимость от внешних KV-сервисов. Недостатки &mdash; объём ограничен ресурсами воркера; формат БД, её схема и совместимость версий ложатся на команду; доставку новой версии (Resource, переподготовка, чек целостности) нужно эксплуатировать самостоятельно. Подходит для относительно небольших справочников (единицы гигабайт), которые обновляются нечасто.

{% if audience == "internal" %}

### Plutonium KV {#alt-plutonium}

[Plutonium KV](https://docs.yandex-team.ru/plutonium/reference/indexes/kv/overview) &mdash; формат хранения key-value данных в оптимизированном для чтения виде плюс рантайм, позволяющий лукапить значения по сети. Лукап из пайплайна &mdash; обычный HTTP-запрос в рантайм (типичное время ответа &mdash; единицы миллисекунд).

Когда стоит выбирать: справочник большой, нагрузка на лукапы высокая, а полная перезаливка в стейт нереалистична. Данные в рантайме хранятся в трёх репликах независимо от числа потребителей, рантайм масштабируется отдельно по объёму базы и по количеству запросов, а при изменении части ключей индекс перестраивается частично &mdash; на таких профилях Plutonium KV ощутимо дешевле динтаблиц по железу.

Цена &mdash; **операционная сложность**. Нужны:

- батч-процесс сборки индекса через интерфейсы построения Plutonium;
- квота MDS/S3 под файлы индекса;
- спека инсталляции в [infractl](https://docs.yandex-team.ru/plutonium/reference/infractl/installation) (слои рантайма, схема шардирования);
- мониторинг доставки новых поколений индекса.

Пример развёртывания и эксплуатации &mdash; в документации Plutonium по ссылке выше; в Flow для лукапа достаточно обычного HTTP-клиента из computation.

{% endif %}

## См. также

- [Список коннекторов](../../../flow/connectors/about.md)
- [Sorted Dynamic Table](../../../flow/connectors/sorted-dynamic-table.md)
- [Стейтфул computations](../../../flow/concepts/stateful.md)
