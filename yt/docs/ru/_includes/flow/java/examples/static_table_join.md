# Static Table Join в {{product-name}} Flow (Java и Kotlin)

Пайплайн загружает статический справочник во внешний стейт и обогащает события из очереди по общему ключу. Java и Kotlin используют один SDK и одинаковую топологию пайплайна.

[Исходники Java]({{source-root}}/yt/yt/flow/examples/java/static_table_join)

[Исходники Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join)

## Структура пайплайна {#structure}

- `reference_reader` читает статический справочник;
- `reference_loader` нормализует значения и записывает их в `/reference_state` через `external_state_managers`;
- `event_reader` читает события из очереди;
- `enricher` читает `/reference_state` через `external_state_joiners` и публикует успешные совпадения в `enriched`.

Менеджер и joiner ссылаются на одну динамическую таблицу стейта. Её ключи должны совпадать с `group_by_schema` компьютейшенов. См. [спеку Java]({{source-root}}/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/resources/pipeline.yson) или [спеку Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/resources/pipeline.yson).

## Загрузка справочника {#load}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/java/tech/ytsaurus/flow/examples/statictablejoin/ReferenceLoaderFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/kotlin/tech/ytsaurus/flow/examples/statictablejoin/ReferenceLoader.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

## Обогащение событий {#enrich}

Read-only дескриптор выбирает строку по ключу текущего сообщения. Если строка или `normalized_name` не найдены, пример не публикует результат.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/java/tech/ytsaurus/flow/examples/statictablejoin/EnricherFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/kotlin/tech/ytsaurus/flow/examples/statictablejoin/Enricher.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

Оба компьютейшена находятся по `@FlowComputation`; схемы трёх стримов, которые используют нативные источники и компаньонские функции, объявлены в спеке пайплайна.

## См. также

- [External State (Java)](../../../../flow/java/external-state.md)
- [Пример External State Join](../../../../flow/java/examples/external_state_join.md)
