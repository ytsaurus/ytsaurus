# External State Join в {{product-name}} Flow (Java и Kotlin)

Пайплайн обогащает события из очереди данными из заранее созданного динамического справочника. Java и Kotlin используют один read-only API присоединённого стейта.

[Исходники Java]({{source-root}}/yt/yt/flow/examples/java/external_state_join)

[Исходники Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join)

## Структура пайплайна {#structure}

`event_reader` публикует стрим `event`. Компаньонский компьютейшен `lookup_join` группирует сообщения по `key`, читает `/reference` через `external_state_joiners` и передаёт успешные совпадения в `enriched`. Queue-sink сохраняет результат. `path` joiner-а может указывать на Cypress-ссылку, чтобы переключать справочник без перезапуска пайплайна.

См. [спеку Java]({{source-root}}/yt/yt/flow/examples/java/external_state_join/external_state_join/src/main/resources/pipeline.yson) или [спеку Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join/external_state_join/src/main/resources/pipeline.yson).

## Чтение присоединённого стейта {#lookup}

`StateDescriptors.externalReadOnly("/reference")` связывает process-функцию с joiner-ом под тем же именем. Аксессор выбирает строку по ключу текущего сообщения. Если строка не найдена, значение `name` отсутствует и пример не публикует результат. Присоединённый аксессор не предоставляет путь записи.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/external_state_join/external_state_join/src/main/java/tech/ytsaurus/flow/examples/externalstatejoin/LookupJoinFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/external_state_join/external_state_join/src/main/kotlin/tech/ytsaurus/flow/examples/externalstatejoin/LookupJoin.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

Функция регистрируется как `lookup_join` через `@FlowComputation`; схемы стримов `event` и `enriched` объявлены в спеке пайплайна.

## См. также

- [External State (Java)](../../../../flow/java/external-state.md)
- [Пример Static Table Join](../../../../flow/java/examples/static_table_join.md)
