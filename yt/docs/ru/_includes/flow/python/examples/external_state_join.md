# External State Join в {{product-name}} Flow (Python)

Пайплайн обогащает события из очереди данными из заранее созданной динамической таблицы через read-only external state joiner.

[Исходники]({{source-root}}/yt/yt/flow/examples/python/external_state_join)

## Структура пайплайна {#structure}

`event_reader` публикует стрим `event`. Компаньонский компьютейшен `lookup_join` группирует сообщения по `key`, читает `/reference` через `external_state_joiners` и передаёт успешные совпадения в `enriched`. Queue-sink сохраняет выходные сообщения. `path` joiner-а может указывать на Cypress-ссылку, чтобы переключать справочник без перезапуска пайплайна.

См. [спеку пайплайна]({{source-root}}/yt/yt/flow/examples/python/external_state_join/pipeline.yson).

## Чтение присоединённого стейта {#lookup}

`ctx.joined_external_state("/reference", message)` выбирает строку по ключу сообщения. Возвращаемый аксессор доступен только для чтения. Если строка не найдена, он содержит пустой payload: `get("name")` возвращает `None`, и пример не публикует результат.

{% code '/yt/yt/flow/examples/python/external_state_join/__main__.py' lang='python' lines='[BEGIN lookup_join]-[END lookup_join]' %}

## См. также

- [External State (Python)](../../../../flow/python/external-state.md)
- [Пример Static Table Join](../../../../flow/python/examples/static_table_join.md)
