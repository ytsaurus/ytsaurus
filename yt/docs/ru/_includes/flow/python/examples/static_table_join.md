# Static Table Join в {{product-name}} Flow (Python)

Пайплайн загружает статический справочник во внешний стейт и обогащает события из очереди по общему ключу.

[Исходники]({{source-root}}/yt/yt/flow/examples/python/static_table_join)

## Структура пайплайна {#structure}

- `reference_reader` читает статическую таблицу;
- `reference_loader` нормализует значения и записывает их в `/reference_state`;
- `event_reader` читает события из очереди;
- `enricher` выполняет read-only lookup через joiner `/reference_state` и пишет успешные совпадения в `enriched`.

Менеджер и joiner ссылаются на одну динамическую таблицу стейта. Её ключи должны совпадать с `group_by_schema` компьютейшенов. См. [спеку пайплайна]({{source-root}}/yt/yt/flow/examples/python/static_table_join/pipeline.yson).

## Загрузка и join {#implementation}

Загрузчик получает стейт через `ctx.external_state(...)` и сохраняет нормализованное значение. Обогатитель вызывает `ctx.joined_external_state(...)`; если `normalized_name` отсутствует, он не публикует результат.

{% code '/yt/yt/flow/examples/python/static_table_join/__main__.py' lang='python' lines='[BEGIN reference_loader]-[END reference_loader]' %}

{% code '/yt/yt/flow/examples/python/static_table_join/__main__.py' lang='python' lines='[BEGIN enricher]-[END enricher]' %}

## См. также

- [External State (Python)](../../../../flow/python/external-state.md)
- [Пример External State Join](../../../../flow/python/examples/external_state_join.md)
