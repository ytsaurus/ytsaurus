# Параметры запросов

В данном разделе собрана информация о дополнительных параметрах запросов. Подробнее о командах читайте в разделе [Команды](../../api/commands.md).

## Предварительные условия { #conditions }

Ниже представлен список команд, для которых можно указать условия успешного выполнения:

- `set`;
- `remove`;
- `create`;
- `lock`;
- `copy`;
- `move`;
- `link`;
- `create`.

Условия бывают двух видов:

- `prerequisite_revisions` — набор указаний вида `path`, `transaction_id`, `revision`. Запрос будет выполнен, только если все указанные пути с учетом заданных транзакций существуют, а ревизии соответствуют указанным;
- `prerequisite_transactions` — список [транзакций](../../user-guide/storage/transactions.md), которые обязаны существовать на момент выполнения запроса.

Пример:

```python
revision = client.get("//tmp/my_table/@revision")
#... some calculations ...
revision_parameter = yt.create_revision_parameter(path="//tmp/my_table", revision=revision)
revision_client = yt.create_client_with_command_params(prerequisite_revisions=[revision_parameter])
revision_client.move("//tmp/transformed_table", "//tmp/my_table")
```

## Источник данных для чтения { #data-source }

В [немутирующих](../../api/commands.md#concepts) запросах к [Кипарису]{% if audience == "internal" %}(../../user-guide/storage/cypress.md){% else %}(../../user-guide/storage/cypress.md){% endif %} можно указать источник данных для чтения при помощи параметра `read_from`.




Популярным значением параметра `read_from` является `cache`.  В таком случае чтение ответа будет происходить из кеша (который обычно расположен на выделенных машинах кластера), что позволит снизить нагрузку на мастер-сервер. Это важно при выполнении большого количества тяжелых запросов к Кипарису: например, необходимо обойти большое поддерево.

Пример:

```python
for obj in client.search("//tmp/my_table", read_from="cache"):
    if "abc" in str(obj):
        ...
```