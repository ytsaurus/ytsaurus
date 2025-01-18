# Спецификация консьюмера и продюсера
__ВАЖНО__: Для использования спецификации необходимо выставить настройку `Settings.use_deprecated_spec_format=False`.

Консьюмер - сущность, отслеживающая состояние чтения [очереди](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues).

Продюсер - сущность, позволяющая писать в очередь с exactly once по протоколу схожему с kafka (дедупликация через сессии и seqNo).

Очередь -
[упорядоченная динамическая таблица](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/ordered-dynamic-tables),
может быть создана как и остальные таблицы через `YtSync`.

Спецификация консьюмера в коде описана [здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/consumer.py),
см. класс `Consumer`. <br>
Спецификация продюсера в коде описана [здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/producer.py),
см. класс `Producer`.

## Таблица консьюмера
Таблица консьюмера - это обычная динамическая таблица с
[фиксированной схемой](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#data_model).

Чтобы таблица воспринималась как консьюмер, ей необходимо выставить свойство `treat_as_consumer=true`.

Таблица может быть так же и реплицированной/хаосной.

Для удобства рекомендуемые атрибуты и схема таблицы консьюмера вынесены в константы:
- схема: `yt.yt_sync.core.constants.CONSUMER_SCHEMA`
- атрибуты: `yt.yt_sync.core.constants.CONSUMER_ATTRS`

## Регистрация очереди в консьюмере
Подробности можно почитать [здесь](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#registraciya-konsyumera-k-ocheredi).

Спецификация регистрации очереди в коде описана
[здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/consumer.py), см. класс `QueueRegistration`.

_Примечание_: когда необходимо указать регистрацию для реплицированной или хаосной очереди, то кластер и путь
указываем для реплицированной/хаосной таблицы, соответственно.

## Таблица продюсера
Полностью аналогична таблице консьюмера, с небольшими отличиями:
* На ней должно быть свойство `treat_as_producer=true`.
* Рекомендуемая схема и атрибуты вынесены в константы
`yt.yt_sync.core.constants.PRODUCER_SCHEMA` и `yt.yt_sync.core.constants.PRODUCER_ATTRS`

## Использование спецификации
Спецификация передается в метод `YtSync.add_desired_consumer()`. Можно передавать как заполненный экземпляр класса
`Consumer`, так и `dict`, заполненный в соответствии со спецификацией. Аналогично для продюсеров.

```python
from yt.yt_sync import Consumer, Producer
from yt.yt_sync import QueueRegistration, SchemaSpec, Table, YtSync
from yt.yt_sync.core.constants import CONSUMER_ATTRS, CONSUMER_SCHEMA, PRODUCER_ATTRS, PRODUCER_SCHEMA

yt_sync = YtSync(settings=Settings(use_deprecated_spec_format=False, ..), ..)

# Consumer from dataclass.
yt_sync.add_desired_consumer(
    Consumer(
        table=Table(
            schema=SchemaSpec.parse(CONSUMER_SCHEMA),
            clusters={
                "primary": ClusterTable(path="consumer1_path", attributes=CONSUMER_ATTRS)
            }
        ),
        queues=[QueueRegistration(cluster="primary", path="queue1_path")]
    )
)

# Consumer from dict.
yt_sync.add_desired_consumer(
    {
        "table": {
            "schema": CONSUMER_SCHEMA,
            "clusters": {"primary": {"path": "consumer2_path", "attributes": CONSUMER_ATTRS}}
        },
        "queues": [{"cluster": "primary", "path": "queue2_path", "vital": True}]
    }
)

# Producer from dataclass.
yt_sync.add_desired_producer(
    Producer(
        table=Table(
            schema=SchemaSpec.parse(PRODUCER_SCHEMA),
            clusters={
                "primary": ClusterTable(path="producer1_path", attributes=PRODUCER_ATTRS)
            }
        ),
    )
)

# Producer can be created from dict in the same way as consumer.
```
