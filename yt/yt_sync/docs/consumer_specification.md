# Спецификация консюмера
__ВАЖНО__: Для использования спецификации необходимо выставить настройку `Settings.use_deprecated_spec_format=False`.
Консюмер - сущность, отслеживающая состояние [очереди](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues).

Очередь -
[упорядоченная динамическая таблица](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/ordered-dynamic-tables),
может быть создана как и остальные таблицы через `YtSync`.

`YtSync` управляет консюмерами только если выставлена настройка `Settings.ensure_native_queue_consumers=True`.

Спецификация консюмера в коде описана [здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/consumer.py),
см. класс `Consumer`.

## Таблица консюмера
Таблица консюмера - это обычная динамическая таблица с
[фиксированной схемой](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#data_model).

Чтобы таблица воспринималась как консюмер, ей необходимо выставить свойство `treat_as_consumer=true`.

Таблица может быть так же и реплицированной/хаосной.

Для удобства рекомендуемые атрибуты и схема таблицы консюмера вынесены в константы:
- схема: `yt.yt_sync.core.constants.CONSUMER_SCHEMA`
- атрибуты: `yt.yt_sync.core.constants.CONSUMER_ATTRS`


## Регистрация очереди в консюмере
Подробности можно почитать [здесь](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#registraciya-konsyumera-k-ocheredi).

Спецификация регистрации очереди в коде описана
[здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/consumer.py), см. класс `QueueRegistration`.

_Примечание_: когда необходимо указать регистрацию для реплицированной или хаосной очереди, то кластер и путь
указываем для реплицированной/хаосной таблицы, соответственно.

## Использование спецификации
Спецификация передается в метод `YtSync.add_desired_consumer()`. Можно передавать как заполненный экземпляр класса
`Consumer`, так и `dict`, заполненый в соответствии со спецификацией.

```python
from yt.yt_sync import Consumer
from yt.yt_sync import QueueRegistration
from yt.yt_sync import SchemaSpec
from yt.yt_sync import Table
from yt.yt_sync import YtSync
from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA

yt_sync = YtSync(settings=Settings(use_deprecated_spec_format=False, ..), ..)

# from dataclass
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

# from dict
yt_sync.add_desired_consumer(
    {
        "table": {
            "schema": CONSUMER_SCHEMA,
            "clusters": {"primary": {"path": "consumer2_path", "attributes": CONSUMER_ATTRS}}
        },
        "queues": [{"cluster": "primary", "path": "queue2_path", "vital": True}]
    }
)
```
