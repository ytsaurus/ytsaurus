## Queue Agent

Queue Agent - это сервис, который следит за всем очередями и консьюмерами на кластерах и делает для них следующие вещи:

1. Собирает метрики (скорость чтения/записи, лаг чтения и т.п.) - пример метрики: https://nda.ya.ru/t/mVjHJcL2763xWa;
2. Тримает очереди, удаляя строки, которые уже не нужны;
3. Откладывает периодические снепшоты очередей в статические таблицы.

Подробнее можно почитать в [документации](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#queue-agent).

### Инсталяции

Есть 4 инсталяции, тип инсталяции называется stage. У каждой очереди/консьюмера есть `@queue_agent_stage`, обозначающий то, какая инсталяция им занимается.

| | production | testing | prestable | experimental |
| - | - | - | - | - |
| Cервис в nanny | [yt\_queue\_agent\_production](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_queue_agent_production) |  [yt\_queue\_agent\_testing](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_queue_agent_testing) | [yt\_queue\_agent\_prestable](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_queue_agent_prestable) | [yt\_queue\_agent\_experimental](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_queue_agent_experimental) |
| Состояние | [markov](https://beta.yt.yandex-team.ru/markov/navigation?path=//sys/queue_agents) | [pythia](https://beta.yt.yandex-team.ru/pythia/navigation?path=//sys/queue_agents) | [hume](https://beta.yt.yandex-team.ru/hume/navigation?path=//sys/queue_agents) | [ada](https://beta.yt.yandex-team.ru/ada/navigation?path=//sys/queue_agents) |

### Выкатка

Queue Agent релизится из стабильной ветки аналогично остальным серверным компонентам YT. Достаточно создать снепшот в nanny и перевести его в active.

Если менялась схема таблиц состояния, то перед обновлением нужно провести миграцию, запустив [`init_queue_agent_state`](https://a.yandex-team.ru/arcadia/yt/python/yt/environment/init_queue_agent_state.py) из соответствующей релизной ветки.

