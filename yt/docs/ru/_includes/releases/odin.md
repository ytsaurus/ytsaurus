## Odin


Распространяется в виде docker-образа. Инструкция по установке доступна в [руководстве по установке Odin](https://ytsaurus.tech/docs/ru/admin-guide/install-odin).




**Релизы:**

{% cut "**0.0.10**" %}

**Дата релиза:** 2026-06-01


**Страница релиза:** [0.0.10](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.10)


**Docker-образ:** [0.0.10](https://github.com/orgs/ytsaurus/packages/container/odin-chart/907524343?tag=0.0.10)


#### Новые возможности:
- Добавлена генерация `PrometheusRule` для проверок Odin. Подробнее читайте в [документации](https://ytsaurus.tech/docs/en/admin-guide/install-odin) [f38559d4b90d72d63efae09085895d9b86045767]
- Алерт о недостаточных ресурсах узлов выделен в отдельную проверку `scheduler_alerts_nodes_with_insufficient_resource_limits` [8cd9141d5012e949aa42d30dad1506cbb5157684]


{% endcut %}


{% cut "**0.0.9**" %}

**Дата релиза:** 2026-04-20


**Страница релиза:** [0.0.9](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.9)


**Docker-образ:** [0.0.9](https://github.com/orgs/ytsaurus/packages/container/odin-chart/810778603?tag=0.0.9)


#### Новые возможности
- Добавлены новые проверки Odin: `cypress_commands`, `read_static_table_commands`, `write_static_table_commands` [19f4319f81ebe8541cbbb1c2e90c3624f29c0a8e, 8d758f30284fb63c24aa1a4f354121db985ff1d3]

#### Исправления:
- Исправлены проблемы с привязкой IPv4/IPv6 dual-stack [545338d48c649f9248fb9fd4a131297771410c60]

#### Критические изменения
- Значение `host: "::"` больше недействительно. Чтобы слушать все интерфейсы, используйте `host: "*"`. Если вы не переопределяли параметр `host` в вашем `values.yaml`, никаких действий не требуется [545338d48c649f9248fb9fd4a131297771410c60]
- Параметр `config.webservice.debug` удалён из `values.yaml` [545338d48c649f9248fb9fd4a131297771410c60]


{% endcut %}


{% cut "**0.0.8**" %}

**Дата релиза:** 2026-03-23


**Страница релиза:** [0.0.8](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.8)


**Docker-образ:** [0.0.8](https://github.com/orgs/ytsaurus/packages/container/odin-chart/752466721?tag=0.0.8)


#### Исправления
- Исправлена проблема с созданием одноразовых сокетов [c0d18d14808f9e086ba0b2d568f26174d9387fb9]
- Исправлено некорректное поведение `check_virtual_map_size` при неудачных проверках Odin [399dcfe92e222fe2fae5a963323e81a511f18510]
- Исправлены множественные проблемы в проверках виртуальных карт Odin [3e812ec48ba2016e1d73a8db7c2c9b1601517b66, c37a2a70c6e55e62348bec5cbf07d54375129498, ca3698d07e7ddac28194cbc2e69f8ff583018c50]
- Добавлены повторные попытки при исключениях `all writes disabled` во время записи в хранилище, чтобы предотвратить перезапуски Odin, вызванные перегрузкой бандлов таблет-селлов [178f0a3df344e341ad420c479a3e06fad131e9d6]
- Исправлен недостаточный таймаут для проверки Odin `stuck_missing_part_chunks` [9eca40a6f830a144023544ccd5b0bb07cb6d046b]


{% endcut %}


{% cut "**0.0.7**" %}

**Дата релиза:** 2026-02-02


**Страница релиза:** [0.0.7](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.7)


**Docker-образ:** [0.0.7](https://github.com/orgs/ytsaurus/packages/container/odin-chart/667861164?tag=0.0.7)


#### Исправления
- Исправлена проверка `Missing Part Chunks`
- Проверка `Quorum Missing Chunks` включена по умолчанию
- Проверка `Inconsistently Placed Chunks` включена по умолчанию


{% endcut %}


{% cut "**0.0.6**" %}

**Дата релиза:** 2026-01-26


**Страница релиза:** [0.0.6](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.6)


**Docker-образ:** [0.0.6](https://github.com/orgs/ytsaurus/packages/container/odin-chart/657705318?tag=0.0.6)


#### Новые возможности
- Добавлена проверка `system_quotas_yt_job_logs`
- Добавлены проверки Odin для сэмплов чанков

#### Исправления
- Исправлена проверка `tmp_node_count` при отсутствии директории
- Исправлена невозможность поднятия сразу после падения
- Исправлено неверное имя атрибута в `queue_agent_alerts`


{% endcut %}


{% cut "**0.0.5**" %}

**Дата релиза:** 2025-11-19


**Страница релиза:** [0.0.5](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.5)


**Docker-образ:** [0.0.5](https://github.com/orgs/ytsaurus/packages/container/odin-chart/582216203?tag=0.0.5)


Мы рады сообщить о первом публичном релизе **Odin** — сервиса мониторинга для кластеров YTsaurus.

Пошаговая инструкция по установке доступна здесь: [Руководство по развёртыванию Odin](https://ytsaurus.tech/docs/en/admin-guide/install-odin)

{% endcut %}