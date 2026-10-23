## Task-прокси


Заметки о релизах этого компонента.




**Релизы:**

{% cut "**0.4.1**" %}

**Дата релиза:** 2026-08-10


**Страница релиза:** [0.4.1](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.4.1)


**Helm-чарт:** [0.4.1](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/1116607758?tag=0.4.1)


исправлены скрипты сборки

{% endcut %}


{% cut "**0.4.0**" %}

**Дата релиза:** 2026-08-10


**Страница релиза:** [0.4.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.4.0)


#### Добавлено

- Метрики Prometheus для обнаружения сервисов, запросов YTsaurus и авторизации.
- Поддержка Helm для `ServiceMonitor` / `VMServiceScrape` и правила оповещений для Prometheus или VictoriaMetrics.
- Кеширование результатов авторизации с настраиваемыми TTL, ёмкостью, параллелизмом запросов к бэкенду и упреждающим обновлением.
- Дашборд Grafana для метрик авторизации.
- Глобальные настройки тайм-аутов Task-прокси:
   - `timeouts.connectTimeoutSeconds`
   - `timeouts.routeTimeoutSeconds`
   - `timeouts.streamIdleTimeoutSeconds`
- Переопределения аннотации `task_proxy` для каждой операции:
   - `route_timeout_seconds`
   - `stream_idle_timeout_seconds`
- Документация по формату аннотации `task_proxy` и переопределениям тайм-аутов.

#### Изменено

- Улучшены логирование и метрики ошибок инфраструктуры авторизации и обнаружения сервисов.
- Добавлены повторные попытки HTTP-запросов к YTsaurus.
- Helm теперь поддерживает явное значение `ytProxy`; если оно не указано, используется сервис HTTP-прокси в пространстве имён релиза.
- Теги образов в конфигурации Helm-чарта теперь необязательны.

#### Примечания по миграции

- Для развёртываний отдельного бинарного файла `--namespace` заменён обязательным флагом `--yt-proxy`.
- Развёртывания Helm остаются совместимыми: оставьте `ytProxy` пустым, чтобы сохранить автоматически сгенерированный адрес прокси.

{% endcut %}


{% cut "**0.3.0**" %}

**Дата релиза:** 2026-03-23


**Страница релиза:** [0.3.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.3.0)


**Helm-чарт:** [0.3.0](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/752333866?tag=0.3.0)


Добавлена маршрутизация на основе тройки `operationID/alias, taskName, service` как альтернатива хешу (8-значная шестнадцатеричная строка, например `645236d8`).

Если у операции есть алиас, его можно использовать в домене: `<operationAlias>-<taskName>-<service>.<baseDomain>`, что удобно для открытия в браузере через UI. Для этого достаточно указать алиас операции, например для standalone-кластера SPYT или прямой отправки.

Это также поддерживается в заголовках: можно использовать старый вариант `x-yt-taskproxy-id: 645236d8` или новый, указывая значения из тройки в заголовках `x-yt-taskproxy-operation-id: <operationID>`, `x-yt-taskproxy-task-name: <taskName>`, `x-yt-taskproxy-service: <service>`. Если у операции есть алиас, вместо заголовка с ID операции можно использовать `x-yt-taskproxy-operation-alias: <operationAlias>`.

Обратите внимание, что алиасы операций, имена задач и сервисов должны соответствовать регулярному выражению `[a-z0-9_]{1,30}$`, чтобы избежать проблем при использовании тройки в домене.

{% endcut %}


{% cut "**0.2.3**" %}

**Дата релиза:** 2026-02-16


**Страница релиза:** [0.2.3](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.3)


**Helm-чарт:** [0.2.3](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/688915103?tag=0.2.3)


Изменено имя сервиса rest api мастера SPYT (просто `rest`, чтобы избежать использования дефисов для более стабильных FQDN).

{% endcut %}


{% cut "**0.2.2**" %}

**Дата релиза:** 2026-02-16


**Страница релиза:** [0.2.2](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.2)


**Helm-чарт:** [0.2.2](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/688652887?tag=0.2.2)


Автообнаружение сервиса restAPI мастера standalone-кластера SPYT.

{% endcut %}


{% cut "**0.2.1**" %}

**Дата релиза:** 2026-02-04


**Страница релиза:** [0.2.1](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.1)


**Helm-чарт:** [0.2.1](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/670837677?tag=0.2.1)


Поддержка заголовка `x-yt-taskproxy-id` как альтернативы доменной маршрутизации.

{% endcut %}


{% cut "**0.2.0**" %}

**Дата релиза:** 2026-01-29


**Страница релиза:** [0.2.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.0)


**Helm-чарт:** [0.2.0](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/662319261?tag=0.2.0)


Начальная версия task proxy для YTsaurus.

{% endcut %}

