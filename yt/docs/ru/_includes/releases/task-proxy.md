## Task-proxy

Заметки о релизах этого компонента.

**Релизы:**

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