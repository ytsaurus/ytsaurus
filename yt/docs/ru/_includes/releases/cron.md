## Cron


Заметки о релизах этого компонента.




**Релизы:**

{% cut "**0.0.4**" %}

**Дата релиза:** 2026-02-13


**Страница релиза:** [0.0.4](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.4)


**Helm-чарт:** [0.0.4](https://github.com/orgs/ytsaurus/packages/container/cron-chart/689408263?tag=0.0.4)


#### Новые возможности
- Добавлены cron-задачи для обработки слепков. Подробнее см. в [документации](https://ytsaurus.tech/docs/en/admin-guide/install-cron#process_master_snapshot). [fdacc5428e1cfc107c71dc1dc0212ec23c708edd]

#### Исправления
- Исправлен короткий таймаут в cron-задаче `clear-tmp` [511aa1ce1f053263182382aa2a6a33b4e2989ff3]


{% endcut %}


{% cut "**0.0.2**" %}

**Дата релиза:** 2025-04-24


**Страница релиза:** [0.0.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.2)


**Helm-чарт:** [0.0.2](https://github.com/orgs/ytsaurus/packages/container/cron-chart/401688677?tag=0.0.2)


#### Исправления

- Скрипт `prune_offline_servers` переименован в `prune_offline_cluster_nodes`.

{% endcut %}


{% cut "**0.0.1**" %}

**Дата релиза:** 2025-04-11


**Страница релиза:** [0.0.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.1)


**Helm-чарт:** [0.0.1](https://github.com/orgs/ytsaurus/packages/container/cron-chart/393601221?tag=0.0.1)


Первый релиз YTsaurus Cron.

#### Новые возможности
- Устанавливает настраиваемый набор cron-задач для обслуживания кластера YTsaurus
- Встроенные задачи включают:
    - clear_tmp_location
    - clear_tmp_files
    - clear_tmp_trash
    - prune_offline_servers
- Поддержка пользовательских задач через additionalJobs
- Безопасная настройка токена через прямое значение или Kubernetes Secret
- Настраиваемые параметры ресурсов, политики расписания и управление параллелизмом

{% endcut %}