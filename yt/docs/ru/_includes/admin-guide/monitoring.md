# Мониторинг

## Настройка Prometheus
### Пререквизиты
- запущенный кластер {{product-name}};
- установленный Odin [по инструкции](../../admin-guide/install-odin.md);
- установленный prometheus-operator [по инструкции](https://github.com/prometheus-operator/prometheus-operator#quickstart).

### Установка

Сервисы для мониторинга создаются оператором {{product-name}} и helm-чартов Odin автоматически.

Для сбора метрик необходимо:
1. Создать [ServiceMonitor](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_monitor.yaml) для сбора метрик с компонент {{product-name}}.
2. Создать ServiceMonitor для метрик с Odin, указав `metrics.serviceMonitor.enable: true` в values для helm-chart (по умолчанию он не создается).
3. Создать [сервисный аккаунт](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_service_account.yaml).
4. Выдать созданному аккаунту [роль](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus_role_binding.yaml).
5. [Создать Prometheus](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/prometheus/prometheus.yaml).

## Способы мониторинга и важные метрики {{product-name}}

Подходы к организации мониторинга технических систем можно условно разделить на два основных направления, назовем их качественный и количественный мониторинг.

### Качественный мониторинг { #odin }

Качественный мониторинг проверяет, способна ли система выполнять свою базовую функциональность. Для этого запускаются тестовые задания или, по-другому, проверки. Такие проверки возвращают качественное описание проверяемой системы:

- работает в штатном режиме (OK);
- не работает (CRITICAL);
- работает, но с отклонениями от штатного режима (WARNING).

Список наиболее важных качественных проверок {{product-name}}:

- `sort_result` — запускает sort операцию над небольшой тестовой таблицей, ждет её завершения и проверяет результат сортировки;
- `map_result` — запускает map операцию с небольшой тестовой таблицей на входе, ждет её завершения и проверяет результат map;
- `dynamic_table_commands` — проверяет работоспособность динамических таблиц, проходя весь жизненный цикл динамической таблицы: создаёт тестовую таблицу, монтирует таблицу, записывает и читает данные из таблицы, после чего отмонтирует и удаляет таблицу.

Существуют проверки сообщений самодиагностики подсистем {{product-name}} кластера:

- `master_alerts` — проверяет значение атрибута `//sys/@master_alerts`;
- `scheduler_alerts` — проверяет значение атрибута `//sys/scheduler/@alerts`;
- `controller_agent_alerts` — вычитывает атрибут alerts из всех узлов из `//sys/controller_agents/instances`;
- `lost_vital_chunks` — проверяет, что счетчик lost_vital_chunks равен нулю из `//sys/lost_vital_chunks/@count`. Подробнее про vitality можно прочитать [по ссылке](../../user-guide/storage/chunks.md#vitality);
- `tablet_cells` — проверяет, что в динамических таблицах нет tablet cells в статусе failed;
- `quorum_health` — проверяет, что все мастера работают и находятся в кворуме.
- `clock_quorum_health` (отключена по умолчанию) - проверяет, что все clock-серверы работают и находятся в кворуме;
- `suspicious_jobs` - проверяет, что на кластере нет джобов, которые тормозят и не происходит никакого прогресса;
- `tablet_cell_snapshots` - проверяет, что снепшоты таблет-целлов успевают формироваться;
- `scheduler_uptime` - проверяет, что шедулер работает стабильно и не переподключается к мастеру;
- `controller_agent_count` - проверяет, что есть достаточное число подключенных контроллер-агентов;
- `controller_agent_uptime` - проверяет, что контроллер-агенты работают стабильно и не переподключаются к шедулеру;
- `operations_snapshots` - проверяет, что для операций регулярно строятся снепшоты;
- `operations_count` - проверяет, что не стало слишком много операций в Кипарисе;
- `dynamic_table_replication` (отключена по умолчанию) - проверяет, что на кластере работает репликация для реплицированных динамическик таблиц;
- `register_watcher` - проверяет, ноды кластера работают стабильно и не переподключаются к мастеру слишком часто;
- `tmp_node_count` - проверяет, что количество объектов в директориях в `//tmp` не превышает разумного порога;
- `destroyed_replicas_size` - проверяет, что система успевает подчищать реплики неактуальных (удаленных) чанков;
- `query_tracker_yql_liveness` -  проверяет работоспособность YQL запросов через Query Tracker
- `query_tracker_chyt_liveness` (отключена по умолчанию) - проверяет работоспособность CHYT запросов через Query Tracker
- `query_tracker_ql_liveness` (отключена по умолчанию) - проверяет работоспособность YT QL запросов через Query Tracker.
- `query_tracker_dq_liveness` (отключена по умолчанию) - проверяет живость DQ;
- `controller_agent_operation_memory_consumption` - проверяет потребление память операциями в контроллер-агентах;
- `discovery` (отключена по умолчанию) - проверяет, что запущено достаточно число инстансов discovery серверов;
- `master` - проверяет, что успешно выполняется `get` для корня кипариса;
- `medium_balancer_alerts` - проверяет, что Medium Balancer работает без ошибок.
- `oauth_health` - проверяет, что токен Odin успешно авторизовывается;
- `operations_archive_tablet_store_preload` - проверяет, что системные таблицы, необходимые для работы архива операций, успешно подгружаются в память;
- `proxy` - проверяет, что запущены и работают HTTP proxy для исполнения тяжелых запросов;
- `queue_api` (отключена по умолчанию) - создает очередь, консьюмер и пытается записать/прочитать данные, то есть проверяет, что API очередей работает штатно;
- `queue_agent_alerts` (отключена по умолчанию) - проверяет, что запущено достаточное число инстансов Queue Agent и на них нет алертов;
- `query_tracker_alerts` - проверяет, что запущено достаточное число инстансов Query Tracker и на них нет алертов;
- `scheduler` - проверяет, что шедулер запущен (его орхидея доступна);
- `scheduler_alerts_jobs_archivation` - проверяет, что нет ошибок архивации джобов на нодах;
- `scheduler_alerts_update_fair_share` - проверяет, что хватает вычислительных ресурсов для исполнения гарантий;
- `stuck_missing_part_chunks`, `missing_part_chunks` - проверяют, что нет erasure-данных, которые долгое время недоступны и не могут восстановиться;
- `unaware_nodes` (отключена по умолчанию) - проверяет, что на кластере нет нод, которые не привязаны ни к какому rack;
- `operations_satisfaction` - проверяет, что операции, запущенные на кластере, получают положенную долю ресурсов.

Реализацию данных проверок можно посмотреть в [Odin](https://github.com/ytsaurus/ytsaurus/tree/main/yt/odin).

В текущей реализации odin не поддерживает алерты, только запуск проверок с сохранением результатов в динамическую таблицу с отображением в UI.

### Количественный мониторинг

Для количественного мониторинга с различных подсистем снимаются метрики в виде временных рядов (Time series data). С их помощью можно оценить тенденции нагрузки и потребляемых ресурсов и исправить ситуацию до появления проблем на кластере, заметных пользователям.

Некоторые важные количественные метрики в Prometheus:

- `yt_resource_tracker_total_cpu{service="yt-master", thread="Automaton"}` — загрузка Automaton, основного исполняющего потока мастера. Загрузка не должна быть больше 90%;
- `yt_resource_tracker_memory_usage_rss{service="yt-master"}` — использование мастером памяти. Порог должен быть чуть меньше половины памяти контейнера. Мастер делает fork для записи снапшота, потому должен быть двойной запас по памяти;
- `yt_resource_tracker_memory_usage_rss` — важная метрика и для других компонентов кластера, позволяющая оценить тенденцию потребления памяти для избежания событий out of memory;
- `yt_changelogs_available_space{service="yt-master"}` — свободное место под changelogs. В конфигурационном файле мастера есть настройка хранения changelogs, свободного места должно быть больше, чем эта настройка;
- `yt_snapshots_available_space{service="yt-master"}` — аналогично для snapshots;
- `yt_logging_min_log_storage_available_space{service="yt-master"}` — аналогично для logs.

Необходимо следить за ростом чанков в «нехороших» состояниях:

- `yt_chunk_server_underreplicated_chunk_count` — число чанков, имеющих на дисках кластера меньше реплик, чем указано в атрибуте replication_factor. Постоянный рост underreplicated может говорить о том, что настройки [media](../../user-guide/storage/media.md) не позволяют выполнять условия replication_factor в текущей конфигурации кластера. Число underreplicated чанков можно посмотреть в //sys/underreplicated_chunks/@count;
- `yt_chunk_server_parity_missing_chunk_count` и `yt_chunk_server_data_missing_chunk_count` — постоянный рост parity_missing_chunk или data_missing_chunk может быть вызван тем, что процесс починки erasure не успевает за поломкой при выходе хостов или дисков из строя. Число parity_missing и data_missing чанков можно посмотреть в //sys/parity_missing_chunks/@count и //sys/data_missing_chunks/@count.

Важно отслеживать [квоты](../../user-guide/storage/quotas.md) системных аккаунтов, таких как sys, tmp, intermediate, а также пользовательские квоты продуктовых процессов. Пример для аккаунта sys:

- `yt_accounts_chunk_count{account="sys"} / yt_accounts_chunk_count_limit{account="sys"}` — процент использования чанковой квоты;
- `yt_accounts_node_count{account="sys"} / yt_accounts_node_count_limit{account="sys"}` — процент использования квоты на число узлов Кипариса;
- `yt_accounts_disk_space_in_gb{account="sys"} / yt_accounts_disk_space_limit_in_gb{account="sys"}` — процент использования дисковой квоты.

