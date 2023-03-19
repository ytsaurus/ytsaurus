При первом старте, кластерные ноды регистрируются в кипарисе, создавая объект типа "cluster_node" в //sys/cluster_nodes.

Их атрибуты можно прочитать выполнив команду:

    yt --proxy cluster.fqdn get //sys/cluster_nodes/fqdn:port/@

### Список атрибутов:
#### @state
string, неизменяемый атрибут

Показывает состояние кластерной ноды с точки зрения {{product-name}} мастера, может принимать значения:
- offline - Нода считается незарегистрированной в кластере, т.е. выключенной.
                Причин перехода в состояние offline может быть несколько.
                - Если {{product-name}} мастер перестает получать регулярные heartbeats, то нода переводится в offline.
                - Выставлен атрибут @banned.

- online - Зарегистрирована в кластере, отправляет регулярно все нужные heartbeats.

- mixed - В multicell кластере такое состояние ноды указывает, что регистрация прошла на части mastercells, а на части еще нет.

    Другие значения нужно указывать?
    https://a.yandex-team.ru/arcadia/yt/yt/ytlib/node_tracker_client/public.h?#L38

#### @last_seen_time
string, utc time, неизменяемый атрибут

Время получения последнего heartbeat от ноды мастером.

#### @register_time
string, utc time, неизменяемый атрибут

Время регистрации, т.е. перехода @state из offline в online.

#### @banned
bool (%true %false), изменяемый атрибут

При выставлении в %true {{product-name}} мастер принудительно выключает ноду из кластера. @state меняется на offline.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@banned %true


#### @disable_write_sessions
bool (%true %false), изменяемый атрибут

При выставлении в %true перестают создаваться новые write сессии на эту ноду. Т.е. выключается запись новых чанков на эту ноду.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@disable_write_sessions %true


#### @disable_scheduler_jobs
bool (%true %false), изменяемый атрибут

При выставлении в %true на ноде перестают запускаться новые mr jobs. При этом уже запущенные задачи продолжают выполняться.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@disable_scheduler_jobs %true


#### @disable_tablet_cells
bool (%true %false), изменяемый атрибут

При выставлении в %true на ноде выключаются tablet cell slots, все запущенные tablet cell выключаются и переносятся на другие ноды кластера.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@disable_tablet_cells %true


#### @decommissioned
bool (%true %false), изменяемый атрибут

Атрибут нужен для безопасного отключения ноды из кластера.
При выставлении атрибута в %true, мастер запускает декомиссию ноды.
Это значит, что будет создана копия всех чанков этой ноды на других нодах кластера.
Будут выключены write сессии, запуск новых mr jobs и унесены tablet cells.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@decommissioned %true


#### @rack
string, изменяемый атрибут

Задает имя rack в котором размещена нода. Перед выставлением у ноды, объект rack должен быть уже создан.
Важный атрибут, определяющий на каких нодах будут размещены чанки таблиц.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@rack "RACK-A"


#### @host
string, неизменяемый атрибут?

В атрибуте указан fqdn:port ноды


#### @data_center
string, изменяемый атрибут

Задает имя data_center в котором размещена нода. Перед выставлением у ноды, объект data_center должен быть уже создан.
Важный атрибут для кластера, размещенного в нескольких датацентрах.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@data_center "REGION-A"


#### @user_tags
list[string], изменяемый атрибут

Массив с тегами ноды, которые проставлены пользователем в кипарисе.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@user_tags/end "10gbps" - добавить новый тег в конец массива.
    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@user_tags/-1 "1gbps" - поменять последний тег.
    yt --proxy cluster.fqdn get //sys/cluster_nodes/fqdn:port/@user_tags/0 - получить первый тег.
    yt --proxy cluster.fqdn remove //sys/cluster_nodes/fqdn:port/@user_tags/0 - удалить первый тег.

#### @tags
list[string], неизменяемый атрибут

Массив тегов, состоящий из автоматических тегов, тегов из конфигурационного файла и @user_tags.
В теги автоматически добавляется @host, @rack и @data_center.


#### @version
string, неизменяемый атрибут

Версия yt node на этой ноде.


#### @job_proxy_build_version
string, неизменяемый атрибут

Версия job_proxy на этой ноде.

#### @full
bool, неизменяемый атрибут

Индикатор, что все chunk_store локации полностью заполнены.

#### @alerts
list, неизменяемый атрибут

Список ошибок, которые есть на ноде. Например, если на одной из локаций будет поймана IO error, локация будет отключена, а в alerts появится запись об этой ошибке.

#### @alert_count
int, неизменяемый атрибут

Число ошибок в @alerts

#### @annotations
dict, неизменяемый атрибут

Атрибут @annotations добавляется из конфигурационного файла, из ключа cypress_annotations. В процессе генерации конфигурационного файла на ноде, в этот ключ можно записать различную информацию, чтобы после она была доступна через cypress.

Пример:

    yt --proxy cluster.fqdn get //sys/cluster_nodes/fqdn:port/@annotations/cpu_model
    "Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz"


#### @resource_limits
dict, неизменяемый атрибут

В этом атрибуте хранятся различные числовые характеристики доступных на ноде ресурсов.

Пример:

    yt --proxy cluster.fqdn get //sys/cluster_nodes/fqdn:port/@resource_limits
    {
        "user_slots" = 500;
        "cpu" = 126.;
        "gpu" = 0;
        "user_memory" = 444964098436;
        "system_memory" = 8966070209;
        "network" = 100;
        "replication_slots" = 64;
        "replication_data_size" = 10737418240;
        "merge_data_size" = 10737418240;
        "removal_slots" = 4096;
        "repair_slots" = 8;
        "repair_data_size" = 2147483648;
        "seal_slots" = 16;
        "merge_slots" = 4;
        "autotomy_slots" = 4;
        "vcpu" = 126.;
    }

#### @resource_limits_overrides
dict, изменяемый атрибут

Атрибут позволяет переопределить часть доступных ноде ресурсы.

Пример:

    yt --proxy cluster.fqdn set //sys/cluster_nodes/fqdn:port/@resource_limits_overrides '{repair_slots=100;}'
    yt --proxy cluster.fqdn get //sys/cluster_nodes/fqdn:port/@resource_limits_overrides
    {
        "repair_slots" = 16;
    }
    yt --proxy cluster.fqdn remove //sys/cluster_nodes/fqdn:port/@resource_limits_overrides/repair_slots

#### @resource_usage
dict, неизменяемый атрибут

Атрибут показывает текущее потребление ресурсов ноды.


Уще нужно:
multicell_states
lease_transaction_id
statistics
addresses
flavors ?
tablet_slots
cellars ?
io_weights ?
chunk_replica_count
destroyed_chunk_replica_count
push_replication_queue_size
pull_replication_queue_size
pull_replication_chunk_count
consistent_replica_placement_token_count
chunk_locations
use_imaginary_chunk_locations
