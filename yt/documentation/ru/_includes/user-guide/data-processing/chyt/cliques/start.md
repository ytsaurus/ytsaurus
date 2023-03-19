# Как запустить приватную клику

*Приватная клика* — это клика, доступ к которой имеют только определенные пользователи или подразделения.

## Условия запуска { #conditions }
Для запуска приватной клики понадобится выделенный вычислительный пул с гарантиями (`strong_guarantee`) по CPU. Если его нет, следует:

- узнать у коллег, можно ли найти [вычислительный пул](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) с недоизрасходованными ресурсами;
- сделать заказ новых ресурсов на будущее;
- запустить клику без гарантий (не рекомендуется).

{% if audience == "public" %} {% else %}
{% note warning "Внимание" %}

Не рекомендуется запускать приватную клику в пуле **Research** (в частности, это случится, если не указать pool при запуске клики).

В данном пуле отсутствуют гарантии по CPU. Как следствие, джобы операции, в которой запущена клика, могут [вытесняться](../../../../../user-guide/data-processing/chyt/cliques/resources.md) в произвольные моменты времени. При вытеснении все выполняющиеся запросы аварийно завершаются.


{% endnote %}{% endif %}

## Порядок запуска { how-start }

1. Установить [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) в составе пакета `ytsaurus-client`.

2. Запустить клику. Ниже приведен пример клики из пяти инстансов с настройками по умолчанию (16 ядер на инстанс) в пуле `chyt`. Клики идентифицируются по алиасам &mdash; уникальным именам, начинающимся с `*`. В примере запускается клика с алиасом `*example`.

{% note tip "Заказ CPU квоты" %}

Для поднятия клики с `N` инстансами на кластере `<cluster_name>` необходимо:
- `<cluster_name> <pool_tree_name> <running_operations> ` — `1` (клика – это одна {{product-name}} операция, которая запущена всегда);
- `<cluster_name> <pool_tree_name> <total_operations>` — `1` (должен быть больше либо равен лимиту на кол-во выполняющихся операций);
- `<cluster_name> <pool_tree_name> <CPU strong guarantee>` — `N * 16` (по умолчанию в 1 инстансе 16 ядер).


{% endnote %}

Чтобы не указывать в каждой команде кластер {{product-name}} через аргумент `--proxy`, первым шагом установите значение по умолчанию через переменную окружения:

```bash
export YT_PROXY=<cluster_name>
```

Далее необходимо создать клику, используя команду из [CHYT Controller CLI](../../../../../user-guide/data-processing/chyt/cliques/controller.md):

```bash
yt clickhouse ctl create chyt_example_clique
```
После создания клики необходимо ее настроить, задать все необходимые опции. Для всех клик есть одна обязательная опция `pool`, в которой должно быть передано название вычислительного пула, где будет запускаться операция с инстансами для клики. Так как операции для клики запускаются из-под системного робота `robot-chyt`, предварительно необходимо выдать право `Use` на указываемый пул этому роботу. {% if audience == "internal" %}
Сделать это можно через веб-интерфейс {{product-name}} или IDM.{% else %}{% endif %}

При установке опции `pool` убедитесь, что у вас есть право `Use` на указываемый пул (иначе команда завершится ошибкой):

```bash
yt clickhouse ctl set-option pool chyt_example_pool --alias chyt_example_clique
```

Установите нужное количество инстансов с помощью опции `instance_count`:

```bash
yt clickhouse ctl set-option instance_count 2 --alias chyt_example_clique
```

Для запуска клики переведите ее в активное состояние, установив опцию `active` в значение `%true`:

```bash
yt clickhouse ctl set-option active %true --alias chyt_example_clique
```

Посмотреть статус клики можно с помощью команды `status`. Когда операция клики будет запущена, значение поля `status` должно стать `Ok`, а `operation_state` перейдет в `running`:

```bash
yt clickhouse ctl status chyt_example_clique
{
    "status" = "Waiting for restart: oplet does not have running yt operation";
}
# a few moments later
yt clickhouse ctl status chyt_example_clique
{
    "status" = "Ok";
    "operation_state" = "running";
    "operation_url" = "https://domain.com/<cluster_name>/operations/48bdec5d-ed641014-3fe03e8-4289d62e";
}
```


Убедитесь, что клика работает, сделав в ней тестовый запрос к таблице `//sys/clickhouse/sample_table`, доступной на всех кластерах, на которых есть CHYT:

```bash
yt clickhouse execute --proxy <cluster_name> --alias *example 'select avg(a) from `//sys/clickhouse/sample_table`'
224.30769230769232
```

Если клика остается недоступной более 10 минут, обратитесь за помощью в {% if audience == "internal" %}
[чат CHYT](https://nda.ya.ru/t/Dqb57xyQ5psK3X){% else %}[чат {{product-name}}](https://t.me/ytsaurus_ru){% endif %}.
