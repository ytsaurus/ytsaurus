# Добавление вычислительных ресурсов

Иногда при работе с кликами возникают проблемы с производительностью из-за полного использования имеющихся ресурсов и, как следствие, деградации запросов. В таких случаях нужно увеличить объём вычислительных ресурсов.
Вычислительные ресурсы — это инстансы {{clickhouse}}: серверы с выделенными процессорами (CPU) и оперативной памятью (RAM). Чаще всего пользователям достаточно регулировать только число инстансов.

{% note warning "Важно" %}

Чтобы настроить клику, у вас должно быть право `manage` на эту клику. Проверьте, есть ли у вас этот тип прав, на вкладке **ACL** на [Панели вкладок](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs) в разделе **Object permissions**.

{% endnote %}

Добавить вычислительные ресурсы клике можно двумя способами:

{% list tabs %}

- Через веб-интерфейс

  1. Откройте интерфейс клики, как описано в разделе [Как перейти в интерфейс клики](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
  1. Нажмите ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) или кнопку **Edit speclet** на вкладке **Speclet** на [Панели вкладок](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).

  1. Выберите слева вкладку **Resources**.

  1. В поле _Instances_ введите количество инстансов для клики от 1 до 100.

     {% note info %}

     Значение поля по умолчанию — `1`. Универсальной рекомендации по выбору количества инстансов нет: параметр зависит от ваших задач и подбирается опытным путём. Советуем начать со значения по умолчанию.

     {% endnote %}

  1. Чтобы применить изменения, нажмите кнопку **Confirm**.

- С помощью CLI

  1. Установите [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) в составе пакета `ytsaurus-client`, если вы этого ещё не сделали.

  1. Сохраните адрес прокси в переменную окружения. Это нужно, чтобы не указывать кластер {{product-name}} в каждой команде через аргумент `--proxy`.

     ```bash
     export YT_PROXY=<cluster_name>
     ```

  1. Задайте переменную окружения с адресом контроллера:

     ```bash
     export CHYT_CTL_ADDRESS=<address>
     ```

     , где `<address>` — адрес контроллера. Например, для демо-кластера адрес имеет вид: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
     Адрес контроллера можно получить из поля `controller` из результата выполнения команды

     ```bash
     yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
     ```

  1. Сохраните в переменную окружения имя кластера:

     ```bash
     export CLUSTER_NAME=<cluster_name>
     ```

     , где `<cluster_name>` — имя кластера. Например, имя демо-кластера: `ytdemo`.

  1. Установите нужное количество инстансов с помощью опции `instance_count`:

     ```bash
     yt clickhouse ctl set-option instance_count COUNT --alias chyt_example_clique
     ```

     , где `COUNT` — количество инстансов.

{% endlist %}

## Продвинутые настройки вычислительных ресурсов {#advanced}

{% note warning "Важно" %}

Эти опции предназначены для опытных пользователей. Если вы не уверены, нужны ли они вам, оставьте значения по умолчанию.

{% endnote %}

К продвинутым настройкам относятся параметры CPU и RAM для каждого инстанса. При выборе значений CPU и RAM учитывайте физические ресурсы ваших серверов и потребности базовых операций с кликами.

Рекомендуемые значения:

- от 1 до 100 ядер CPU на инстанс;
- от 20 до 300 ГБ RAM на инстанс.

{% note info %}

Выбирайте значения опций так, чтобы они не превышали физические ресурсы серверов. Например, на сервере с 10 ядрами CPU и 40 ГБ памяти нельзя выделить инстанс с 16 ядрами и 65 ГБ.

{% endnote %}

Установить значения опций `Instance CPU` и `Instance Total Memory` можно через:

{% list tabs %}

- Веб-интерфейс

  1. Откройте интерфейс клики, как описано в разделе [Как перейти в интерфейс клики](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
  1. Нажмите ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu) или кнопку **Edit speclet** на вкладке **Speclet** на [Панели вкладок](../../../../../user-guide/data-processing/chyt/cliques/ui.md#tabs).
  1. Выберите слева вкладку **Resources**.
  1. В поле _Instance CPU_ укажите количество ядер CPU на инстанс.
  1. В поле _Instance Total Memory_ укажите объём оперативной памяти RAM на инстанс.
  1. Чтобы сохранить настройки, нажмите кнопку **Confirm**.

- CLI

  1. Установите [CHYT CLI](../../../../../user-guide/data-processing/chyt/cli-and-api.md) в составе пакета `ytsaurus-client`, если вы этого ещё не сделали.

  1. Сохраните адрес прокси в переменную окружения. Это нужно, чтобы не указывать кластер {{product-name}} в каждой команде через аргумент `--proxy`.

     ```bash
     export YT_PROXY=<cluster_name>
     ```

  1. Задайте переменную окружения с адресом контроллера:

     ```bash
     export CHYT_CTL_ADDRESS=<address>
     ```

     , где `<address>` — адрес контроллера. Например, для демо-кластера адрес имеет вид: `https://strawberry-XXXXXXXX.demo.ytsaurus.tech`.
     Адрес контроллера можно получить из поля `controller` из результата выполнения команды

     ```bash
     yt get //sys/strawberry/chyt/<alias>/@strawberry_info_state
     ```

  1. Сохраните в переменную окружения имя кластера:

     ```bash
     export CLUSTER_NAME=<cluster_name>
     ```

     , где `<cluster_name>` — имя кластера. Например, имя демо-кластера: `ytdemo`.

  1. Установите нужное количество CPU, которое будет выделено под каждый инстанс клики с помощью опции `instance_cpu`:

     ```bash
     yt clickhouse ctl set-option instance_cpu CPU_AMOUNT --alias chyt_example_clique
     ```

     , где `CPU_AMOUNT` — количество ядер CPU.

  1. Установите нужный объём RAM, которое будет выделено под каждый инстанс клики с помощью опции `instance_total_memory`:

     ```bash
     yt clickhouse ctl set-option instance_total_memory RAM_AMOUNT --alias chyt_example_clique
     ```

     , где `RAM_AMOUNT` — количество оперативной памяти RAM в ГБ.

{% endlist %}

{% include [memory-usage-warning](./_includes/memory-usage-warning.md) %}

Учитывайте, что при включённой опции **Restart on speclet change**, изменение любых настроек приведёт к автоматическому перезапуску клики, при этом перезапуск займёт некоторое время. Опцию **Restart on speclet change** можно включить на вкладке **General** в диалоговом окне интерфейса спеклета.
