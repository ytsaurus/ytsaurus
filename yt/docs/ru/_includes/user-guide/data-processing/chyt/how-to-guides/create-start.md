# Создание, запуск и остановка клики

{% note info "Примечание" %}

Описанные ниже операции и действия не запускают вычисления, а лишь подготавливают клику к началу работы.

{% endnote %}

## Как создать новую клику { #create }

Новую клику можно создать двумя способами:

{% list tabs %}

- Через веб-интерфейс

  1. В главном меню {{product-name}} перейдите в раздел **Cliques**.
  1. В открывшемся разделе **CHYT cliques** нажмите в правом верхнем углу кнопку **Create clique**.
  1. Заполните поля формы:
      - _Alias name_ — название клики;
      - _Instance count_ — количество инстансов клики (от 1 до 100);
      - _Pool tree_ — дерево пулов, выберите значение из списка или оставьте значение по умолчанию;
      - _Pool_ — название вычислительного [пула](../../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md#scheduler), в котором необходимо запускать операцию клики.

        {% note info %}

        На этапе создания клики эта опция необязательна. Её необходимо [задать](#set-up-options) до запуска клики.

        {% endnote %}
  1. Чтобы завершить создание, нажмите кнопку **Confirm**.

  После подтверждения вы попадёте в интерфейс созданной клики, и она станет доступна в списке клик в разделе **CHYT cliques**.

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
  1. Создайте клику с помощью команды из [CHYT Controller CLI](../../../../../user-guide/data-processing/chyt/controller.md):
  
      ```bash
      yt clickhouse ctl create chyt_example_clique
      ```

{% endlist %}

## Настройка опций для запуска { #set-up-options }

После создания клики её нужно настроить — задать необходимые [опции](../../../../../user-guide/data-processing/chyt/cliques/configs.md#options). Основная опция для запуска клики — это `pool`, в неё нужно передать название вычислительного пула, где будет запускаться операция с инстансами для клики.

Перед установкой опции `pool` убедитесь, что у вас есть право `Use` на указываемый пул. Проверьте свои права на вкладке **ACL** на [Панели вкладок](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui) в разделе **Object permissions**. Если вы не знаете, какой пул использовать, обратитесь к администратору кластера {{product-name}}.

  {% if audience == "internal" %}
  {% note warning "Внимание" %}

  Не запускайте приватную клику в пуле **Research** (в частности, это случится, если не указать pool при запуске клики).

  В этом пуле отсутствуют гарантии по CPU. Из‑за этого джобы операции, в которой запущена клика, могут [вытесняться](../../../../../user-guide/data-processing/chyt/resources.md) в произвольные моменты времени. При вытеснении все текущие запросы завершатся аварийно.

  {% endnote %}
  {% endif %}

Задать опцию `pool` можно через:

{% list tabs %}

- Веб-интерфейс

  1. Откройте интерфейс клики, как описано в разделе [Как перейти в интерфейс клики](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
  1. Нажмите ![edit speclet](../../../../../../images/edit-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui), или кнопку **Edit speclet** на вкладке **Speclet** на [Панели вкладок](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).
  1. В открывшемся окне настроек, в поле _Pool_ введите название вычислительного пула.
  1. Для завершения нажмите кнопку **Confirm**.

  {% note info %}

  Так как операции для клики запускаются из-под системного робота {% if audience == "internal" %}`robot-chyt`{% else %}`robot-strawberry-controller`{% endif %}, заранее
  выдайте этому роботу право `Use` на указываемый пул этому роботу. {% if audience == "internal" %} Сделать это можно через веб-интерфейс {{product-name}} или IDM.{% endif %}

  {% endnote %}

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
  1. Установите опцию с помощью команды:

      ```bash
      yt clickhouse ctl set-option pool chyt_example_pool --alias chyt_example_clique
      ```

Теперь клика настроена и все настройки сохранены в [спеклете](../../../../../user-guide/data-processing/chyt/cliques/configs.md#speclet) в Кипарисе.

{% endlist %}

## Как запустить клику { #start }

Запустить сконфигурированную клику можно через:

{% list tabs %}

- Веб-интерфейс

  1. Откройте интерфейс клики, как описано в разделе [Как перейти в интерфейс клики](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
  1. Нажмите ![start](../../../../../../images/start-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu).
  1. Убедитесь, что:

      - параметр `Health` перешёл в состояние `Pending` и через некоторое время сменился на `Good`,
      - параметр `State` принял значение `Active`.
  
      Посмотреть статусы этих параметров можно в интерфейсе клики — в блоке с [характеристиками](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).
  1. Проверьте работоспособность клики. Для этого сделайте тестовый запрос в интерфейсе **Query Tracker**:

      - нажмите ![sql](../../../../../../images/sql-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu);
      - в открывшемся окне введите и выполните SQL-запрос.

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
  1. Запустите клику с помощью команды:
  
      ```bash
      yt clickhouse ctl start chyt_example_clique
      ```

  1. Проверьте статус клики с помощью команды `status`. Когда операция клики будет запущена, значение поля `status` должно стать `Ok`, а `operation_state` перейдёт в `running`:
  
      ```bash
        $ yt clickhouse ctl status chyt_example_clique
        {
            "status" = "Waiting for restart: oplet does not have running yt operation";
        }
        # a few moments later
        $ yt clickhouse ctl status chyt_example_clique
        {
            "status" = "Ok";
            "operation_state" = "running";
            "operation_url" = "https://domain.com/<cluster_name>/operations/<some_hash>";
        }
      ```

  1. Убедитесь, что клика работает. Для этого сделайте тестовый запрос, например:
  
      ```bash
      yt clickhouse execute --alias chyt_example_clique 'select 42'
      ```

{% endlist %}

{% note warning %}

Если клика остаётся недоступной более 10 минут, проверьте статус YT-операции:

- в веб-интерфейсе — по ссылке из параметра `YT operation id`;
- в CLI — по ссылке `operation_url` из вывода команды `status`.

{% if audience == "public" %}
Если самостоятельно разобраться не удалось, обратитесь за помощью в [чат {{product-name}}](https://t.me/ytsaurus_ru) или в ваш персональный канал поддержки.
{% endif %}

{% if audience == "internal" %}
Если самостоятельно разобраться не удалось, обратитесь за помощью в [наш чат в Мессенджере](https://nda.ya.ru/t/bRGOsLUy7hWZiX) или [архивный чат в Telegram](https://nda.ya.ru/t/Dqb57xyQ5psK3X).
{% endif %}

{% endnote %}

## Как остановить клику { #stop }

Остановить клику можно через:

{% list tabs %}

- Веб-интерфейс

  1. Откройте интерфейс клики, как описано в разделе [Как перейти в интерфейс клики](../../../../../user-guide/data-processing/chyt/cliques/ui.md#where).
  1. Нажмите ![stop](../../../../../../images/stop-btn.png){width=24 height=24} в правом верхнем углу, в блоке [Кнопки действий](../../../../../user-guide/data-processing/chyt/cliques/ui.md#action-menu).
  1. Убедитесь, что:

      - параметр `Health` перешёл в состояние `Pending` и через некоторое время сменился на `Failed`,
      - параметр `State` принял значение `Inactive`.
  
      Посмотреть статусы этих параметров можно в интерфейсе клики — в блоке с [характеристиками](../../../../../user-guide/data-processing/chyt/cliques/ui.md#ui).

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
    1. Остановите клику с помощью команды:

        ```bash
        yt clickhouse ctl stop chyt_example_clique
        ```

{% endlist %}
