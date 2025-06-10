# Запуск Spark задач на выполнение в {{product-name}}

## Установка пакета ytsaurus-spyt и необходимых зависимостей { #install }

Установите пакет `ytsaurus-spyt` и все необходимые зависимости:

```bash
$ pip install ytsaurus-spyt[all]
```

В этом случае помимо пакета `ytsaurus-spyt` также будут установлены `pyspark` и `ytsaurus-client` последней совместимой версии. При необходимости использования других версий `pyspark` их можно будет установить отдельно с явным указанием версии, например `pip install pyspark==3.2.2`.

Совместимость SPYT с различными версиями Spark приведена [в этой таблице](../../../../user-guide/data-processing/spyt/overview.md#spyt-compatibility).

При запуске задач будет использован дистрибутив Spark, номер версии которого совпадает с версией пакета `pyspark` на машине, с которой выполняется запуск. Для загрузки Spark дистрибутива на кластер можно воспользоваться скриптом `tools/release/publisher/spark-distrib.py`.

## Запуск задачи напрямую в {{product-name}} (доступно с версии SPYT 1.76.0) { #submit }

Данный способ подходит для случаев, когда нет потребности в непрерывной работе кластера. Он позволяет использовать ресурсы кластера только в случае реальной потребности в них. В сравнении с запуском приложений во внутреннем Standalone Spark кластере данный способ работает чуть дольше за счет того, что каждый раз необходимо поднимать отдельную операцию для запуска приложения, однако с другой стороны он позволяет освободить ресурсы сразу же, как только в них больше не будет необходимости.

Запуск задач напрямую в {{product-name}} рекомендуется в следующих случаях:

- Выполнение разовых расчетов.
- Запуск задач, имеющих низкую периодичность (реже 1 раза в час).
- Ad-hoc аналитика при помощи консольных утилит `spark-shell`, `pyspark` или `Jupyter`.

Для того чтобы воспользоваться данным способом, необходимо выполнить следующие шаги:

1. Активируйте конфигурацию SPYT при помощи команды `source spyt-env`.
2. Загрузите исполняемый файл и его зависимости в Кипарис. Начиная с версии 2.4.0 также можно указывать локальные файлы как для исполняемого файла, так и для зависимостей.
3. Запустите задачу на выполнение при помощи следующей команды.
```bash
$ spark-submit --master ytsaurus://<cluster-name> --deploy-mode cluster --num-executors 5 --queue research yt:/<path to .py file or .jar file on cypress>
```
При использовании локальных файлов:
```bash
$ spark-submit --master ytsaurus://<cluster-name> --deploy-mode cluster --num-executors 5 --queue research <path to .py file or .jar file on local drive>
```

Опции:
- `--master` — адрес прокси кластера;
- `--queue` — название пула в планировщике, в котором нужно запустить расчёт

Назначение остальных опций соответствует их описанию в документации `spark-submit` (полный список выводится по команде `spark-submit --help`). Можно использовать почти все доступные опции за исключением следующих:

- `--conf spark.dynamicAllocation.enabled` — в настоящий момент поддержка динамического выделения ресурсов не реализована, поэтому эту опцию лучше не выставлять в `true`;
- `--py-files, --files, --archives` — с локальными файлами не работают, можно использовать только те, которые были предварительно загружены в Кипарис.

Для использования History server в этом режиме его необходимо поднять отдельно согласно [инструкции](../../../../user-guide/data-processing/spyt/cluster/shs.md). Для получения диагностической информации также можно воспользоваться логами {{product-name}} операций. Следует учесть следующее:
- Доступны только логи, которые пишутся в stderr, поэтому нужно сделать соответствующие настройки логирования в запускаемом Spark приложении.
- В настоящий момент драйвер и экзекьюторы запускаются в разных {{product-name}} операциях, соответственно логи нужно смотреть и в той, и в другой.

Конфигурационные параметры, доступные при запуске задач напрямую, можно найти [здесь](../../../../user-guide/data-processing/spyt/thesaurus/configuration.md#direct-submit)

{% note warning "Внимание" %}

Начиная с версии Spark 3.4.0 при использовании сети с IPv6 адресами необходимо устанавливать переменную окружения `SPARK_PREFER_IPV6=true`

{% endnote %}

## Выполнение локальных файлов { #submit-local }

Начиная с версии SPYT 2.4.0 при запуске задач напрямую в {{product-name}} можно указывать локальные файлы в качестве исполняемых модулей и зависимостей без их предварительной загрузки на Кипарис. В этом случае локальные файлы будут предварительно загружены в файловый кеш на Кипарисе и затем будут оттуда использованы в приложении. При повторном запуске будут использоваться уже закешированные файлы. Пример команды `spark-submit` для данного способа приведен ниже:

```bash
spark-submit --master ytsaurus://<cluster name> \
             --deploy-mode cluster \
             --num-executors 5 \
             --executor-cores 4 \
             --py-files ~/path/to/my/dependencies.py \
             ~/path/to/my/script.py
```

## Запуск standalone Spark кластера { #standalone }

Данный способ подходит при интенсивном использовании кластера. В данном режиме {{product-name}} выделяет ресурсы под внутренний Standalone Spark кластер, который занимается выполнением расчетов. Для запуска в этом режиме необходимо использовать обёртки над стандартными командами Spark `spark-submit-yt` и `spark-shell-yt`, входящими в состав пакета `ytsaurus-spyt`. Этот режим рекомендуется использовать в следующих случаях:

- Запуск задач с высокой периодичностью (более 1 раза в час). Эффективность достигается за счет того, что время запуска задачи в standalone кластере существенно меньше, чем время запуска операции в {{product-name}}.
- Ad-hoc аналитика в Jupyter-ноутбуках.
- Ad-hoc аналитика с использованием Query tracker и livy.

Для запуска внутреннего standalone Spark кластера необходимо выполнить следующие шаги:

1. Выберите пользователя, от имени которого необходимо запустить кластер. Код, который регулярно запускается на Spark, нужно загрузить в систему {{product-name}}. У пользователя, от имени которого запущен кластер, должны быть права на чтение кода.
2. Создайте директорию для служебных данных Spark, например `my_discovery_path`. Пользователь, от имени которого запущен кластер, должен иметь права на запись в директорию. Пользователи, которые будут запускать джобы на Spark, должны иметь права на чтение директории.
3. Запустите кластер:
    ```bash
   $ spark-launch-yt \
    --proxy <cluster-name> \
    --pool  my_pool \
    --discovery-path my_discovery_path \
    --worker-cores 16 \
    --worker-num 5 \
    --worker-memory 64G
   ```

   Опции:
    - `spark-launch-yt` — запуск в Vanilla-операции {{product-name}} с клиентского хоста;
    - `--proxy` — имя кластера;
    - `--pool` — вычислительный пул {{product-name}};
    - `--spyt-version` — директория для служебных данных Spark;
    - `--worker-cores` — количество ядер у [воркера](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone-в-yt--spark-standalone);
    - `--worker-num` — количество воркеров;
    - `--worker-memory` — количество памяти у каждого воркера;
    - `--spark-cluster-version` — [версия](../../../../user-guide/data-processing/spyt/version.md) кластера (опционально).


4. Запустите тестовый джоб на кластере:
    ```bash
    $ spark-submit-yt \
    --proxy <cluster-name> \
    --discovery-path my_discovery_path \
    --deploy-mode cluster \
    yt:///sys/spark/examples/smoke_test.py
    ```

    Опции:
    - `spark-submit-yt` — обертка над [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html), позволяет определить адрес мастера Spark из Vanilla-операции. Поиск производится по аргументам: `proxy`, `id`, `discovery-path`;
    - `--proxy` — имя кластера;
    - `--discovery-path` — директория для служебных данных Spark;
    - `--deploy-mode` (`cluster` или `client`) — [режим](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode--cluster-mode) запуска кластера;
    - `--spyt-version` — версия SPYT (опционально);
    - адрес файла с кодом в {{product-name}}.

## Работа с использованием внутреннего Spark кластера { #use }

- **spark-launch-yt**

   Запуск Spark кластера внутри {{product-name}}:

    ```bash
    $ spark-launch-yt \
    --proxy <cluster-name> \
    --pool my_pool \
    --discovery-path my_discovery_path \
    --worker-cores 16 \
    --worker-num 5 \
    --worker-memory 64G \
    --spyt-version {{spyt-version}}
    ```

- **spark-discovery-yt**

    Получить ссылки на UI мастера, операцию, Spark History Server:

    ```bash
    $ spark-discovery-yt \
    --proxy <cluster-name> \
    --discovery-path my_discovery_path
    ```

- **spark-submit-yt**

   Запуск задач на кластере:

   ```bash
   $ spark-submit-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% note info "Примечание" %}

   Вместо некоторых аргументов команд можно установить переменные окружения, например: `YT_PROXY` — вместо `--proxy`.

   ```bash
   $ export YT_PROXY=<cluster-name>

   $ spark-submit-yt \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% endnote %}

## Использование определённой версии JDK

Начиная с версии SPYT 2.6.0 поддерживаются JDK 11 и JDK 17. Более ранние версии поддерживают только JDK 11. JDK 17 можно использовать только в случае, если версия Spark 3.3.0 и выше, Spark 3.2.x JDK 17 не поддерживает. По умолчанию на кластере будет использоваться та же версия JDK, что и на машине, с которой выполняется запуск задачи.

## Дополнительные параметры

О дополнительных параметрах при запуске кластера можно узнать в разделе [Запуск кластера Spark](../../../../user-guide/data-processing/spyt/cluster/cluster-start.md).
