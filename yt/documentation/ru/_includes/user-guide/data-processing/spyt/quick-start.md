# Быстрый старт

## Установка клиента { #install }

Установите пакет `ytsaurus-spyt`:

```bash
pip install ytsaurus-spyt
```
## Запуск кластера { #start }

1. Выберите пользователя от имени которого необходимо запустить кластер. Код, который регулярно запускается на Spark, нужно загрузить в систему {{product-name}}. У пользователя, от имени которого запущен кластер, должны быть права на чтение кода.
2. Создайте директорию для служебных данных Spark, например `my_discovery_path`. Пользователь,  от имени которого запущен кластер, должен иметь права на запись в директорию. Пользователи, которые будут запускать джобы на Spark, должны иметь права на чтение директории.
3. Запустите кластер:
    ```bash
   spark-launch-yt \
    --proxy <cluster-name> \
    --pool  my_pool \
    --discovery-path my_discovery_path \
    --worker-cores 16 \
    --worker-num 5 \
    --worker-memory 64G \
    --spark-cluster-version 2.4.4-0.5.1
   ```

   Опции:
    - `spark-launch-yt` – запуск в Vanilla-операции {{product-name}} с клиентского хоста;
    - `--proxy` – имя кластера;
    - `--pool` – вычислительный пул {{product-name}};
    - `--spyt-version` – директория для служебных данных Spark;
    - `--worker-cores` – количество ядер у [воркера](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone-в-yt--spark-standalone);
    - `--worker-num` – количество ворверов;
    - `--worker-memory` – количество памяти у каждого воркера;
    - `--spark-cluster-version` – [версия](../spyt/version.md) кластера (опционально).


4. Запустите тестовый джоб на кластере:
    ```bash
    spark-submit-yt \
    --proxy <cluster-name> \
    --discovery-path my_discovery_path \
    --deploy-mode cluster \
    --spyt-version 0.5.2 \
    yt:///sys/spark/examples/smoke_test.py
    ```

    Опции:
    - `spark-submit-yt` – обертка над [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html), позволяет определить адрес мастера Spark из Vanilla-операции. Поиск производится по аргументам: `proxy`, `id`, `discovery-path`.
    - `--proxy` – имя кластера;
    - `--discovery-path` – директория для служебных данных Spark;
    - `--deploy-mode` (`cluster` или `client`) – [режим](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode--cluster-mode) запуска кластера;
    - `--spyt-version` – версия SPYT (опционально);
    - адрес файла с кодом в {{product-name}}.

## Использование { #use }

- **spark-launch-yt**

    ```
    spark-launch-yt \
    --proxy <cluster-name> \
    --pool my_pool \
    --discovery-path my_discovery_path \
    --worker-cores 16 \
    --worker-num 5 \
    --worker-memory 64G
    ```

- **spark-discovery-yt**

    Получить ссылки на UI мастера, операцию, Spark History Server:

    ```bash
    spark-discovery-yt \
    --proxy <cluster-name> \
    --discovery-path my_discovery_path
    ```

- **spark-submit-yt**

   ```bash
   spark-submit-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   --spyt-version 0.5.2 \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% note info "Примечание" %}

   Вместо некоторых аргументов команд можно установить переменные окружения, например: `YT_PROXY` — вместо `--proxy`.

   {% endnote %}


## Дополнительные параметры

О дополнительных параметрах при запуске кластера можно узнать в разделе [Запуск кластера Spark](../../../../user-guide/data-processing/spyt/cluster/cluster-start.md).



