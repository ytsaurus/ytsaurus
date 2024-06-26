# Использование GPU

При исполнении задач некоторые вычисления могут быть произведены на GPU устройствах. Про планирование GPU ресурсов в {{product-name}} можно прочесть в соответствующей [статье](../../../../../user-guide/data-processing/overview#podderzhka-gpu).

Исполнение задач с использованием GPU опирается на плагин [RAPIDS Accelerator](https://docs.nvidia.com/spark-rapids/user-guide/23.08/getting-started/on-premise.html). Он переносит часть операций, для которых описаны преобразования, на GPU. Основные параметры описаны в документации плагина.

## Настройка GPU при запуске задач напрямую

При запуске задач напрямую необходимо правильно сконфигурировать пулы. Зачастую кроме пула необходимо переопределить и дерево пулов.

Драйверу для работы не требуется GPU, поэтому можно использовать произвольный пул (опция `--queue`). Но экзекьюторам GPU устройства необходимы, поэтому им следует указать дерево пулов и пул с GPU ресурсами отдельно.

```bash
spark-submit \
    ... \
    --queue no_gpu_pool \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.5 \
    --conf spark.rapids.memory.pinnedPool.size=2G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --jars yt:///home/spark/lib/rapids-4-spark_2.12-23.12.2-cuda11.jar \
    --conf spark.ytsaurus.executor.operation.parameters="{pool=gpu_pool;pool_trees=[gpu_tree]}" \
    yt:///home/spark/examples/smoke_test_raw.py
```

Каждому экзекьютору может быть выделено только целое количество GPU устройств, однако таски могут делить GPU между собой.

Библиотека должна быть скомпилирована для версии CUDA, поставляемой в окружение джобов. Она указана в `//home/spark/conf/global/cuda_toolkit_version`.

Также в репозитории доступен пример задачи в клиентском режиме:

```bash
python python-examples/gpu_example/main.py <cluster-name> gpu_pool gpu_tree
```

## Вычисления на Standalone кластере

### Запуск кластера

При [старте кластера](../../../../../user-guide/data-processing/spyt/launch#standalone) каждому воркеру будет выделено целое количество GPU, указанное в опции `--worker-gpu-limit`:

```
spark-launch-yt ... --pool gpu_pool --params '{operation_spec={pool_trees=[gpu_tree]}}' --worker-gpu-limit 2
```

Все компоненты кластера будут запущены в указанном пуле, при этом GPU устройства будут запрошены только для воркеров.

### Запуск задач

Пример конфигурации Spark задачи:

```bash
spark-submit-yt \
    ... \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.25 \
    --conf spark.rapids.memory.pinnedPool.size=2G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --jars yt:///home/spark/lib/rapids-4-spark_2.12-23.12.2-cuda11.jar \
    yt:///home/spark/examples/smoke_test_raw.py
```
