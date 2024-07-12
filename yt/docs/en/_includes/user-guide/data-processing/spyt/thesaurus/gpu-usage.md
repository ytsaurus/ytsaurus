# GPU usage

In task execution, some computations can be performed on GPU devices. You can read about GPU resource scheduling in {{product-name}} [here](../../../../../user-guide/data-processing/overview#podderzhka-gpu).

GPU-based task execution relies on the [RAPIDS Accelerator](https://docs.nvidia.com/spark-rapids/user-guide/23.08/getting-started/on-premise.html) plugin. It takes some of the operations for which conversions are described and transfers them to the GPU. The main parameters are described in the plugin documentation.

## Configuring the GPU for direct execution

For direct execution, pools must be configured correctly. Besides redefining the pool, you'll often also need to redefine the pool tree.

The driver doesn't need a GPU, meaning you can use an arbitrary pool (the `--queue` option). However, executors do need GPUs, so the pool tree and the pool with GPU resources should be specified for them separately.

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

Only an integer number of GPU devices can be allocated to each executor, but tasks can share GPUs.

The library must be compiled for the CUDA version supplied to the job environment. It is specified at `//home/spark/conf/global/cuda_toolkit_version`.

An example task in client mode is also available in the repository:

```bash
python python-examples/gpu_example/main.py <cluster-name> gpu_pool gpu_tree
```

## Computations on a Standalone cluster

### Starting a cluster

At [cluster startup](../../../../../user-guide/data-processing/spyt/launch#standalone), each worker will be allocated an integer number of GPUs specified in the `--worker-gpu-limit` option:

```
spark-launch-yt ... --pool gpu_pool --params '{operation_spec={pool_trees=[gpu_tree]}}' --worker-gpu-limit 2
```

All cluster components will be run in the specified pool, with GPU devices only requested for workers.

### Running tasks

Sample Spark task configuration:

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
