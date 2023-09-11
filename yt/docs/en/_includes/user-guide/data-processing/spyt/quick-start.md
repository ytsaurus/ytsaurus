# Quick start

## Client install { #install }

Install the `ytsaurus-spyt` package:

```bash
pip install ytsaurus-spyt
```
## Cluster start { #start }

1. Select an account you will use to start your cluster. You will need to upload any code that regularly runs on Spark to the {{product-name}} system. The account used to start the cluster must have enough privilege to read the code.
2. Create a directory for Spark housekeeping data, such as `my_discovery_path`. The account used to start the cluster must have write privileges to the directory. Users that will run Spark jobs must have read access to the directory.
3. Start your cluster:
   ```bash
   spark-launch-yt \
   --proxy <cluster-name> \
   --pool  my_pool \
   --discovery-path my_discovery_path \
   --worker-cores 16 \
   --worker-num 5 \
   --worker-memory 64G
   ```

   Options:
   - `spark-launch-yt`: Start the Vanilla {{product-name}} transaction from a client host.
   - `--proxy`: Cluster name.
   - `--pool`: {{product-name}} computational pool.
   - `--spyt-version`: Spark housekeeping data directory.
   - `--worker-cores`: Number of [worker](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone-в-yt--spark-standalone) cores.
   - `--worker-num`: Number of workers.
   - `--worker-memory`: Amount of each worker's memory.
   - `--spark-cluster-version`: Cluster [version](../../../../user-guide/data-processing/spyt/version.md) (optional).


4. Start a test job on your cluster:
   ```bash
   spark-submit-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   yt:///sys/spark/examples/smoke_test.py
   ```

   Options:
   - `spark-submit-yt`: [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) wrapper that enables you to find out the Spark master address from the Vanilla transaction. The search uses `proxy`, `id`, and `discovery-path` as arguments.
   - `--proxy`: Cluster name.
   - `--discovery-path`: Spark housekeeping data directory.
   - `--deploy-mode` (`cluster` or `client`): Cluster startup [mode](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode--cluster-mode).
   - `--spyt-version`: SPYT version (optional).
   - Address of the file with the code in {{product-name}}.

## Use { #use }

- **spark-launch-yt**

   ```
   spark-launch-yt \
   --proxy <cluster-name> \
   --pool my_pool \
   --discovery-path my_discovery_path \
   --worker-cores 16 \
   --worker-num 5 \
   --worker-memory 64G \
   --spark-cluster-version 1.72.0
   ```

- **spark-discovery-yt**

   Retrieving links to the UI master, transaction, Spark History Server:

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
   --spyt-version 1.72.0 \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% note info "Note" %}

   You can set environment variables to use instead of some of the command arguments, such as `YT_PROXY` instead of `--proxy`.

   {% endnote %}


## Additional parameters

For additional cluster startup parameters, see [Starting a Spark cluster](../../../../user-guide/data-processing/spyt/cluster/cluster-start.md).



