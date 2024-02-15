# Launching Spark tasks in {{product-name}}

## Client install { #install }

Install the `ytsaurus-spyt` package:

```bash
$ pip install ytsaurus-spyt
```

## Submitting Spark applications directly to {{product-name}} (available from SPYT version 1.76.0) { #submit }

This method is applicable when there is no need for continuous cluster operation. It allows to use cluster resources only on demand. This method works a little bit longer comparing to inner Spark standalone cluster due to the need to launch a separate operation for every application, but it allows to release cluster resources immediately after application completes.

Direct submitting to {{product-name}} is recommended in the following cases:

- One-time calculations.
- Low periodic tasks (less than once per hour).
- Ad-hoc analytinc using `spark-shell` or `pyspark` console utilities (For analytics using `Jupyter` it is still needed to use inner standalone cluster).

To use this method follow these steps:

1. Activate SPYT configuration with `source spyt-env` command.
2. Upload an application executable file and it's dependencies to Cypress.
3. Submit the application with the following command:
```bash
$ spark-submit --master ytsaurus://<cluster-name> --deploy-mode cluster --num-executors 5 --queue research yt:/<path to .py file or .jar file>
```

Options:
- `--master` — cluster proxy address;
- `--queue` — the name of the scheduler pool where the task should be launched

The usage of other options corresponds to their description in `spark-submit` documentation (the full list is available with `spark-submit --help` command). Almost all of the options can be used except these:

- `--conf spark.dynamicAllocation.enabled` — dynamic allocation of executors is not yet implemented so setting this option to `true` will have ho effect;
- `--py-files, --files, --archives` — these options are not working with local files. All required files must be uploaded to Cypress before submitting an application.

In this mode the support for History server is not implemented. For diagnostics you can use {{product-name}} logs. Two thigs need to be considered: first, only logs that are written to stderr are available so it is needed to make appropriate configurations in Spark application logging settings. Second, the driver and executors are launched in separate {{product-name}} operations so logs need to be viewed in both.

## Launching inner Spark Standalone cluster { #standalone }

This method is applicable for intensive cluster usage. In this mode {{product-name}} allocates resources for inner Spark standalone cluster which is used to launch Spark applications. This mode is recommended in the following cases:

- Launching high-frequent tasks (more than once per hour). The efficiency is achieved because the task startup time in standalone cluster is significantly less than {{product-name}} operation startup time.
- Ad-hoc analytics in Jupyter notebooks.
- Ad-hoc analytics using Query tracker and livy.

For launching an inner standalone Spark cluster follow these steps:

1. Select an account you will use to start your cluster. You will need to upload any code that regularly runs on Spark to the {{product-name}} system. The account used to start the cluster must have enough privilege to read the code.
2. Create a directory for Spark housekeeping data, such as `my_discovery_path`. The account used to start the cluster must have write privileges to the directory. Users that will run Spark jobs must have read access to the directory.
3. Start your cluster:
   ```bash
   $ spark-launch-yt \
   --proxy <cluster-name> \
   --pool  my_pool \
   --discovery-path my_discovery_path \
   --worker-cores 16 \
   --worker-num 5 \
   --worker-memory 64G
   ```

   Options:
   - `spark-launch-yt` — Start the Vanilla {{product-name}} transaction from a client host.
   - `--proxy` — Cluster name.
   - `--pool` — {{product-name}} computational pool.
   - `--spyt-version` — Spark housekeeping data directory.
   - `--worker-cores` — Number of [worker](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone-в-yt--spark-standalone) cores.
   - `--worker-num` — Number of workers.
   - `--worker-memory` — Amount of each worker's memory.
   - `--spark-cluster-version` — Cluster [version](../../../../user-guide/data-processing/spyt/version.md) (optional).


4. Start a test job on your cluster:
   ```bash
   $ spark-submit-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   yt:///sys/spark/examples/smoke_test.py
   ```

   Options:
   - `spark-submit-yt` — [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) wrapper that enables you to find out the Spark master address from the Vanilla transaction. The search uses `proxy`, `id`, and `discovery-path` as arguments.
   - `--proxy` — Cluster name.
   - `--discovery-path` — Spark housekeeping data directory.
   - `--deploy-mode` (`cluster` or `client`) — Cluster startup [mode](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#cluster-mode--cluster-mode).
   - `--spyt-version` — SPYT version (optional).
   - Address of the file with the code in {{product-name}}.

## Use { #use }

- **spark-launch-yt**

   ```bash
   $ spark-launch-yt \
   --proxy <cluster-name> \
   --pool my_pool \
   --discovery-path my_discovery_path \
   --worker-cores 16 \
   --worker-num 5 \
   --worker-memory 64G \
   --spark-cluster-version 1.76.1
   ```

- **spark-discovery-yt**

   Retrieving links to the UI master, transaction, Spark History Server:

   ```bash
   $ spark-discovery-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path
   ```

- **spark-submit-yt**

   ```bash
   $ spark-submit-yt \
   --proxy <cluster-name> \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   --spyt-version 1.76.1 \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% note info "Note" %}

   You can set environment variables to use instead of some of the command arguments, such as `YT_PROXY` instead of `--proxy`.

   ```bash
   $ export YT_PROXY=<cluster-name>

   $ spark-submit-yt \
   --discovery-path my_discovery_path \
   --deploy-mode cluster \
   yt:///sys/spark/examples/smoke_test.py
   ```

   {% endnote %}


## Additional parameters

For additional cluster startup parameters, see [Starting a Spark cluster](../../../../user-guide/data-processing/spyt/cluster/cluster-start.md).



