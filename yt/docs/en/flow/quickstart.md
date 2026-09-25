# Quick start with {{product-name}} Flow

Run the C++ Word Count example as a first pipeline. It reads text rows from a queue, splits them into words, and stores per-word counts in a dynamic table. You need access to a {{product-name}} cluster and a pool in which you can start a [Vanilla operation](devops/vanilla/initial-deploy.md#prerequisites).

## Build the example {#build}

From the [source repository]({{source-root}}), build the example binary:

```bash
ya make yt/yt/flow/examples/cpp/word_count
```

The binary is `yt/yt/flow/examples/cpp/word_count/word_count`. In the `reader` computation's `processing_function_parameters`, the example sets `min_word_length = 4`, so it ignores words shorter than four bytes; set it to `0` to count every word. The [C++ walkthrough](cpp/getting-started.md#define-messages) explains its source, computations, and state.

## Prepare the pipeline {#prepare}

Choose an existing writable Cypress directory. The commands below use `//home/flow/word-count` as a sample base path: replace it everywhere with a directory you own, and replace `<cluster>` with your cluster proxy name. From the [Word Count example's schema]({{source-root}}/yt/yt/flow/examples/cpp/word_count/test/yt_sync.py), create the input queue, its consumer, and the sorted external state table:

```bash
yt --proxy <cluster> create table //home/flow/word-count/input_queue --attributes '{dynamic=%true;schema=[{name=text;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
yt --proxy <cluster> create queue_consumer //home/flow/word-count/consumer
yt --proxy <cluster> register-queue-consumer //home/flow/word-count/input_queue //home/flow/word-count/consumer --vital
yt --proxy <cluster> create table //home/flow/word-count/word_counts --attributes '{dynamic=%true;schema=[{name=hash;type=uint64;sort_order=ascending;expression="farm_hash(word)"};{name=word;type=string;sort_order=ascending};{name=count;type=int64}]}'
yt --proxy <cluster> mount-table //home/flow/word-count/input_queue
yt --proxy <cluster> mount-table //home/flow/word-count/word_counts
```

The `hash` and `word` key columns match the counter's `group_by_schema`; `count` stores the value. The queue has the `text` input column and the two system columns used by the [Queue API](../user-guide/dynamic-tables/queues.md#api). The `create queue_consumer` command mounts the consumer's table automatically. The vital registration prevents automatic queue trimming from deleting rows before this consumer reads them. Before continuing, check that all three objects exist:

```bash
yt --proxy <cluster> exists //home/flow/word-count/input_queue
yt --proxy <cluster> exists //home/flow/word-count/consumer
yt --proxy <cluster> exists //home/flow/word-count/word_counts
```

Each command should print `true`. This C++ runner creates the pipeline object and its internal tables on first launch, then mounts those tables. For a separate preparation step, see [creating a pipeline object](concepts/pipeline-object.md#create).

Open `yt/yt/flow/examples/cpp/word_count/pipeline.yson`. Add `cluster_url` and the pipeline `path` for your cluster. Replace `queue_path` and `consumer_path` with `<cluster=cluster_name>//home/flow/word-count/input_queue` and `<cluster=cluster_name>//home/flow/word-count/consumer`, substituting your actual cluster name for `cluster_name`. Replace the external state manager's `//path/to/word_counts` with the state-table path above. Use the same base path you chose for every object.

Add a `vanilla` block at the top level of the config with `enable = %true`, your pool name, and a worker count. The [initial deployment guide](devops/vanilla/initial-deploy.md#enable) gives the block and its resource settings.

```yson
"cluster_url" = "<your-cluster>";
"path" = "//home/flow/word-count/pipeline";
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 1};
};
```

Put these fields inside the existing outer braces of `pipeline.yson` and replace the placeholders with your cluster and pool. Replace the sample pipeline path if you chose another base directory.

If your cluster runs jobs in Docker/CRI on Kubernetes, check [cluster-name resolution](devops/vanilla/docker-environment.md#cluster-name) from inside the jobs and [external proxy access](devops/vanilla/docker-environment.md#external-access) from the runner before launching. The C++ binary needs no custom Docker image; add the documented network settings only when your installation requires them.

## Validate and launch {#launch}

For this C++ runner, `--validate-only` checks the config locally without starting or updating a pipeline:

```bash
./yt/yt/flow/examples/cpp/word_count/word_count --config yt/yt/flow/examples/cpp/word_count/pipeline.yson --validate-only
```

After validation, launch the pipeline:

```bash
YT_FLOW_WAIT=0 ./yt/yt/flow/examples/cpp/word_count/word_count --config yt/yt/flow/examples/cpp/word_count/pipeline.yson
```

`YT_FLOW_WAIT=0` returns after launch. Without it, the default `YT_FLOW_WAIT=1` keeps the runner in the foreground streaming controller log records while this ongoing pipeline runs. Interrupting the runner does not stop the Vanilla operation. Check that the pipeline was created and is working:

```bash
yt --proxy <cluster> exists //home/flow/word-count/pipeline
yt --proxy <cluster> flow get-pipeline-state //home/flow/word-count/pipeline
```

The first command should print `true`, and the second `working`. Send one input row and inspect the counts after the next processing epoch:

```bash
echo '{text="hello world hello"}' | yt --proxy <cluster> insert-rows //home/flow/word-count/input_queue --format yson
yt --proxy <cluster> select-rows '* from [//home/flow/word-count/word_counts]' --format json
```

Look for `hello` with `count` 2 and `world` with `count` 1. If the table is still empty, wait for another processing epoch and repeat the query; inspect [diagnostics](devops/diagnostics.md) if the jobs fail or make no progress. You can also inspect running jobs in the {{product-name}} UI. When finished, follow [pipeline removal](devops/vanilla/pipeline-operations.md#remove) for the Flow node and decide separately whether to remove the example's queue, consumer, and external state table.

For another language, follow its guide: [Go](go/getting-started.md), [Java](java/getting-started.md), [Python](python/getting-started.md), or [YQL](yql/getting-started.md).
