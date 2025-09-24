# Nvidia GPU Support

To add hosts with GPUs to the {{product-name}} cluster and use them, you need to properly configure the compute nodes.

{% note info "Warning" %}

GPU support appeared starting from {{product-name}} version 25.2 and the operator from version 0.27.0.

{% endnote %}

## k8s Cluster Setup

Two steps are required:

1. Install Nvidia drivers on the k8s cluster nodes.

As a result, the driver must be mounted in the exec nodes of {{product-name}}; that is, the exec node should see nvidia devices in `/dev`.

2. Install the [GPU Operator](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html) from Nvidia.

Make sure that the operator is installed successfully and that the `nvidia.com/gpu` resource is available on the GPU nodes of the k8s cluster.

## YTsaurus Specification

Add a group of nodes with GPU support to the `execNodes` section of the specification.

For this group, you need to specify a special `entrypointWrapper`, enable nvidia runtime, and specify the `nvidia.com/gpu` resource in the resource request.

Example config for a group of one node with one GPU:

```yaml
execNodes:
- instanceCount: 1
  tags:
    - gpu
  jobEnvironment:
    cri:
      entrypointWrapper:
      - tini
      - --
      - /usr/bin/gpuagent_runner.sh
  runtime:
    nvidia: {}
  privileged: true
  jobResources:
    requests:
      limits:
        cpu: 16
        memory: 128Gi
        nvidia.com/gpu: 1
      requests:
        cpu: 16
        nvidia.com/gpu: 1
```

After applying the {{product-name}} specification, make sure the node is up successfully and that GPUs are available on it. You can check GPU availability either in the cluster UI by looking at the node resources, or using the CLI:

```bash
yt get //sys/exec_nodes/<node_address>/@resource_limits/gpu
```


## Pool Tree Setup

For efficient scheduler operation, it is highly desirable that resources on the exec nodes of the cluster and in operation jobs are sufficiently homogeneous. Therefore, GPU nodes should be allocated to a separate pool tree.

To do this, run the following command:

```bash
yt create scheduler_pool_tree --attributes '{name=gpu; config={nodes_filter=gpu; main_resource=gpu;}}'
```

Now you can run a test operation and check that `nvidia-smi` is available in the job:

```bash
yt vanilla --tasks '{task={job_count=1; command="nvidia-smi 1>&2"; gpu_limit=1;};}' --spec '{pool_trees=[gpu];}'
```
