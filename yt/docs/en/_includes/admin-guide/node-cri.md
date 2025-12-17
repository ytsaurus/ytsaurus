# CRI for Job Container Runtime

## Types of Containerization

{{product-name}} supports different types of job containerization within an exe node:
  - `simple` (used by default) — jobs are launched directly inside the exe node container; this mode always works and does not require additional configuration, however it is unsafe, since jobs can interfere with the exe node process, and it also does not allow jobs to use an environment different from that of the node container.
{% if audience == "internal" %}
  - `porto` — a [containerization system](https://github.com/yandex/porto) developed at Yandex. It is well integrated with {{product-name}}, but is complex to install and operate.
{% endif %}
  - `cri` — a scheme in which the Kubernetes containerization system is used to run jobs.

## {{product-name}} Integration with CRI

{% note info "Note" %}

This description assumes the use of the [containerd](https://containerd.io/) runtime.
<!---
TODO: make the description more generic, taking cri-o into account
---> 

{% endnote %}

The current integration works as follows. Inside the exe node pod, next to the `ytserver` container, a separate container named `jobs` is launched. In the `jobs` container, the `containerd` daemon is started; it creates a socket through which the node process launches job containers. The created socket is located at `/config/containerd.sock`.

This socket provides the [CRI interface](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/cri-api) for creating containers. In our case, `containerd` is used as the containerization system.

When starting up, the exe node process creates an environment (called a PodSandbox in CRI terms) for each job slot. When a job is launched, a slot is selected, after which a container with user code is created and executed within that environment. These containers launch inside the `jobs` container and are limited by its resources.

<!---
Write about downloading layers in containerd
--->

## CRI Configuration

To enable CRI, you need to make the following changes in the exe node configuration in the cluster spec:

1. Enable CRI.
```
execNodes:
  - ...
    jobEnvironment:
      cri:
        criService: containerd
        apiRetryTimeoutSeconds: 180
```

2. Split resources between the compute node and jobs.

For example, if 10 CPU cores and 50 GB of memory are allocated to the exe node pod, you can specify the following settings in the exe node configuration:
```
execNodes:
  - ...
    resources:
      limits:
        cpu: 2
        memory: 10G
    jobResources:
      limits:
        cpu: 8
        memory: 40G
```

3. Specify the `ImageCache` location.

```
execNodes:
  - ...
    locations:
      ...
      - locationType: ImageCache
        path: /yt/node-data/image-cache
```

{% note warning "Important" %}

This location must reside on a filesystem different from overlayfs (note that by default, the root filesystem of pods in a Kubernetes cluster is mounted using overlayfs).

If the location is on an overlayfs filesystem, the `containerd` daemon running inside the `jobs` container will not be able to create job containers due to the inability to mount overlayfs on top of overlayfs.

{% endnote %}


[Configuration example](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_cri.yaml) for the operator.

### Verifying Operation

You should verify that the exe node pods have started successfully after applying the configuration, that resources are available on them, and that there are no alerts.

The easiest way to do this is via the Nodes tab in the UI. When using the CLI, you can run the following commands, specifying the address of a specific node as `<node-address>`:
```
$ yt get //sys/exec_nodes/<node-address>/@state
"online"
$ yt get //sys/exec_nodes/<node-address>/@alerts
[]
$ yt get //sys/exec_nodes/<node-address>/@resource_limits
{
    "user_slots" = 32;
    "cpu" = 8.;
    ...
}
```

After that, you can try running a simple operation and make sure it executes correctly:
```
yt vanilla --proxy <cluster-address> --tasks '{task={command="sleep 1"; job_count=1; docker_image="docker.io/library/ubuntu:24.04"}}'
```

### Troubleshooting

If an alert is present on a node, the primary information about the problem will be contained directly in the alert. If the node does not transition to the `online` state, you should enter the node container and look for errors in the `exec-node.info.log` log.

Example problem (alert message):  
`failed to create containerd task: failed to create shim task: failed to mount rootfs component: invalid argument: unknown`.

For troubleshooting, it is recommended to perform the following steps:
1. Ensure that all required steps for configuring CRI have been completed.
2. Ensure that the `jobs` container is running and that the `containerd` daemon is operating inside it.
3. Ensure that the `/config/containerd.sock` socket has been created and is accessible in the `ytserver` container.
4. Inspect the logs of the `jobs` container; they can be accessed using the command `kubectl logs --container jobs <pod-name>`.

In the above example, the problem was that the `ImageCache` location was not specified. As a result, `containerd` used the default location, which was located on the pod’s root filesystem. Because of this, it was unable to create containers due to the inability to mount overlayfs on top of overlayfs.
