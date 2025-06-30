# Installing {{product-name}}

## Deploying {{product-name}} in K8s

You can deploy the {{product-name}} cluster in [Kubernetes](https://kubernetes.io/) using the [operator](#operator).

To successfully deploy {{product-name}} in a Kubernetes cluster, there must be at least three nodes with at least four CPU cores and 8 GB of RAM.

Before using the operator, make sure you have the [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) utility installed and configured.

## Kubernetes operator { #operator }

The {{product-name}} team supports and develops its own K8s operator. The operator code is available on [GitHub](https://github.com/ytsaurus/ytsaurus-k8s-operator).

Operator releases are posted on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/k8s-operator).

Additionally, [helm charts](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart) are laid out so you can install all the components you need.

### Installing the operator

1. Install the [helm](https://helm.sh/docs/intro/install/) utility.
2. Install cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`
3. Install the charts `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Check the result:

```bash
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Running   0           1m
```

You can read about updating the operator in [that section](../../admin-guide/update-ytsaurus.md#operator).

## Starting cluster {{product-name}} {#starting-cluster}

Create a namespace to run the cluster:
```bash
$ kubectl create namespace <namespace>
```

Create a secret containing the login, password, and token of the cluster administrator:
```bash
$ kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

[Prepare the `Ytsaurus` specification](../../admin-guide/prepare-spec.md) for the resource and upload it to K8s ([sample specification](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_demo.yaml)):
```bash
$ kubectl apply -f my_ytsaurus_spec.yaml -n <namespace>
```

The cluster will move to the `Initializing` state, and the operator will go to start all the necessary components.

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Initializing   None
```

You can track how pods are started using `kubectl get pod -n <namespace>`.

{% cut "Sample successfully raised cluster" %}

What the pods look like during cluster initialization.

```bash
$ kubectl get pod -n <namespace>
NAME                                                    READY   STATUS     RESTARTS   AGE
ds-0                                                    0/1     Init:0/1   0          1s
ms-0                                                    0/1     Init:0/1   0          1s
ytsaurus-ytop-chart-controller-manager-fbbffc97-6stk8   2/2     Running    0          2m43s

$ kubectl get pod -n <namespace>
NAME                                                    READY   STATUS      RESTARTS   AGE
ds-0                                                    1/1     Running     0          28s
ms-0                                                    1/1     Running     0          28s
yt-master-init-job-default-vtkzw                        0/1     Completed   1          25s
ytsaurus-ytop-chart-controller-manager-fbbffc97-6stk8   2/2     Running     0          3m10s

$ kubectl get pod -n <namespace>
NAME                                                    READY   STATUS              RESTARTS   AGE
ca-0                                                    0/1     PodInitializing     0          9s
dnd-0                                                   1/1     Running             0          10s
ds-0                                                    1/1     Running             0          41s
end-0                                                   0/1     Init:0/1            0          9s
hp-0                                                    1/1     Running             0          10s
hp-control-0                                            0/1     PodInitializing     0          9s
ms-0                                                    1/1     Running             0          41s
rp-0                                                    0/1     PodInitializing     0          9s
rp-heavy-0                                              0/1     PodInitializing     0          9s
yt-master-init-job-default-vtkzw                        0/1     Completed           1          38s
yt-ui-init-job-default-2rsfz                            0/1     ContainerCreating   0          8s
ytsaurus-ytop-chart-controller-manager-fbbffc97-6stk8   2/2     Running             0          3m23s

$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Initializing        None

$ kubectl get pod -n <namespace>
NAME                                                    READY   STATUS      RESTARTS   AGE
ca-0                                                    1/1     Running     0          21s
dnd-0                                                   1/1     Running     0          22s
ds-0                                                    1/1     Running     0          53s
end-0                                                   1/1     Running     0          21s
hp-0                                                    1/1     Running     0          22s
hp-control-0                                            1/1     Running     0          21s
ms-0                                                    1/1     Running     0          53s
rp-0                                                    1/1     Running     0          21s
rp-heavy-0                                              1/1     Running     0          21s
sch-0                                                   1/1     Running     0          7s
yt-client-init-job-user-jtl8p                           0/1     Completed   0          11s
yt-master-init-job-default-vtkzw                        0/1     Completed   1          50s
yt-ui-init-job-default-2rsfz                            0/1     Completed   0          20s
ytsaurus-ui-deployment-7b5d4776df-w42mj                 1/1     Running     0          5s
ytsaurus-ytop-chart-controller-manager-fbbffc97-6stk8   2/2     Running     0          3m35s

$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

{% endcut %}

Wait for the operator to create the cluster. The `Ytsaurus` resource should be in the `Running` state:
```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

### Diagnosing problems during initialization

If the cluster is stuck in the `Initializing` state, start debugging by checking `Ytsaurus sync status` messages in the operator logs, which list the components that didn't start properly. To view the logs, use the command `kubectl logs deployment.apps/ytsaurus-ytop-chart-controller-manager`.

{% cut "Sample operator logs with component status" %}
```bash
kubectl logs deployment.apps/ytsaurus-ytop-chart-controller-manager
...
2023-09-10T11:43:00.405Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "a9fcb896-5976-4d60-b18b-730a2c969758", "notReadyComponents": ["Discovery", "Master", "YtsaurusClient", "DataNode", "HttpProxy", "HttpProxy-control", "UI", "RpcProxy", "RpcProxy-heavy", "ExecNode", "Scheduler", "ControllerAgent"], "readyComponents": [], "updateState": "", "clusterState": ""}
...
2023-09-10T11:43:37.743Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "ff6e6ff0-bee0-40dc-9573-894d72b5cfc2", "notReadyComponents": ["YtsaurusClient", "DataNode", "HttpProxy", "HttpProxy-control", "UI", "RpcProxy", "RpcProxy-heavy", "ExecNode", "Scheduler", "ControllerAgent"], "readyComponents": ["Discovery", "Master"], "updateState": "None","clusterState": "Initializing"}
...
2023-09-10T11:43:46.403Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "01b7b375-0376-4cfb-bb98-d1866ae0488d", "notReadyComponents": ["YtsaurusClient", "UI", "Scheduler"], "readyComponents": ["Discovery", "Master", "DataNode", "HttpProxy", "HttpProxy-control", "RpcProxy", "RpcProxy-heavy", "ExecNode", "ControllerAgent"], "updateState": "None", "clusterState": "Initializing"}
...
2023-09-10T11:43:56.632Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "08cd28ca-05ba-4628-88cd-20cf7bbae77d", "notReadyComponents": ["YtsaurusClient"], "readyComponents": ["Discovery", "Master", "DataNode", "HttpProxy", "HttpProxy-control", "UI", "RpcProxy", "RpcProxy-heavy", "ExecNode", "Scheduler", "ControllerAgent"], "updateState": "None","clusterState": "Initializing"}
...
2023-09-10T11:43:57.507Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "39326ae0-b4ad-4f1e-923a-510d9ac73405", "notReadyComponents": [], "readyComponents": ["Discovery", "Master", "YtsaurusClient", "DataNode", "HttpProxy", "HttpProxy-control", "UI", "RpcProxy", "RpcProxy-heavy", "ExecNode", "Scheduler", "ControllerAgent"], "updateState": "None", "clusterState": "Initializing"}
...
2023-09-10T11:43:59.212Z        INFO    Ytsaurus sync status    {"controller": "ytsaurus", "controllerGroup": "cluster.ytsaurus.tech", "controllerKind": "Ytsaurus", "ytsaurus": {"name":"minisaurus","namespace":"default"}, "namespace": "default", "name": "minisaurus", "reconcileID": "4bf01d74-affe-4173-ac9e-b697b9c356de", "notReadyComponents": [], "readyComponents": ["Discovery", "Master", "YtsaurusClient", "DataNode", "HttpProxy", "HttpProxy-control", "UI", "RpcProxy", "RpcProxy-heavy", "ExecNode", "Scheduler", "ControllerAgent"], "updateState": "None", "clusterState": "Running"}
```
{% endcut %}

To find out why a component isn't running, check the `Conditions` field in the `Ytsaurus` resource status. You can view the status by executing the command `kubectl describe ytsaurus -n <namespace>`.

{% cut "Conditions sample during cluster initialization" %}

The example shows that the `Scheduler` component isn't initializing since the `ExecNode` component is not running yet (`Message:Wait for ExecNode`), as it awaits its pods in statefulset (`Wait for pods`).

```yaml
Status:
  Conditions:
    Last Transition Time:  2023-09-10T11:53:43Z
    Message:               Ready
    Reason:                Ready
    Status:                True
    Type:                  DiscoveryReady
    Last Transition Time:  2023-09-10T11:54:27Z
    Message:               yt-master-init-job-default completed
    Reason:                Ready
    Status:                True
    Type:                  MasterReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for HttpProxy
    Reason:                Blocked
    Status:                False
    Type:                  YtsaurusClientReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  DataNodeReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  HttpProxyReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  HttpProxy-controlReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for yt-ui-init-job-default completion
    Reason:                Blocked
    Status:                False
    Type:                  UIReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  RpcProxyReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  RpcProxy-heavyReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  ExecNodeReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for ExecNode
    Reason:                Blocked
    Status:                False
    Type:                  SchedulerReady
    Last Transition Time:  2023-09-10T11:53:33Z
    Message:               Wait for pods
    Reason:                Blocked
    Status:                False
    Type:                  ControllerAgentReady
    Last Transition Time:  2023-09-10T11:54:25Z
    Message:               Init job successfully completed
    Reason:                InitJobCompleted
    Status:                True
    Type:                  defaultMasterInitJobCompleted
  State:                   Initializing
```

{% endcut %}

One possible reason for the cluster getting stuck in the `Initializing` state could be that one of the init jobs is still running, either because it didn't have had enough time to complete or failed. Check the job logs using the command `kubectl logs <init-job-pod-name> -n <namespace>`.

Another possible reason is that K8s isn't able to schedule cluster sub-nodes due to a shortage of K8s nodes satisfying `resources.requests` components.

{% cut "Sample pod that can't be deployed" %}

The `Ytsaurus` specification has the following `execNodes` group set:
```yaml
  execNodes:
    - instanceCount: 1
      loggers: *loggers
      resources:
        limits:
          cpu: 10
          memory: 2Gi
        requests:
          cpu: 10

      volumeMounts:
        - name: node-data
          mountPath: /yt/node-data

      volumes:
        - name: node-data
          emptyDir:
            sizeLimit: 5Gi
```

If there isn't a node with 10 CPUs in K8s, the pod will get stuck in the `Pending` state:
```bash
$  kubectl get pod -n <namespace>
NAME                                                    READY   STATUS    RESTARTS   AGE
ca-0                                                    1/1     Running   0          14m
dnd-0                                                   1/1     Running   0          14m
ds-0                                                    1/1     Running   0          15m
end-0                                                   0/1     Pending   0          15m
hp-0                                                    1/1     Running   0          14m
hp-control-0                                            1/1     Running   0          14m
ms-0                                                    1/1     Running   0          15m
...
```

You can find out why using `kubectl describe`:
```bash
$ kubectl describe pod end-0 -n <namespace>
...
Events:
  Type     Reason            Age    From               Message
  ----     ------            ----   ----               -------
  Warning  FailedScheduling  15m42s  default-scheduler  0/1 nodes are available: 1 Insufficient cpu. preemption: 0/1 nodes are available: 1 No preemption victims found for incoming pod..
```

{% endcut %}

## Installing {{product-name}} UI helm chart

### Using with [ytop-chart](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart)

Follow all required steps to [start the {{product-name}} cluster](#starting-cluster). Then install the chart:

```
git clone https://github.com/ytsaurus/ytsaurus-ui.git
helm upgrade --install ytsaurus-ui ytsaurus-ui/packages/ui-helm-chart/
```

### Using with a custom cluster

#### Pre-requirements

The instructions below describe how to start the {{product-name}} UI from the helm chart. You are supposed to have already:

* configured the `kubectl` cli-tool (for example, use [minikube](https://minikube.sigs.k8s.io/docs/start/)),
* started your {{product-name}} cluster and know the hostname of `http_proxy`,
* prepared a special robot-user for the {{product-name}} UI and ready to provide its token (see the [Token management](../../user-guide/storage/auth.md#token-management) section).

#### Quick start

By default, the chart expects existence of `yt-ui-secret` with the `yt-interface-secret.json` key. The secret can be created by the following commands:

```
read -sp "TOKEN: " TOKEN ; echo '{"oauthToken":"'$TOKEN'"}' > tmp.json
kubectl create secret generic yt-ui-secret --from-literal="yt-interface-secret.json=$(cat tmp.json)" && rm tmp.json
```

Also, you have to provide a description of your cluster:

```
read -p "Cluster id: " id_; read -p "http_proxy hostname: " proxy_; read -p "Use https [true/false]: " secure_; read -p "NODE_TLS_REJECT_UNAUTHORIZED [1/0]: " tlsrej_; (
tee values.yaml << _EOF
ui:
  env:
    - name: NODE_TLS_REJECT_UNAUTHORIZED
      value: "$tlsrej_"
    - name: ALLOW_PASSWORD_AUTH
      value: "1"
  clusterConfig:
    clusters:
      - authentication: basic
        id: $id_
        proxy: $proxy_
        description: My first YTsaurus. Handle with care.
        environment: testing
        group: My YTsaurus clusters
        name: my cluster
        primaryMaster:
          cellTag: 1
        secure: $secure_
        theme: lavander
_EOF
)
```

Then you are ready to install or upgrade the chart:

```
git clone https://github.com/ytsaurus/ytsaurus-ui.git
helm upgrade --install yt-ui ytsaurus-ui/packages/ui-helm-chart/ -f values.yaml
# or run specific version of UI (all versions: https://github.com/ytsaurus/ytsaurus-ui/pkgs/container/ui)
helm upgrade --install yt-ui ytsaurus-ui/packages/ui-helm-chart/ -f values.yaml --set ui.image.tag=1.60.1
```

You may want to add port-forwarding to open the {{product-name}} UI in your browser:

```
kubectl port-forward deployment/yt-ui-ytsaurus-ui-chart 8080:80
```

## Cron Installation

### Description

For {{product-name}}, there is a set of cron jobs that are useful for cluster operations, such as cleaning up temporary directories or removing inactive nodes. Both built-in and custom jobs are supported.

List of built-in scripts:

* `clear_tmp` – a script that cleans temporary files on the cluster. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cron/clear_tmp).
* `prune_offline_cluster_nodes` – a script that removes offline nodes from Cypress. The script’s source code lives [here](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cron/prune_offline_cluster_nodes).

### Prerequisites

At this point, you should have:

* Helm 3.x
* a running {{product-name}} cluster and the HTTP proxy address (`http_proxy`)
* a dedicated robot user for Cron with a token issued to it (see [Token Management](../../user-guide/storage/auth.md#token-management))

### Basic Installation

```bash
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart --version 0.0.2 --set yt.proxy="http_proxy" --set yt.token="<ROBOT-CRON-TOKEN>" --set image.tag="0.0.2"
```

### Configuration

All chart parameters can be set via `values.yaml` or overridden with `--set` on the command line.

### Authentication in YTsaurus

Specify the token directly:

```yaml
yt:
  proxy: yt.company.com
  token: Qwerty123!
```

Or configure it via a Kubernetes Secret:

```yaml
unmanagedSecret:
  enabled: true
  secretKeyRef:
    name: ytadminsec
    key: token
```

### Built-in Jobs (`jobs`)

Each job is defined by the following structure:
- `name`: a unique job name
- `enabled`: whether the job is enabled
- `args`: command-line arguments
- `schedule`: cron-format schedule
- `restartPolicy`: restart policy (recommended: `Never`)

Example of enabling a job:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart --version 0.0.2 --set jobs[1].enabled=true --set jobs[1].args[5]="tmp_files"
```

Array indexing of `jobs` starts at zero—keep track of the job order.

### Custom Jobs

You can define your own jobs:

```yaml
additionalJobs:
  - name: my_cleanup
    enabled: true
    args:
      - clear_tmp
      - --directory "//my/custom/path"
    schedule: "0 */6 * * *"
    restartPolicy: Never
```

### Example `values.yaml`

```yaml
yt:
  proxy: yt.mycompany.com
  token: my-secret-token

jobs:
  - name: clear_tmp_files
    enabled: true
    args:
      - clear_tmp
      - --directory "//tmp/yt_wrapper/file_storage"
      - --account "tmp_files"
    schedule: "*/30 * * * *"
    restartPolicy: Never

unmanagedSecret:
  enabled: false
```

To deploy:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart --version 0.0.2 -f my-values.yaml
```

### Commonly Used Parameters

| Parameter                      | Description                                         |
|--------------------------------|-----------------------------------------------------|
| `yt.proxy`                     | HTTP proxy for accessing {{product-name}}           |
| `yt.token`                     | Access token (if `unmanagedSecret` is disabled)     |
| `unmanagedSecret`              | Use a Kubernetes Secret                             |
| `image.repository`             | Docker image repository                             |
| `image.tag`                    | Docker image tag                                    |
| `schedule`                     | Default schedule (if not specified per job)         |
| `concurrencyPolicy`            | `Allow`, `Forbid`, or `Replace`                     |
| `successfulJobsHistoryLimit`   | Number of successful job records to keep            |
| `failedJobsHistoryLimit`       | Number of failed job records to keep                |

