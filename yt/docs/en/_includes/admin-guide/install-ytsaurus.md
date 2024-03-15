# Installing {{product-name}}

## Deploying {{product-name}} in K8s

You can deploy the {{product-name}} cluster in [Kubernetes](https://kubernetes.io/) using the [operator](#operator).

To successfully deploy {{product-name}} in a Kubernetes cluster, there must be at least three nodes with at least four CPU cores and 8 GB of RAM.

Before using the operator, make sure you have the [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) utility installed and configured.

## Kubernetes operator { #operator }

The {{product-name}} team supports and develops its own K8s operator. The operator code is available on [GitHub](https://github.com/ytsaurus/yt-k8s-operator).

Operator releases are posted on [Docker Hub](https://hub.docker.com/r/ytsaurus/k8s-operator/tags).

Additionally, [helm charts](https://hub.docker.com/r/ytsaurus/ytop-chart/tags) are laid out so you can install all the components you need.

### Installing the operator

1. Install the [helm](https://helm.sh/docs/intro/install/) utility.
2. Install cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`
3. Install the charts `helm install ytsaurus oci://registry-1.docker.io/ytsaurus/ytop-chart --version 0.4.1`.
4. Check the result:

```bash
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Running   0           1m
```

You can read about updating the operator in [that section](../../admin-guide/update-ytsaurus.md#operator).

## Starting cluster {{product-name}}

Create a namespace to run the cluster:
```bash
$ kubectl create namespace <namespace>
```

Create a secret containing the login, password, and token of the cluster administrator:
```bash
$ kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

[Prepare the `Ytsaurus` specification](../../admin-guide/prepare-spec.md) for the resource and upload it to K8s ([sample specification](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_demo.yaml)):
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
