# Установка {{product-name}}

## Поднятие {{product-name}} в k8s

Развернуть кластер {{product-name}} в [Kubernetes](https://kubernetes.io/) можно с помощью [оператора](#operator).

Для успешного разворачивания {{product-name}} в кластере Kubernetes, должно быть как минимум три ноды следующей конфигурации: от 4-х ядер CPU и от 8-ми GB RAM.

Перед тем как воспользоваться оператором, убедитесь, что у вас установлена и настроена утилита [kubectl](https://kubernetes.io/ru/docs/tasks/tools/install-kubectl/#установка-kubectl-в-linux).

## Kubernetes оператор { #operator }

Команда {{product-name}} поддерживает и развивает собственный k8s-оператор. Код оператора доступен в [github](https://github.com/ytsaurus/ytsaurus-k8s-operator).

Релизы оператора выкладываются в [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/k8s-operator).

Дополнительно выкладываются [helm-чарты](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart), позволяющие установить все необходимые компоненты.

### Установка оператора

1. Установите утилиту [helm](https://helm.sh/docs/intro/install/).
2. Установите cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`
3. Установите чарт `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Проверьте результат:

```bash
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Running   0           1m
```

Про обновление оператора можно прочитать в разделе [Обновление оператора](../../admin-guide/update-ytsaurus.md#operator).

## Запуск кластера {{product-name}}

Создайте пространство имён для запуска кластера:
```bash
$ kubectl create namespace <namespace>
```

Создайте секрет, содержащий логин, пароль и токен администратора кластера:
```bash
$ kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

[Подготовьте спецификацию](../../admin-guide/prepare-spec.md) `Ytsaurus` ресурса и загрузите ее в k8s ([пример спецификации](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_demo.yaml)):
```bash
$ kubectl apply -f my_ytsaurus_spec.yaml -n <namespace>
```

Кластер перейдет в состояние `Initializing`, и оператор будет пытаться поднять все необходимые компоненты.

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Initializing   None
```

За поднятием подов можно следить с помощью `kubectl get pod -n <namespace>`.

{% cut "Пример того как выглядит успешное поднятие кластера" %}

Как выглядят поды в процессе инициализации кластера.

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

Необходимо дождаться, когда оператор поднимет кластер. В статусе ресурса `Ytsaurus` должно быть состояние `Running`:
```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

### Диагностика проблем при инициализации

Если кластер завис в состоянии `Initializing`, то хорошей отправной точкой для понимания происходящего могут служить сообщения вида `Ytsaurus sync status` в логах оператора, где перечислены компоненты, которые еще не поднялись. Посмотреть логи можно с помощью `kubectl logs deployment.apps/ytsaurus-ytop-chart-controller-manager`.

{% cut "Пример логов оператора со статусом компонент" %}
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

Узнать, почему не поднялась одна из компонент, можно по полю `Conditions` в статусе ресурса `Ytsaurus`. Посмотреть статус ресурса можно с помощью `kubectl describe ytsaurus -n <namespace>`.

{% cut "Пример Conditions в процессе инициализации кластера" %}

В примере видно, что компонента `Scheduler` не начинает подниматься, так как пока не поднялась компонента `ExecNode` (`Message: Wait for ExecNode`), а та в свою очередь ждет поднятия своих подов в statefulset (`Wait for pods`).

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

Одна из возможных причин зависания кластера в состоянии `Initializing` может заключаться в том, что один из init job не завершился. Джоб мог как еще не успеть выполниться, так и пофейлиться. Необходимо изучить логи джоба с помощью `kubectl logs <init-job-pod-name> -n <namespace>`.

Другая возможная причина в том, что k8s не может запланировать поды кластера из-за нехватки k8s-нод, удовлетворяющих `resources.requests` компоненты.

{% cut "Пример неподнимающегося пода" %}

В спецификации `Ytsaurus` указана следующая группа `execNodes`:
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

Если в k8s нет ноды, у которой имеется 10 CPU, то под зависнет в состоянии `Pending`:
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

С помощью `kubectl describe` пода можно узнать причину:
```bash
$ kubectl describe pod end-0 -n <namespace>
...
Events:
  Type     Reason            Age    From               Message
  ----     ------            ----   ----               -------
  Warning  FailedScheduling  15m42s  default-scheduler  0/1 nodes are available: 1 Insufficient cpu. preemption: 0/1 nodes are available: 1 No preemption victims found for incoming pod..
```

{% endcut %}
