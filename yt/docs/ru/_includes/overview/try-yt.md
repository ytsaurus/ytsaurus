# Как попробовать {{product-name}}

В данном разделе можно ознакомиться с различными вариантами установки {{product-name}}.

## С использованием Docker

В целях отладки или тестирования есть возможность запустить [Docker](https://docs.docker.com/get-docker/)-контейнер {{product-name}}.
Код для развёртывания кластера доступен [по ссылке](https://github.com/ytsaurus/ytsaurus/tree/main/yt/docker/local).

Для запуска локального кластера выполните команды:
```
cd yt/docker/local
./run_local_cluster.sh
```

Вы можете подключиться к локальному кластеру, используя следующие учётные данные:
```
user_name="root",
token=""
```

## Демо-стенд

Для демонстрации возможностей системы {{product-name}} доступен стенд.
Перейдите [по ссылке](https://ytsaurus.tech/#demo) чтобы получить к нему доступ.

## Kubernetes

Для разворачивания {{product-name}} в Kubernetes рекомендуется воспользоваться [оператором](https://github.com/ytsaurus/ytsaurus-k8s-operator). Готовые docker-образы с оператором, UI, серверными компонентами и примерами можно найти в [Github Packages](https://github.com/orgs/ytsaurus/packages).

### Развёртывание в кластере Kubernetes

В данном разделе описана установка {{product-name}} в кластере Kubernetes с поддержкой динамического создания volumes, например в Managed Kubernetes в Яндекс.Облаке. Предполагается, что у вас установлена и настроена утилита kubectl. Для успешного разворачивания {{product-name}} в кластере Kubernetes должно быть как минимум три ноды, следующей конфигурации: от 4-х ядер CPU и от 8-ми GB RAM.

#### Установка оператора

1. Установите утилиту [helm](https://helm.sh/docs/intro/install/).
2. Установите cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`.
3. Установите чарт: `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Проверьте результат:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Запуск кластера {{product-name}}

Создайте пространство имён для запуска кластера. Создайте секрет, содержащий логин, пароль и токен администратора кластера.
```
kubectl create namespace <namespace>
kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

Загрузите [спецификацию](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/0.9.1/cluster_v1_demo.yaml), поправьте по необходимости и загрузите в кластер `kubectl apply -f cluster_v1_demo.yaml -n <namespace>`.

Необходимо прописать гарантии или лимиты ресурсов в секции `execNodes`, указанные значения будут отражены в конфигурации нод, и будут видны планировщику. Для надёжного хранения данных, обязательно выделите персистентные тома.

Для доступа к UI {{product-name}} можно использовать тип сервиса LoadBalancer либо отдельно настроить балансировщик для обслуживания HTTP запросов. На данный момент UI {{product-name}} не имеет встроенной возможности работать по протоколу HTTPS.

Для запуска приложений использующих кластер, используйте тот же кластер Kubernetes. В качестве адреса кластера подставьте адрес сервиса http proxy - `http-proxies.<namespace>.svc.cluster.local`.

#### Настройка логирования

По умолчанию логирование включено на уровне `info` для большинства компонент. Есть возможность включить `debug` логирование. Для этого достаточно в спеке Ytsaurus в соответствуеющей компоненте указать `loggers`, например, так:

```
loggers:
  - name: debug
    writerType: file
    minLogLevel: debug
```

### Minikube

Для корректной работы кластера необходимо 150 GiB дискового пространства и от 8-ми ядер на хосте.

#### Установка Minikube
https://kubernetes.io/ru/docs/tasks/tools/install-minikube/

Пререквизиты:
1. Установите [Docker](https://docs.docker.com/engine/install/);
2. Установите [kubectl](https://kubernetes.io/ru/docs/tasks/tools/install-kubectl/#установка-kubectl-в-linux);
3. Установите [Minikube](https://kubernetes.io/ru/docs/tasks/tools/install-minikube/);
4. Выполните команду `minikube start --driver=docker`

В результате должна успешно выполняться команда `kubectl cluster-info`.

#### Установка оператора

1. Установите утилиту [helm](https://helm.sh/docs/intro/install/).
2. Установите cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`.
3. Установите чарт: `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Проверьте результат:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Запуск кластера {{product-name}}

Загрузите [спецификацию](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/0.9.1/cluster_v1_local.yaml) в кластер `kubectl apply -f cluster_v1_local.yaml`.

Если загрузка прошла успешно, через некоторое время список запущенных подов будет выглядеть следующим образом:

```
$ kubectl get pod
NAME                                      READY   STATUS      RESTARTS   AGE
m-0                                       1/1     Running     0          2m16s
s-0                                       1/1     Running     0          2m11s
ca-0                                      1/1     Running     0          2m11s
dn-0                                      1/1     Running     0          2m11s
dn-1                                      1/1     Running     0          2m11s
dn-2                                      1/1     Running     0          2m11s
en-0                                      1/1     Running     0          2m11s
ytsaurus-ui-deployment-67db6cc9b6-nwq25   1/1     Running     0          2m11s
...
```

Настройте сетевой доступ до веб-интерфейса и прокси:
```bash
$ minikube service ytsaurus-ui --url
http://192.168.49.2:30539

$ minikube service http-proxies-lb --url
http://192.168.49.2:30228
```

По первой ссылке будет доступен веб-интерфейс. Для входа используйте:
```
Login: admin
Password: password
```

По второй ссылке можно подключиться к кластеру из [командной строки](../../api/cli/install.md) и [python client](../../api/python/start.md):
```bash
export YT_CONFIG_PATCHES='{proxy={enable_proxy_discovery=%false}}'
export YT_TOKEN=password
export YT_PROXY=192.168.49.2:30228

echo '{a=b}' | yt write-table //home/t1 --format yson
yt map cat --src //home/t1 --dst //home/t2 --format json
```

#### Удаление кластера


Чтобы удалить кластер {{product-name}} выполните команду:
```
kubectl delete -f cluster_v1_minikube.yaml
```

