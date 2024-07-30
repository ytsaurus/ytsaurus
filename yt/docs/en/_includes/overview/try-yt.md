# Try {{product-name}}

This section describes various installation options of the {{product-name}}.

## Using Docker

For debugging or testing purposes, it is possible to run [Docker](https://docs.docker.com/get-docker/)-container {{product-name}}.
The code for cluster deployment is available [by the link](https://github.com/ytsaurus/ytsaurus/tree/main/yt/docker/local).

To start a local cluster, run the commands:
```
cd yt/docker/local
./run_local_cluster.sh
```

You can connect to the local cluster using the following credentials:
```
user_name="root"
token=""
```

## Demo Stand

A stand is available to demonstrate the capabilities of the {{product-name}} system.
Go to [link](https://ytsaurus.tech/#demo ) to get access to it.

## Kubernetes

To deploy {{product-name}} in Kubernetes, it is recommended to use the [operator](https://github.com/ytsaurus/ytsaurus-k8s-operator). Ready-made docker images with operator, UI, server components and examples can be found in [Github Packages](https://github.com/orgs/ytsaurus/packages).

### Deployment in a Kubernetes cluster

This section describes the installation of {{product-name}} in a Kubernetes cluster with support for dynamic creation of volumes, for example in Managed Kubernetes in Yandex.Cloud. It is assumed that you have the kubectl utility installed and configured. For successful deployment of {{product-name}}, there must be at least three nodes in the Kubernetes cluster, of the following configuration: at least 4 CPU cores and 8 GB RAM.

#### Installing the operator

1. Install the [helm utility](https://helm.sh/docs/intro/install/).
2. Install cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`.
3. Install the chart: `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Check the result:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Starting {{product-name}} cluster

Create a namespace to run the cluster. Create a secret containing the username, password, and token of the cluster administrator.
```
kubectl create namespace <namespace>
kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=<password> --from-literal=token=<password>  -n <namespace>
```

Download [specification](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/0.9.1/cluster_v1_demo.yaml) , correct as necessary and upload to the cluster `kubectl apply -f cluster_v1_demo.yaml -n <namespace>`.

It is necessary to specify guarantees or resource limits in the `execNodes` section, the specified values will be reflected in the node configuration, and will be visible to the scheduler. For reliable data storage, be sure to allocate persistent volumes.

To access the {{product-name}} UI, you can use the LoadBalancer service type or configure the load balancer separately to service HTTP requests. Currently, the {{product-name}} UI does not have the built-in HTTPS support.

To run applications using a cluster, use the same Kubernetes cluster. As the cluster address, substitute the address of the http proxy service - `http-proxies.<namespace>.svc.cluster.local`.

### Minikube

You need 150 GB of disk space and at least 8 cores on the host for the cluster to work correctly.

#### Installing Minikube

Minikube installation guide is available via [link](https://kubernetes.io/docs/tasks/tools/#minikube).

Prerequisites:
1. Install [Docker](https://docs.docker.com/engine/install/);
2. Install [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl);
3. Install [Minikube](https://kubernetes.io/docs/tasks/tools/#minikube);
4. Run the command `minikube start --driver=docker`.

As a result, the `kubectl cluster-info` command should be executed successfully.

#### Installing the operator

1. Install the [helm utility](https://helm.sh/docs/intro/install/).
2. Install cert-manager: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml`.
3. Install the chart: `helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version {{k8s-operator-version}}`.
4. Check the result:

```
$ kubectl get pod
NAME                                                      READY   STATUS     RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-5765c5f995-dntph   2/2     Running    0          7m57s
```

#### Starting {{product-name}} cluster

Download [specification](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/0.9.1/cluster_v1_local.yaml) to the cluster via `kubectl apply -f cluster_v1_local.yaml`.

If the download was successful, after a while the list of running hearths will look like this:

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

Configure network access to the web interface and proxy
```bash
$ minikube service ytsaurus-ui --url
http://192.168.49.2:30539

$ minikube service http-proxies-lb --url
http://192.168.49.2:30228
```

The web interface will be available at the first link. To log in, use:
```
Login: admin
Password: password
```

The second link allows you to connect to the cluster from the [command line](../../api/cli/install.md) and [python client](../../api/python/start.md):
```bash
export YT_CONFIG_PATCHES='{proxy={enable_proxy_discovery=%false}}'
export YT_TOKEN=password
export YT_PROXY=192.168.49.2:30228

echo '{a=b}' | yt write-table //home/t1 --format yson
yt map cat --src //home/t1 --dst //home/t2 --format json
```

### Deleting a cluster

To delete a {{product-name}} cluster, run the command:
```
kubectl delete -f cluster_v1_minikube.yaml
```
