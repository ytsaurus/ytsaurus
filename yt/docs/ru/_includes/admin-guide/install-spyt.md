# SPYT

Для установки SPYT на кластер {{product-name}} необходимо запушить в k8s ресурс типа `Spyt`.

Пример спецификации можно найти [по ссылке](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_spyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: myspyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ghcr.io/ytsaurus/spyt:{{spyt-version}}
  sparkVersions:
    - "3.5.7"
    - "3.2.2"
```

Запушить спецификацию можно с помощью `kubectl`:

```bash
$ kubectl apply -f cluster_v1_spyt.yaml -n <namespace>
spyt.cluster.ytsaurus.tech/myspyt created
```

После этого k8s оператор запустит несколько Init Job, которые запишут в Кипарис необходимые файлы. Следить за статусом можно с помощью `kubectl`:

```bash
$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   CreatingUser

$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   UploadingIntoCypress

$ kubectl get spyt
NAME     RELEASESTATUS
myspyt   Finished
```

После успешного выполнения всех джобов (когда `RELEASESTATUS` перешел в `Finished`), можно запускать `SPYT`. Подробнее можно почитать [в отдельном разделе](../../user-guide/data-processing/spyt/launch).

Для открытия Spark UI в браузере можно установить [Task proxy](../../admin-guide/install-task-proxy.md).
