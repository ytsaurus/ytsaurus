# Установка дополнительных компонент

## SPYT { #spyt }

Для установки SPYT на кластер {{product-name}} необходимо запушить в k8s ресурс типа `Spyt`.

Пример спецификации можно найти [по ссылке](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_spyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: myspyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ytsaurus/spyt:1.72.0
```

Запушить спецификацию можно с помощью `kubectl`:

```bash
$ kubectl apply -f cluster_v1_spyt.yaml -n <namespace>
spyt.cluster.ytsaurus.tech/myspyt created
```

После этого k8s оператор запустит несколько init job, которые запишут в Кипарис необходимые файлы. Следить за статусом можно с помощью `kubectl`:

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

После успешного выполнения всех джобов (когда `RELEASESTATUS` перешел в `Finished`), можно запускать `SPYT`. Подробнее можно почитать [в отдельном разделе](../../user-guide/data-processing/spyt/quick-start).

## CHYT { #chyt }

Для установки CHYT на кластер {{product-name}} необходимо запушить в k8s ресурс типа `Chyt`.

{% note info "Важно" %}

На кластере должен быть поднят `strawberry controller`. Он конфигурируется в поле `strawberry` ресурса `Ytsaurus`.

{% endnote %}

Пример спецификации можно найти [по ссылке](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_chyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: mychyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ytsaurus/chyt:2.10.0-relwithdebinfo
  makeDefault: true
```

Запушить спецификацию можно с помощью `kubectl`:

```bash
$ kubectl apply -f cluster_v1_chyt.yaml -n <namespace>
chyt.cluster.ytsaurus.tech/mychyt created
```

После этого k8s оператор запустит несколько init job, которые запишут в Кипарис необходимые файлы.

```bash
$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   CreatingUser

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   UploadingIntoCypress

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   CreatingChPublicClique

$ kubectl get chyt
NAME     RELEASESTATUS
mychyt   Finished
```

После успешного выполнения всех джобов, можно запускать клику `CHYT`. Подробнее про клики можно почитать [в отдельном разделе](../../user-guide/data-processing/chyt/cliques/start).

Если в спецификации `Chyt` был выставлен флаг `makeDefault` и на кластере запущен `strawberry controller`, то также будет поднята клика по умолчанию  — `ch_public`.

