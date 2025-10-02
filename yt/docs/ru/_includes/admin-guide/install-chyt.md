# CHYT

Для установки CHYT на кластер {{product-name}} необходимо запушить в k8s ресурс типа `Chyt`.

{% note info "Важно" %}

На кластере должен быть поднят `strawberry controller`. Он конфигурируется в поле `strawberry` ресурса `Ytsaurus`.

{% endnote %}

Пример спецификации можно найти [по ссылке](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_chyt.yaml):
```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: mychyt
spec:
  ytsaurus:
    name:
      minisaurus
  image: ghcr.io/ytsaurus/chyt:{{chyt-version}}-relwithdebinfo
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

Если в спецификации `Chyt` был выставлен флаг `makeDefault` и на кластере запущен `strawberry controller`, то также будет поднята клика по умолчанию — `ch_public`.

