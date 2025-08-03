# Обновление серверных компонент {{product-name}}

## Выбор образа и старт обновления

Периодически выпускаются новые релизы, а также вносятся исправления в рамках текущих стабильных релизов. Почитать подробнее про выпущенные релизы можно [в отдельном разделе](../../admin-guide/releases.md). Докер-образы со стабильными релизами имеют вид `ghcr.io/ytsaurus/ytsaurus:23.N.M`, например `ghcr.io/ytsaurus/ytsaurus:{{yt-server-version}}`.

Kubernetes оператор поддерживает обновление докер-образов, используемых для серверных компонент. Для обновления необходимо поменять `coreImage` в спецификации {{product-name}} и запушить её в k8s с помощью `kubectl apply -f my_ytsaurus_spec.yaml -n <namespace>`.

{% note warning "Внимание" %}

Можно обновляться только на более свежие версии. Если попытаться обновиться на более старый образ, мастера не смогут подняться. После успешного обновления на свежую версию откатиться обратно уже невозможно.

Крайне не рекомендуется обновляться на `dev` версию, собранную из текущей `main` ветки — такие версии {{product-name}} не являются стабильными и не оттестированы. При этом обновиться обратно на последний выпущенный стабильный релиз будет невозможно по описанным выше причинам.

{% endnote %}

{% note warning "Важно" %}

Перед обновлением {{product-name}} необходимо обновить оператор на последний релиз. Работоспособность новых стабильных образов не гарантируется со старым оператором, а также не гарантируется работоспособность оператора с нестабильными версиями {{product-name}}, например с `dev`. Найти список стабильных образов можно на страницe [Релизы](../../admin-guide/releases).

{% endnote %}

## Статус обновления

Следить за статусом обновления можно с помощью статуса ресурса `Ytsaurus`:
```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE      UPDATESTATE            UPDATINGCOMPONENTS
minisaurus   Updating          WaitingForPodsRemoval
```

Если обновление началось, то кластер переходит в состояние `Updating` (`CLUSTERSTATE`) и далее можно следить за состоянием обновления (`UPDATESTATE`). Более детальную информацию о происходящем можно узнать, выполнив `kubectl describe ytsaurus -n <namespace>`.

После окончания обновления кластер снова переходит в состояние `Running`:

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

## Полное и частичное обновление

В спецификации `Ytsaurus` можно указать как один образ на все серверные компоненты (`coreImage`), так и разные образы для разных компонент (в поле `image` компонент). Указывать отдельные образы следует в редких случаях, желательно предварительно обсудить такое решение с [командой {{product-name}}](https://ytsaurus.tech/#contact). Если у компоненты указан свой собственный образ, то будет использоваться он. При изменении `image` также запускается обновление кластера, но в результате обновится только часть компонент.

## Обновление статических конфигов { #configs }

По спецификации `Ytsaurus` оператор генерирует статические конфиги компонент. Поэтому при некоторых изменениях (например, при изменении `locations`) может потребоваться перегенерировать конфиг, а также перезапустить поды соответствующей компоненты. Поэтому при изменении конфига также будет запускаться обновление.

## Невозможность обновления { #impossible }

Существует ряд ситуаций, в которых запускать обновление может быть небезопасно. Поэтому оператор перед стартом обновления выполняет различные проверки — например, проверяет, живы ли на данный момент все tablet cell bundles. Если по результатам проверок оператор приходит к выводу, что обновление сейчас невозможно, то выставляется состояние обновления `ImpossibleToStart`.

```bash
$ kubectl get ytsaurus -n <namespace>
NAME         CLUSTERSTATE   UPDATESTATE           UPDATINGCOMPONENTS
minisaurus   Updating       ImpossibleToStart
```

Узнать причину можно в статусе `Ytsaurus`, выполнив `kubectl describe ytsaurus -n <namespace>` и посмотрев на `Conditions` в `UpdateStatus`.

{% cut "Пример причины невозможности запуска обновления" %}
```bash
$ kubectl describe ytsaurus -n <namespace>
...
  Update Status:
    Conditions:
      Last Transition Time:  2023-09-26T09:18:11Z
      Message:               Tablet cell bundles ([sys default]) aren't in 'good' health
      Reason:                Update
      Status:                True
      Type:                  NoPossibility
    State:                   ImpossibleToStart
```
{% endcut %}


Если подобное произошло, необходимо вернуть спецификацию в прежнее значение, чтобы образы компонент не поменялись, а также по спецификации генерировались те же статические конфиги, что и раньше. Далее оператор отменит обновление и переведет {{product-name}} обратно в состояние `Running`.

## Ручное вмешательство

В процессе обновления потенциально могут возникнуть различные проблемы и может понадобиться ручное вмешательство. Для этого в спецификации `Ytsaurus` можно выставить флаг `isManaged=false`. После этого оператор прекратит что-либо делать с данным кластером и можно делать ручные действия.

{% note warning "Внимание" %}

В случае возникновения непредвиденных проблем в процессе обновления рекомендуется проконсультироваться с [командой {{product-name}}](https://ytsaurus.tech/#contact) перед осуществлением каких-либо ручных действий.

{% endnote %}

## Обновление оператора { #operator }

{% note warning "Внимание" %}

Перед обновлением оператора кластер должен находиться в работоспособном состоянии. Например, на кластере не должно быть [lvc](../../admin-guide/problems/#lvc), а также все [tablet cell bundles](../../admin-guide/problems/#tabletcellbundles) должны быть живы.

{% endnote %}

### Инструкция

1. Запустите обновление чарта:
    ```bash
    helm upgrade ytsaurus --install oci://ghcr.io/ytsaurus/ytop-chart --version <new-version>
    ```
   Список доступных версий оператора приведён на [странице релизов](../../admin-guide/releases.md#kubernetes-operator).
2. Проверьте, что старые поды оператора удалились, а новые создались:
    ```bash
    $ kubectl get pod -n <namespace>
    NAME                                                      READY   STATUS        RESTARTS   AGE
    ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running       0          21s
    ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Terminating   0          23h

    $ kubectl get pod -n <namespace>
    NAME                                                      READY   STATUS    RESTARTS   AGE
    ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running   0          25s
    ```
3. При необходимости обновите CRD.
    По умолчанию helm не обновляет ранее установленные [CRD](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#customresourcedefinitions), поэтому для использования новых полей спеки может потребоваться обновить их вручную. Имейте в виду — выполнять эту процедуру следует с осторожностью из-за риска потерять данные. Подробнее написано в [документации к helm](https://helm.sh/docs/topics/charts/#limitations-on-crds).

    Чтобы вручную обновить CRD, скачайте чарт локально и затем обновите CRD с помощью `kubectl replace`:
    ```bash
    $ helm pull oci://ghcr.io/ytsaurus/ytop-chart --version <new-version> --untar
    $ helm template ytop-chart --output-dir ./templates
    wrote ./templates/ytop-chart/templates/serviceaccount.yaml
    wrote ./templates/ytop-chart/templates/manager-config.yaml
    ...
    $ kubectl replace -f ./templates/ytop-chart/templates/crds/<crd-to-update>
    ...
    ```
    Вероятнее всего, вам может потребоваться обновить следующие CRD:
    * ```chyts.cluster.ytsaurus.tech.yaml```
    * ```spyts.cluster.ytsaurus.tech.yaml```
    * ```ytsaurus.cluster.ytsaurus.tech.yaml```

### Возможное автоматическое обновление кластера

Разные версии оператора могут генерировать разные конфиги для одних и тех же компонент — например, в новой версии оператора может добавиться новое поле. В таком случае сразу после запуска оператора будет запущено обновление кластера.

Если обновление окажется [невозможным](#impossible), то кластер останется в состоянии `Updating`, а статус обновления станет `ImpossibleToStart`. В таком случае можно откатить оператор, тогда обновление отменится и кластер перейдёт в состояние `Running`. Либо можно выставить флаг `enableFullUpdate = false` в спецификации `Ytsaurus`, в таком случае обновление также будет отменено и новый оператор не будет больше пытаться запускать обновление кластера. После этого можно привести кластер в рабочее состояние и повторить попытку обновления, выставив флаг `enableFullUpdate = true`.

