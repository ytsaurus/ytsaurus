# Обновление серверных компонентов {{product-name}}

{% note warning "Важно" %}

Ниже описано, как выполнить обновление с использованием оператора версии **0.32.0** и новее. О различиях со старыми версиями смотрите в разделе [Переход с enableFullUpdate на updatePlan](../../../admin-guide/update/update_strategy.md#migration-from-enablefullupdate).

{% endnote %}

В этой статье описан процесс полного обновления серверных компонентов кластера {{product-name}} через Kubernetes-оператор: как подготовиться к обновлению, обновить оператор, изменить спецификацию `Ytsaurus`, отследить ход обновления и проверить результат.

## Что важно знать перед обновлением {#important-things-to-know}

Обновляйтесь только на следующую мажорную версию.
: Обновляйте кластер по порядку мажорных версий, не пропуская промежуточные: сначала `25.1 → 25.2`, затем `25.2 → 25.3`. Пропуск версии может нарушить работу кластера. Внутри целевой мажорной версии вы можете выбрать самую свежую минорную и получить последние обновления — например, `25.2.0 → 25.3.2`. {% if audience == "public" %}Доступные версии смотрите в [списке релизов](../../../admin-guide/releases.md#server).{% endif %}

Обновление нельзя отменить.
: После того как мастер-серверы вышли из режима `read-only` и применили необратимые изменения, откат на предыдущую версию не поддерживается.

Сначала обновляйте оператор, потом кластер.
: Перед обновлением кластера обновите оператор. Его версия должна быть совместима с целевой версией кластера.{% if audience == "public" %} Проверьте совместимость по [таблице совместимости](../../../admin-guide/compatibility.md).{% endif %}

Во время обновления кластер может быть недоступен.
: По умолчанию кластер полностью недоступен на время обновления — все операции и запросы прерываются. Планируйте обновление на период минимальной нагрузки. Если кластер должен оставаться доступным, выберите другую стратегию — смотрите [Стратегии обновления](../../../admin-guide/update/update_strategy.md).

{% note info "Обновление без остановки кластера" %}

Если требуется обновить кластер без остановки, настройте стратегию `RollingUpdate` через поле `updatePlan`. Пример — в разделе [Обновление без остановки кластера через RollingUpdate](#rolling-update-scenario).

{% endnote %}

## Обновление кластера {#update}

Процесс обновления состоит из четырёх шагов:

1. [Подготовьте кластер](#before-update)
1. [Обновите оператор](#update-operator)
1. [Запустите обновление](#change-spec)
1. [Проверьте результат](#verify)

### 1\. Подготовьте кластер к обновлению {#before-update}

Перед изменением спецификации `Ytsaurus` проверьте текущее состояние кластера и убедитесь, что оператор готов начать новый цикл обновления.

1. Узнайте текущую версию кластера:

   ```bash
   $ kubectl get ytsaurus <cluster_name> -n <namespace> -o jsonpath='{.spec.coreImage}'
   
   ghcr.io/ytsaurus/ytsaurus:stable-25.1.0-relwithdebinfo
   ```

1. Проверьте состояние кластера:

   ```bash
   $ kubectl get ytsaurus <cluster_name> -n <namespace>
   
   NAME       CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS   BLOCKEDCOMPONENTS
   ytsaurus   Running
   ```

   Перед обновлением кластер должен находиться в состоянии `Running`. Если кластер в другом состоянии, сначала устраните проблемы — смотрите раздел [Решение проблем](#troubleshooting).

1. Убедитесь, что оператор готов к обновлению. Для этого проверьте его логи:

   ```bash
   kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
   
   # Если имя deployment отличается, получите его список
   
   kubectl get deployments -n <namespace>
   ```

   Это важный шаг: оператор не должен быть занят незавершёнными действиями. Переходите к следующему шагу только тогда, когда в логах есть сообщение `INFO Ytsaurus is running and happy`. Это значит, что оператор считает кластер согласованным и не выполняет незавершённых действий.

   {% note warning "Важно" %}
   
   Если оператор не пишет `running and happy`, не начинайте обновление. Это может привести к непредсказуемому поведению кластера.

   {% endnote %}

1. Сохраните резервную копию спецификации:

   Файл необходим для возможности отката до прохождения точки невозврата:

   ```bash
   kubectl get ytsaurus <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-backup.yaml
   ```

   После того как мастер-серверы вышли из режима `read-only` и применили необратимые изменения, откат на предыдущую версию не поддерживается.

Если все проверки пройдены, можно приступать к обновлению оператора.

### 2\. Обновите оператор {#update-operator}

Оператор следует обновлять первым. Новые версии серверных компонентов могут работать некорректно со старой версией оператора.

{% note warning "Внимание" %}

Новая версия оператора может сразу запустить обновление кластера — без изменения `coreImage` с вашей стороны. Это происходит, если новая версия оператора генерирует статические конфиги иначе, чем старая: оператор обнаруживает расхождение и начинает reconciliation. Подробнее о причинах и способах остановить нежелательное обновление — в разделе [Автоматическое обновление после обновления оператора](#auto-update-operator-scenario).

{% endnote %}

Проверьте текущую версию оператора:

```bash
helm list -n <namespace>
```

{% if audience == "public" %}
Список релизов и совместимых версий смотрите на [странице релизов](../../../admin-guide/releases.md#kubernetes-operator) и в [таблице совместимости](../../../admin-guide/compatibility.md).{% endif %}

Обновите оператор:

```bash
helm upgrade ytsaurus --install oci://ghcr.io/ytsaurus/ytop-chart --version <new_version>
```

Проверьте, что оператор обновился:

```bash
kubectl get pods -n <namespace>
```

Сразу после обновления могут быть видны старый и новый pod одновременно:

```bash
NAME                                                      READY   STATUS        RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running       0          21s
ytsaurus-ytop-chart-controller-manager-7478f9b6cb-qr8wd   2/2     Terminating   0          23h
```

Через некоторое время старый pod удалится:

```bash
NAME                                                      READY   STATUS    RESTARTS   AGE
ytsaurus-ytop-chart-controller-manager-6f67fd5d5c-6bbws   2/2     Running   0          25s
```

### 3\. Запустите обновление {#change-spec}

При изменении поля `coreImage` в спецификации `Ytsaurus` оператор пересоздаёт pod'ы всех [серверных компонентов из этой спецификации](#server-components-ref), даже если образ конкретного компонента не изменился. Это гарантирует совместимость версий всех серверных компонентов.

[Компоненты с собственным релизным циклом](#additional-components-ref) — Query Tracker, YQL-агент и другие — при этом не обновляются. Как их обновить, смотрите в разделе [Обновление отдельных компонентов](#partial-update). Перед обновлением проверьте совместимость этих компонентов с целевой версией кластера{% if audience == "public" %} в [таблице совместимости](../../../admin-guide/compatibility.md).{% endif %}

Сохраните актуальную спецификацию в файл для редактирования:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-new.yaml
```

{% note warning "Внимание" %}

Сохраняйте спецификацию только с вашего работающего кластера через команду `kubectl get ytsaurus`. Не используйте спецификацию из GitHub или другого шаблона: в ней нет ваших текущих настроек — например размеченных дисков, параметров шифрования и кастомных параметров. Применение чистой спецификации может сломать кластер.

{% endnote %}

Внесите в файл `ytsaurus-spec-new.yaml` следующие изменения:

- Задайте поле `updatePlan` со списком компонентов, которые нужно обновить. Для полного обновления укажите класс `Everything` — обновятся все [серверные компоненты](#server-components-ref).

- Измените `coreImage` на следующую версию.

{% if audience == "public" %}
- Целевую версию выбирайте на [странице релизов](../../../admin-guide/releases.md#server). Откройте нужный релиз и скопируйте тег образа `ghcr.io/ytsaurus/ytsaurus`.{% endif %}

```yaml
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo # Следующая версия
  updatePlan:
    - class: Everything # Полное обновление всех серверных компонентов
```

{% note warning "Важно" %}

Обновляйтесь только на стабильную версию, следующую за текущей. Не используйте `dev`-образы из ветки `main`. После успешного обновления откат на предыдущую версию не поддерживается.

{% endnote %}

По умолчанию оператор использует стратегию `BulkUpdate` — кластер будет полностью недоступен на время обновления. Если кластер должен оставаться доступным, настройте стратегию `RollingUpdate` через поле `updatePlan` в файле `ytsaurus-spec-new.yaml` до применения изменений. Пример — в разделе [Обновление без остановки кластера через RollingUpdate](#rolling-update-scenario).

Примените изменённую спецификацию:

```bash
kubectl apply -f ytsaurus-spec-new.yaml
```

Ожидаемый вывод:

```bash
ytsaurus.cluster.ytsaurus.tech/<cluster_name> configured
```

Отслеживайте статус в поле `UPDATESTATE` ресурса `Ytsaurus`:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace>
```

Во время обновления вывод может выглядеть следующим образом:

```bash
NAME       CLUSTERSTATE   UPDATESTATE                    UPDATINGCOMPONENTS
ytsaurus   Updating       WaitingForPodsCreation         {ms hp ds dnd rp end tnd sch ca}
```

Описание этапов и состояний pod'ов — в [справочнике статусов](#statuses-reference) ниже.

Проверяйте состояние pod'ов:

```bash
kubectl get pods -n <namespace>
```

{% note info %}

Обновление может занять некоторое время, так как оператор сначала скачивает новый Docker-образ и только потом запускает обновлённые компоненты. Чтобы образы были готовы заранее и обновление проходило быстрее, настройте [предзагрузку образов](../../../admin-guide/update/update_strategy.md#image-heater).

{% endnote %}

Если обновление идёт слишком долго, проверьте события проблемного pod'а:

```bash
kubectl describe pod <pod_name> -n <namespace> | grep -A 10 "Events:"
```

Если в событиях указана ошибка загрузки образа `ImagePullBackOff`, проверьте, что тег образа указан правильно и эта версия есть в реестре. Если pod в состоянии `Pending`, проблема связана с ресурсами — проверьте их доступность в кластере.

### 4\. Проверьте результат {#verify}

Обновление завершено при выполнении следующих условий:

- кластер находится в состоянии `Running`;
- все основные pod'ы находятся в состоянии `Running`;
- в кластере нет pod'ов в состояниях `Init`, `Pending`, `ImagePullBackOff` и `CrashLoopBackOff`.

Проверьте итоговое состояние кластера:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace>
```

Ожидаемый вывод:

```bash
NAME       CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS   BLOCKEDCOMPONENTS
ytsaurus   Running
```

Проверьте состояние pod'ов:

```bash
kubectl get pods -n <namespace>
```

Пример ожидаемого результата:

```bash
NAME                                      READY   STATUS     RESTARTS   AGE
ca-0                                      1/1     Running    0          5m
dnd-0                                     1/1     Running    0          5m
dnd-1                                     1/1     Running    0          5m
dnd-2                                     1/1     Running    0          5m
ds-0                                      1/1     Running    0          10m
end-0                                     2/2     Running    0          5m
hp-0                                      1/1     Running    0          5m
ms-0                                      1/1     Running    0          10m
qt-0                                      1/1     Running    15         75d
rp-0                                      1/1     Running    0          5m
sch-0                                     1/1     Running    0          5m
tnd-0                                     1/1     Running    0          10m
```

Проверьте, что в спецификации указана новая версия:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace> -o jsonpath='{.spec.coreImage}'
```

Ожидаемый вывод:

```bash
ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo
```

Убедитесь, что оператор снова пишет в логах `Ytsaurus is running and happy`:

```bash
kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
```

После успешного обновления очистите поле `updatePlan` в файле `ytsaurus-spec-new.yaml` — оставьте его пустым:

```yaml
spec:
  updatePlan: []
```

Того же результата можно добиться явным правилом с классом `Nothing` — оператор не будет обновлять ни один компонент:

```yaml
spec:
  updatePlan:
    - class: Nothing
```

Примените изменение:

```bash
kubectl apply -f ytsaurus-spec-new.yaml
```

Оператор не очищает `updatePlan` автоматически — сбросьте его сами. Пустой `updatePlan` означает «не обновлять ничего» и защищает от случайного перезапуска компонентов при дальнейших изменениях спецификации. Если оставить `- class: Everything`, то при следующем изменении `coreImage` снова запустится полное обновление всех компонентов.

Обновление на следующую версию. Если нужно обновиться ещё на одну версию, повторите все шаги, начиная с раздела [Подготовка к обновлению](#before-update). Каждый раз сохраняйте спецификацию заново через `kubectl get ytsaurus` — оператор мог изменить её в процессе обновления, и локальный файл с прошлого раза уже не актуален.

## Решение проблем при обновлении {#troubleshooting}

Используйте этот раздел, если обновление не запускается, идёт слишком долго, завершается с ошибкой или ведёт себя непредсказуемо.

{% cut "Конфликт версий при применении спецификации" %}

При попытке применить спецификацию возникла ошибка конфликта версий:

```bash
Error from server (Conflict): error when applying patch: the object has been modified; please apply your changes to the latest version and try again
```

Это означает, что спецификацию изменил другой процесс. Сохраните актуальную версию спецификации и повторите изменение:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace> -o yaml > ytsaurus-spec-new.yaml
```

{% endcut %}

{% cut "Образ не найден" %}

Ошибка загрузки образа:

```bash
Failed to pull image "ghcr.io/ytsaurus/ytsaurus:stable-X.X.X-relwithdebinfo": not found
```

Проверьте, что:

1. тег образа указан правильно;
1. эта версия есть в реестре.


{% endcut %}

{% cut "Pod долго находится в состоянии Init" %}

Такое поведение возможно, когда Kubernetes ещё скачивает новый образ.

```bash
kubectl describe pod <pod_name> -n <namespace> | grep -A 10 "Events:"
```

Пример нормального процесса:

```bash
Events:
  Type    Reason     Age    From               Message
  ----    ------     ----   ----               -------
  Normal  Scheduled  3m10s  default-scheduler  Successfully assigned ytsaurus/ms-0 to docker-desktop
  Normal  Pulling    3m9s   kubelet            Pulling image "ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo"
```

Если pod так и остаётся в состоянии `Init` дольше ожидаемого времени, проверьте логи pod'а и события для выявления проблемы.

{% endcut %}

{% cut "Джоб находится в состоянии CrashLoopBackOff" %}

Например, вы можете увидеть `yt-scheduler-init-job-op-archive` в состоянии `CrashLoopBackOff`.

Во время обновления это может быть временным состоянием. Джоб может стартовать раньше, чем кластер полностью подготовит таблет-целлы. Из-за этого джоб уходит в цикл перезапусков, но позже обычно сходится — это нормально.

Сначала проверьте общий статус кластера:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace>
```

Если кластер продолжает обновляться, подождите завершения процесса и проверьте состояние джоба повторно. Если кластер не обновляется или джоб не сходится в течение длительного времени, проверьте логи джоба для выявления причины проблемы.

{% endcut %}

{% cut "Оператор не запускает обновление" %}

Проверьте, что:

1. в поле `updatePlan` указаны компоненты, которые нужно обновить — например `- class: Everything`. Если `updatePlan` пуст, а вы изменили `coreImage`, кластер перейдёт в состояние `UpdateBlocked` и обновление не начнётся;
2. оператор уже завершил предыдущие действия, и в логах есть `running and happy`;
3. кластер до начала обновления находился в состоянии `Running`;
4. проверьте логи оператора:

```bash
kubectl logs -n <namespace> deployment/ytsaurus-ytop-chart-controller-manager --tail=100
```

{% endcut %}

{% cut "Обновление перешло в состояние ImpossibleToStart" %}

Kubernetes-оператор может остановить обновление, если считает его небезопасным. Например, если часть таблет-целл bundle'ов находится в плохом состоянии — таблет-целлы не работают или имеют ошибки. В этом случае обновление не начнётся, пока вы не устраните проблему.

Проверьте статус ресурса:

```bash
kubectl get ytsaurus -n <namespace>
```

Пример вывода:

```bash
NAME         CLUSTERSTATE   UPDATESTATE           UPDATINGCOMPONENTS
minisaurus   Updating       ImpossibleToStart
```

Чтобы узнать причину, выполните команду:

```bash
kubectl describe ytsaurus -n <namespace>
```

И проверьте блок `Conditions` в `UpdateStatus`.

{% cut "Пример причины невозможности запуска обновления" %}

```bash
kubectl describe ytsaurus -n <namespace>
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

Если обновление нельзя запустить, верните спецификацию к предыдущему значению. После этого оператор отменит обновление и вернёт кластер в состояние `Running`.

{% endcut %}

{% cut "Нераспознаваемые опции мастера после обновления" %}

После обновления кластера в конфигах master-серверов могут остаться нераспознанные опции, которые перестали использоваться в новой версии {{product-name}}. Особенно часто это случается при мажорных обновлениях. В этом случае вы увидите предупреждение:

```bash
Found unrecognized options in dynamic cluster config
```

Удалить нераспознанные опции мастера можно двумя способами:

- **Командой CLI** — [yt admin remove-master-unrecognized-options](../../../admin-guide/cli-admin.md#remove-master-unrecognized-options). Этот способ поддерживает флаг `--dry` для предварительного просмотра изменений.
- **Standalone-скриптом** — [remove_master_unrecognized_options](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts/remove_master_unrecognized_options) из репозитория на GitHub.

{% endcut %}

## Дополнительные сценарии {#advanced-scenarios}

### Обновление отдельных компонентов {#partial-update}

{% note warning "Внимание" %}

Используйте частичное обновление только в редких случаях, например для исправления ошибки. Рекомендуем предварительно обсудить такое решение с [командой {{product-name}}](https://ytsaurus.tech/#contact).

Более новая версия одного из компонентов может требовать определённую версию мастер-серверов. Если зависимости не соблюдены, кластер может не запуститься. Для обновления кластера рекомендуется использовать обновление через поле `coreImage`. Это гарантирует совместимость версий всех серверных компонентов.

{% endnote %}

В спецификации `Ytsaurus` можно управлять образами компонентов по отдельности. То, как оператор обрабатывает такие изменения, зависит от типа компонента:

- Компоненты с собственным релизным циклом — Query Tracker, YQL-агент, Strawberry и другие. Подробнее — в разделе [Компоненты с собственным релизным циклом](#additional-components-ref).

  Для обновления этих компонентов измените поле `image` в спецификации `Ytsaurus`. Это единственный способ обновления этих компонентов. При изменении их образа оператор обновит только их pod'ы, не останавливая работу кластера.

  При полном обновлении кластера через поле `coreImage` pod'ы этих компонентов перезапустятся, но их образ останется прежним — тем, что задан в поле `image`.

  Пример: обновление Query Tracker до версии `0.0.7`:

  ```yaml
  spec:
    queryTrackers:
      image: ghcr.io/ytsaurus/query-tracker:0.0.7
  ```

  ```bash
  kubectl apply -f ytsaurus-spec-new.yaml
  ```

  Оператор обновит только pod'ы `qt`, не затрагивая остальные компоненты.

- Точечное обновление серверных компонентов

  Вы можете переопределить образ для конкретного серверного компонента — например только для `tabletNodes`, указав для него отдельный `image` вместо общего `coreImage`.

  Укажите поле `image` для нужного компонента в файле `ytsaurus-spec-new.yaml` и примените изменение:

  ```bash
  kubectl apply -f ytsaurus-spec-new.yaml
  ```

### Обновление статических конфигов {#configs}

Оператор автоматически генерирует конфигурационные файлы для каждого компонента — мастер-серверов, прокси, нод — на основе спецификации `Ytsaurus`. Иногда нужно изменить параметры, которые попадают в эти файлы, — например добавить новое место хранения `locations` или настроить специфичные параметры компонента через `configOverrides`.

В таких случаях не задавайте поле `updatePlan`. Достаточно изменить спецификацию и применить её — оператор сам обнаружит, что конфигурация изменилась, и перезапустит только pod'ы затронутых компонентов.

Внесите нужные изменения в `ytsaurus-spec-new.yaml` и примените:

```bash
kubectl apply -f ytsaurus-spec-new.yaml
```

Во время перезапуска pod'ов кластер перейдёт в состояние `Reconfiguration`:

```bash
kubectl get ytsaurus <cluster_name> -n <namespace>
```

```bash
NAME       CLUSTERSTATE      UPDATESTATE
ytsaurus   Reconfiguration
```

После того как pod'ы перезапустятся и станут готовы, кластер вернётся в `Running`.

### Автоматическое обновление после обновления оператора {#auto-update-operator-scenario}

При обновлении оператора его новая версия может автоматически запустить обновление кластера — без изменения `coreImage` с вашей стороны. Это происходит потому, что разные версии оператора могут генерировать разные конфигурационные файлы для одних и тех же компонентов — например, в новой версии может добавиться поле. Оператор замечает расхождение между текущим состоянием кластера и желаемым и запускает reconciliation.

Если обновление окажется невозможным, кластер останется в состоянии `Updating`, а `UPDATESTATE` перейдёт в `ImpossibleToStart`. В этом случае есть два варианта:

- Откатить оператор до предыдущей версии:

  ```bash
  helm rollback ytsaurus -n <namespace>
  ```

- Оставить поле `updatePlan` пустым в спецификации `Ytsaurus` — оператор отменит обновление и не будет запускать его автоматически:

  ```yaml
  spec:
    updatePlan: []
  ```

  ```bash
  kubectl apply -f ytsaurus-spec-new.yaml
  ```

  После этого приведите кластер в рабочее состояние и повторите попытку, когда будете готовы, задав в `updatePlan` нужные компоненты — например `- class: Everything`.

### Обновление без остановки кластера через RollingUpdate {#rolling-update-scenario}

Чтобы кластер оставался доступным во время обновления, настройте стратегию `RollingUpdate` через поле `updatePlan` в спецификации `Ytsaurus`. Оператор обновит pod'ы по одному, сохраняя минимальное количество доступных инстансов.

{% note warning "Требования" %}

Для корректной работы `RollingUpdate` у каждого обновляемого компонента должно быть **минимум 2 инстанса**. Если инстанс один, при его обновлении компонент будет недоступен так же, как при `BulkUpdate`. Для мастер-серверов `RollingUpdate` пока не реализован — они обновляются через `BulkUpdate` или `OnDelete`.

Подробнее о стратегии, доступности компонентов и рекомендуемом количестве инстансов — в разделе [Стратегия `RollingUpdate`](../../../admin-guide/update/update_strategy.md#configure-rolling).

{% endnote %}

{% cut "Минимальный пример: `RollingUpdate` для всех компонентов" %}

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytsaurus
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo
  updatePlan:
    - class: Everything
      strategy:
        rollingUpdate: {}
```

{% endcut %}

{% cut "Своя стратегия для каждого компонента для production" %}

```yaml
updatePlan:
  # HTTP Proxies — Rolling Update
  - component:
      type: HttpProxy
    strategy:
      rollingUpdate: {}
      runPreChecks: true

  # RPC Proxies — Rolling Update
  - component:
      type: RpcProxy
    strategy:
      rollingUpdate: {}
      runPreChecks: true

  # Data Nodes — Rolling Update
  - component:
      type: DataNode
    strategy:
      rollingUpdate: {}
      runPreChecks: true

  # Exec Nodes — Rolling Update с drain
  - component:
      type: ExecNode
    strategy:
      rollingUpdate: {}
      runPreChecks: true

  # Остальные компоненты — BulkUpdate
  - class: Everything
    concurrency: 2  # не более 2 instance groups одновременно
```

{% endcut %}

{% cut "RollingUpdate с предзагрузкой образов" %}

Флаг `enableImageHeater` скачивает новые образы на узлы до начала обновления. Вместе с `RollingUpdate` это сокращает время переключения pod'ов:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytsaurus
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-25.2.0-relwithdebinfo
  clusterFeatures:
    enableImageHeater: true
  updatePlan:
    - class: Everything
      strategy:
        rollingUpdate: {}
```

Подробнее о предзагрузке — в разделе [Предзагрузка образов](../../../admin-guide/update/update_strategy.md#image-heater).

{% endcut %}

Описание всех полей `updatePlan` — в разделе [Настройка поля updatePlan](../../../admin-guide/update/update_strategy.md#updateplan).

### Ручное вмешательство {#manual-intervention}

{% note warning "Внимание" %}

Перед ручным вмешательством рекомендуется проконсультироваться с [командой {{product-name}}](https://ytsaurus.tech/#contact).

{% endnote %}

Если автоматическое обновление не может завершиться и нужно выполнить ручные действия, временно отключите управление кластером со стороны оператора. Для этого установите в спецификации `Ytsaurus` флаг `isManaged: false`.

После этого оператор перестанет вносить изменения в кластер, и вы сможете выполнить ручные действия.

Этот сценарий может быть полезен, если оператор застрял на проблемном компоненте. Также он поможет, если нужно вручную проверить или восстановить состояние части кластера. Или если вы разбираете нестандартную ошибку, которую нельзя устранить обычным повторным применением спецификации.

## Справочник статусов {#statuses-reference}

- [Как проверить статус кластера](#get-status)
- [UPDATESTATE](#update-statuses)
- [CLUSTERSTATE](#cluster-states)
- [Состояния pod'ов](#pod-states)

#### Как проверить статус кластера {#get-status}

Следить за обновлением можно по статусу ресурса `Ytsaurus`:

```bash
kubectl get ytsaurus -n <namespace>
```

Во время обновления кластер переходит в состояние `Updating`, а подробный этап отображается в поле `UPDATESTATE`. После завершения обновления кластер возвращается в состояние `Running`:

```bash
NAME         CLUSTERSTATE   UPDATESTATE   UPDATINGCOMPONENTS
minisaurus   Running        None
```

#### UPDATESTATE {#update-statuses}

Не все статусы появляются при каждом обновлении — набор зависит от того, какие компоненты затронуты. Например, статусы `WaitingForTabletCells` появляются только при обновлении tablet-нод, `WaitingForOpArchiveUpdate` — только при обновлении планировщика.

{% cut "Таблица статусов поля UPDATESTATE" %}

#|
|| **Статус** | **Условие** | **Описание** ||
|| `None` | — | Обновление не выполняется ||
|| `WaitingForImageHeater` | Если включена предзагрузка образов | Оператор ожидает предзагрузки нового образа на узлы кластера ||
|| `PossibilityCheck` | Если обновляются мастер-серверы или tablet-ноды | Оператор проверяет, безопасно ли запускать обновление ||
|| `ImpossibleToStart` | — | Оператор не может запустить обновление. Например, таблет-целлы bundle'ов в плохом состоянии. Обновление не начнётся, пока вы не устраните проблему ||
|| `WaitingForSafeModeEnabled` | Если обновляются мастер-серверы | Оператор включает safe mode — запрещает запись в кластер на время обновления мастер-серверов ||
|| `WaitingForTabletCellsSaving` | Если обновляются tablet-ноды | Оператор сохраняет текущую топологию таблет-целлы перед их удалением ||
|| `WaitingForTabletCellsRemovingStart` | Если обновляются tablet-ноды | Оператор инициирует удаление таблет-целлы ||
|| `WaitingForTabletCellsRemoved` | Если обновляются tablet-ноды | Оператор ожидает полного удаления таблет-целлы ||
|| `WaitingForImaginaryChunksAbsence` | Если обновляются data-ноды или мастер-серверы | Оператор ожидает исчезновения «воображаемых» чанков — временных метаданных, создаваемых при перебалансировке данных ||
|| `WaitingForSnapshots` | Если обновляются мастер-серверы | Оператор ожидает, пока мастер-серверы сохранят снапшоты своего состояния ||
|| `WaitingForPodsRemoval` | Всегда | Оператор удаляет старые pod'ы обновляемых компонентов ||
|| `WaitingForPodsCreation` | Всегда | Оператор ожидает запуска новых pod'ов с обновлённым образом ||
|| `WaitingForMasterExitReadOnly` | Если обновляются мастер-серверы | Оператор ожидает, пока мастер-серверы выйдут из режима read-only после перезапуска ||
|| `WaitingForSidecarsInitializingPrepare` | Если обновляются мастер-серверы | Оператор подготавливает инициализацию sidecar-контейнеров ||
|| `WaitingForSidecarsInitialize` | Если обновляются мастер-серверы | Оператор инициализирует sidecar-контейнеры ||
|| `WaitingForCypressPatch` | Всегда | Оператор применяет изменения в дереве метаинформации Cypress после обновления компонентов ||
|| `WaitingForTabletCellsRecovery` | Если обновляются tablet-ноды | Оператор восстанавливает таблет-целлы на обновлённых нодах ||
|| `WaitingForOpArchiveUpdatingPrepare` | Если обновляется планировщик | Оператор подготавливает обновление operational archive ||
|| `WaitingForOpArchiveUpdate` | Если обновляется планировщик | Оператор обновляет operational archive — внутреннее хранилище метаданных операций ||
|| `WaitingForQTStateUpdatingPrepare` | Если обновляется query tracker | Оператор подготавливает обновление состояния query tracker ||
|| `WaitingForQTStateUpdate` | Если обновляется query tracker | Оператор обновляет состояние query tracker ||
|| `WaitingForYqlaUpdatingPrepare` | Если обновляется YQL-агент | Оператор подготавливает обновление состояния YQL-агента ||
|| `WaitingForYqlaUpdate` | Если обновляется YQL-агент | Оператор обновляет состояние YQL-агента ||
|| `WaitingForQAStateUpdatingPrepare` | Если обновляется queue agent | Оператор подготавливает обновление состояния queue agent ||
|| `WaitingForQAStateUpdate` | Если обновляется queue agent | Оператор обновляет состояние queue agent ||
|| `WaitingForSafeModeDisabled` | Если обновляются мастер-серверы | Оператор выключает safe mode после успешного обновления мастер-серверов ||
|| `WaitingForTimbertruckPrepared` | Всегда | Оператор ожидает готовности Timbertruck — компоненты для сбора структурированных логов ||
|#

{% endcut %}

#### CLUSTERSTATE {#cluster-states}

{% cut "Таблица статусов поля CLUSTERSTATE" %}

#|
|| **Статус** | **Описание** ||
|| `Created` | Кластер создан, но ещё не инициализирован ||
|| `Initializing` | Кластер инициализируется впервые ||
|| `Running` | Кластер работает нормально ||
|| `Reconfiguration` | Оператор применяет изменения конфигурации без полного обновления компонентов ||
|| `Updating` | Кластер обновляется ||
|| `UpdateBlocked` | Образ изменён, но `updatePlan` не разрешает обновление этих компонентов. Оператор ждёт, пока вы добавите нужные компоненты в `updatePlan` ||
|| `UpdateFinishing` | Обновление завершается — оператор применяет финальные изменения ||
|| `CancelUpdate` | Оператор отменяет обновление и возвращает кластер в состояние `Running` ||
|#

{% endcut %}

#### Состояния pod'ов {#pod-states}

{% cut "Таблица статусов состояния pod'ов" %}

#|
|| **Статус** | **Описание** ||
|| `Init:0/2` | Инициализация pod'а ||
|| `Pending` | Ожидание ресурсов ||
|| `ContainerCreating` | Создание контейнера ||
|| `Running` | Pod работает ||
|| `ImagePullBackOff` | Ошибка загрузки образа ||
|| `CrashLoopBackOff` | Pod падает и перезапускается ||
|#

{% endcut %}

## Справочник компонентов {#via-ytsaurus-crd}

#### Компоненты, которые обновятся вместе с кластером {#server-components-ref}

Оператор обновляет эти компоненты одновременно с кластером при изменении поля `coreImage` в спецификации `Ytsaurus`. Для каждого компонента можно переопределить образ отдельно через поле `image`.

{% cut "Таблица компонентов, которые обновятся вместе с кластером" %}

#|
|| **Компонент** | **Поле в спецификации** ||
|| Master primary, secondary | `primaryMasters`, `secondaryMasters` ||
|| Master caches | `masterCaches` ||
|| HTTP-прокси | `httpProxies` ||
|| RPC-прокси | `rpcProxies` ||
|| TCP-прокси | `tcpProxies` ||
|| Kafka-прокси | `kafkaProxies` ||
|| Cypress-прокси | `cypressProxies` ||
|| Data nodes | `dataNodes` ||
|| Exec nodes | `execNodes` ||
|| Tablet nodes | `tabletNodes` ||
|| Discovery | `discovery` ||
|| Scheduler | `schedulers` ||
|| Controller agents | `controllerAgents` ||
|| Queue agents | `queueAgents` ||
|| Tablet balancer | `tabletBalancers` ||
|| Bundle controller | `bundleController` ||
|#

{% endcut %}

#### Компоненты с собственным релизным циклом {#additional-components-ref}

Компоненты имеют собственные релизные циклы, оператор обновляет их независимо через поле `image` конкретного компонента в спецификации `Ytsaurus`.

{% cut "Таблица компонентов с собственным релизным циклом" %}

#|
|| **Компонент** | **Поле в спецификации** ||
|| Query trackers | `queryTrackers` ||
|| YQL agents | `yqlAgents` ||
|| Strawberry controller | `strawberry` ||
|#

{% endcut %}
