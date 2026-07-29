# Обновление Cron

Эта статья описывает процесс обновления Cron. Выполните обновление с помощью Helm.

## Что важно знать перед обновлением {#important}

При обновлении Cron кластер {{product-name}} продолжает работать в обычном режиме, вы можете выполнять все операции с данными.

Во время обновления Cron происходят следующие изменения

- Кластер {{product-name}} продолжает работать в штатном режиме;
- Серверные компоненты {{product-name}} не требуют перезапуска;
- Доступ к данным через {{product-name}} API сохраняется;
- Cronjobs могут временно быть недоступны во время обновления.

{% if audience == "public" %}Доступные версии Cron и изменения в них описаны в [релизах Cron](../../../admin-guide/releases.md#cron).{% endif %}

## Подготовка к обновлению {#before-update}

#### 1. Проверить текущую версию Cron

Проверьте текущую версию Cron в Helm

```bash
helm list -n <namespace> | grep cron
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
ytsaurus-cron   ytsaurus    1           2026-05-27 13:23:50.855024 +0300 MSK   deployed    cron-chart-0.0.2
```

#### 2. Проверить состояние Cron

Проверьте, что Cronjobs работают корректно

```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

Пример вывода
```
NAME                                                   SCHEDULE       SUSPEND   ACTIVE   LAST SCHEDULE   AGE
ytsaurus-cron-cron-chart-clear-tmp-files               */15 * * * *   True      0        <none>          13s
ytsaurus-cron-cron-chart-clear-tmp-location            */15 * * * *   False     0        <none>          13s
ytsaurus-cron-cron-chart-clear-tmp-trash               */15 * * * *   False     0        <none>          13s
ytsaurus-cron-cron-chart-prune-offline-cluster-nodes   */15 * * * *   True      0        <none>          13s
```

#### 3. Сохранить резервную копию конфигурации Cron

Сохраните текущую конфигурацию Cron

```bash
helm get values ytsaurus-cron -n <namespace> -o yaml > cron-backup.yaml
```

## Обновление Cron {#update-process}

Выполняйте обновление последовательно через все версии. Для каждой версии выполните следующие шаги.

### 1. Получить текущую конфигурацию Cron

Получите текущую конфигурацию Cron

```bash
helm get values ytsaurus-cron -n <namespace> -o yaml > cron-values.yaml
```

### 2. Обновить Cron до новой версии

Обновите Cron до новой версии

```bash
helm upgrade ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version <new-version> \
  -f cron-values.yaml \
  -n <namespace>
```

Пример обновления
```bash
helm upgrade ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version 0.0.4 \
  -f cron-values.yaml \
  -n ytsaurus
```

### 3. Следить за обновлением

Следите за состоянием Cron во время обновления

```bash
helm status ytsaurus-cron -n <namespace>
```

### 4. Проверить результат

Проверьте, что Cron успешно обновился

```bash
helm list -n <namespace> | grep cron
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
ytsaurus-cron   ytsaurus    2           2026-05-27 13:24:38.855024 +0300 MSK   deployed    cron-chart-0.0.4
```

Проверьте версию чарта
```bash
helm history ytsaurus-cron -n <namespace>
```

Пример вывода
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Wed May 27 13:23:50 2026	superseded	cron-chart-0.0.2	           	Install complete
2       	Wed May 27 13:24:38 2026	deployed  	cron-chart-0.0.4	           	Upgrade complete
```

Проверьте наличие новых cronjobs
```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

Пример вывода
```
NAME                                                   SCHEDULE       SUSPEND   ACTIVE   LAST SCHEDULE   AGE
ytsaurus-cron-cron-chart-clear-tmp-files               */15 * * * *   True      0        <none>          94s
ytsaurus-cron-cron-chart-clear-tmp-location            */15 * * * *   False     0        <none>          94s
ytsaurus-cron-cron-chart-clear-tmp-trash               */15 * * * *   False     0        <none>          94s
ytsaurus-cron-cron-chart-process-master-snapshot       0 * * * *      True      0        <none>          45s
ytsaurus-cron-cron-chart-prune-offline-cluster-nodes   */15 * * * *   True      0        <none>          94s
```


## Откат на предыдущую версию {#rollback}

Выполните откат Cron на предыдущую версию без потери данных.

#### Шаги для отката

1. Проверьте историю обновлений

```bash
helm history ytsaurus-cron -n <namespace>
```

Пример вывода
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Wed May 27 13:23:50 2026	superseded	cron-chart-0.0.2	           	Install complete
2       	Wed May 27 13:24:38 2026	deployed  	cron-chart-0.0.4	           	Upgrade complete
```

2. Откатитесь на предыдущую версию

```bash
helm rollback ytsaurus-cron -n <namespace>
```

3. Проверьте результат

```bash
helm list -n <namespace> | grep cron
```

## Управление историей релизов {#history-management}

Helm хранит историю всех обновлений релиза, что позволяет откатываться на предыдущие версии. История релизов не занимает ресурсы в кластере и хранится в Kubernetes Secrets.

### Просмотр истории

```bash
helm history ytsaurus-cron -n <namespace>
```

### Очистка истории

Если вы уверены, что не будете откатываться на старые версии, вы можете очистить историю релизов.

{% note warning "Внимание" %}

Очистка истории релизов необратима. После удаления истории вы не сможете откатиться на предыдущие версии.

{% endnote %}

Для очистки истории удалите и переустановите релиз

```bash
helm get values ytsaurus-cron -n <namespace> -o yaml > cron-values.yaml
helm uninstall ytsaurus-cron -n <namespace>
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version <version> \
  -f cron-values.yaml \
  -n <namespace>
```
