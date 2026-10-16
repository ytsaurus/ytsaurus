# Обновление Odin

Эта статья описывает процесс обновления Odin. Выполните обновление с помощью Helm.

## Что важно знать перед обновлением {#important}

При обновлении Odin кластер {{product-name}} продолжает работать в обычном режиме, вы можете выполнять все операции с данными.

Во время обновления Odin происходят следующие изменения

- Кластер {{product-name}} продолжает работать в штатном режиме;
- Серверные компоненты {{product-name}} не требуют перезапуска;
- Доступ к данным через {{product-name}} API сохраняется;
- Проверки Odin могут временно быть недоступны во время обновления.

{% if audience == "public" %}Доступные версии Odin и изменения в них описаны в [релизах Odin](../../../admin-guide/releases.md#odin){% endif %}.

## Подготовка к обновлению {#before-update}

#### 1. Проверить текущую версию Odin

Проверьте текущую версию Odin в Helm

```bash
helm list -n <namespace> | grep odin
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            ytsaurus    1           2026-05-28 12:29:52.273705 +0300 MSK   deployed    odin-chart-0.0.7
```

#### 2. Проверить состояние Odin

Проверьте, что поды Odin работают корректно

```bash
kubectl get pods,svc,deploy -n <namespace> | grep -i odin
```

Пример вывода
```
pod/odin-odin-chart-686c5bfbb5-t5c89                             1/1     Running            0             49s
pod/odin-odin-chart-web-75c78c8498-dzgv2                         1/1     Running            0             49s
service/odin-odin-chart-web              ClusterIP   10.100.248.191   <none>        9002/TCP         49s
deployment.apps/odin-odin-chart          1/1     1            1           49s
deployment.apps/odin-odin-chart-web      1/1     1            1           49s
```

#### 3. Сохранить резервную копию конфигурации Odin

Сохраните текущую конфигурацию Odin

```bash
helm get values odin -n <namespace> -o yaml > odin-backup.yaml
```

## Обновление Odin {#update-process}

Выполняйте обновление до целевой версии.

### 1. Получить текущую конфигурацию Odin

Получите текущую конфигурацию Odin

```bash
helm get values odin -n <namespace> -o yaml > odin-values.yaml
```

### 2. Обновить Odin до новой версии

Обновите Odin до новой версии

```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
  --version <new-version> \
  -f odin-values.yaml \
  -n <namespace>
```

Пример обновления
```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
  --version 0.0.9 \
  -f odin-values.yaml \
  -n ytsaurus
```

### 3. Следить за обновлением

Следите за состоянием Odin во время обновления

```bash
helm status odin -n <namespace>
```

### 4. Проверить результат

Проверьте, что Odin успешно обновился

```bash
helm list -n <namespace> | grep odin
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            ytsaurus    2           2026-05-28 12:42:06.910572 +0300 MSK   deployed    odin-chart-0.0.9
```

Проверьте версию чарта
```bash
helm history odin -n <namespace>
```

Пример вывода
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	odin-chart-0.0.7	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	odin-chart-0.0.9	           	Upgrade complete
```

Проверьте состояние подов после обновления
```bash
kubectl get pods,svc,deploy -n <namespace> | grep -i odin
```

Пример вывода
```
pod/odin-odin-chart-556fcb9db7-5fs75                             1/1     Running            0             4m20s
pod/odin-odin-chart-web-66bf9849dd-pvgrk                         1/1     Running            0             4m20s
service/odin-odin-chart-web              ClusterIP   10.100.248.191   <none>        9002/TCP         15m
deployment.apps/odin-odin-chart          1/1     1            1           15m
deployment.apps/odin-odin-chart-web      1/1     1            1           15m
```

## Откат на предыдущую версию {#rollback}

Выполните откат Odin на предыдущую версию без потери данных.

#### Шаги для отката

1. Проверьте историю обновлений

```bash
helm history odin -n <namespace>
```

Пример вывода
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	odin-chart-0.0.7	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	odin-chart-0.0.9	           	Upgrade complete
```

2. Откатитесь на предыдущую версию

```bash
helm rollback odin -n <namespace>
```

Пример вывода
```
Rollback was a success! Happy Helming!
```

3. Проверьте результат

```bash
helm list -n <namespace> | grep odin
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART           APP VERSION
odin            ytsaurus    3           2026-05-28 13:11:01.812794 +0300 MSK   deployed    odin-chart-0.0.7
```

4. Проверьте состояние подов

```bash
kubectl get pods -n <namespace> | grep odin
```

Пример вывода
```
odin-odin-chart-686c5bfbb5-txx2k                             1/1     Running            0             2m3s
odin-odin-chart-web-75c78c8498-2xkz9                         1/1     Running            0             2m3s
```


{% note info "Примечание" %}

При откате Helm автоматически создаёт новые поды с предыдущей версией чарта, а старые поды удаляются. Odin успешно запускается и выполняет проверки после отката.

{% endnote %}

## Управление историей релизов {#history-management}

Helm хранит историю всех обновлений релиза, что позволяет откатываться на предыдущие версии. История релизов не занимает ресурсы в кластере и хранится в Kubernetes Secrets.

### Просмотр истории

```bash
helm history odin -n <namespace>
```

### Очистка истории

Если вы уверены, что не будете откатываться на старые версии, вы можете очистить историю релизов.

{% note warning "Внимание" %}

Очистка истории релизов необратима. После удаления истории вы не сможете откатиться на предыдущие версии.

{% endnote %}

Для очистки истории удалите и переустановите релиз

```bash
helm get values odin -n <namespace> -o yaml > odin-values.yaml
helm uninstall odin -n <namespace>
helm install odin oci://ghcr.io/ytsaurus/odin-chart \
  --version <version> \
  -f odin-values.yaml \
  -n <namespace>
```


