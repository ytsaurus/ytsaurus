# Обновление Task-proxy

В этой статье описано, как обновить Task-proxy с помощью Helm.

## Что важно знать перед обновлением {#important}

При обновлении Task-proxy кластер {{product-name}} продолжает работать в обычном режиме, все операции с данными доступны.

Во время обновления Task-proxy происходят следующие изменения:

- Кластер {{product-name}} работает в штатном режиме;
- Серверные компоненты {{product-name}} не перезапускаются;
- Доступ к данным через {{product-name}} API сохранён;
- Task-proxy временно недоступен во время обновления подов.


Доступные версии Task-proxy и изменения в них описаны в [релизах Task-proxy](../../../admin-guide/releases.md#task-proxy).

## Подготовка к обновлению {#before-update}

#### 1. Проверьте текущую версию Task-proxy

Проверьте текущую версию Task-proxy:

```bash
helm list -n <namespace> | grep task-proxy
```

Пример вывода:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    1           2026-05-28 12:29:52.273705 +0300 MSK   deployed    task-proxy-chart-0.2.2
```

#### 2. Проверьте состояние Task-proxy

Проверьте, что поды Task-proxy находятся в состоянии `Running`:

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Пример вывода:
```
task-proxy-d9fcb9485-txx2k                             2/2     Running            0             49s
```

Если поды Task-proxy не находятся в состоянии `Running`, дождитесь их запуска или устраните проблемы.

#### 3. Сохраните резервную копию конфигурации Task-proxy

Сохраните текущую конфигурацию Task-proxy:

```bash
helm get values task-proxy -n <namespace> -o yaml > task-proxy-backup.yaml
```

## Обновление Task-proxy {#update-process}

Пропускать промежуточные версии можно, если в релизах нет указаний на необходимость последовательного обновления.

### 1. Получите текущую конфигурацию Task-proxy

Получите текущую конфигурацию Task-proxy:

```bash
helm get values task-proxy -n <namespace> -o yaml > task-proxy-values.yaml
```

### 2. Обновите Task-proxy до новой версии

Обновите Task-proxy до новой версии:

```bash
helm upgrade task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart \
  --version <new-version> \
  -f task-proxy-values.yaml \
  -n <namespace>
```

Пример обновления с версии 0.2.2 на 0.3.0:
```bash
helm upgrade task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart \
  --version 0.3.0 \
  -f task-proxy-values.yaml \
  -n ytsaurus
```

### 3. Следите за обновлением

Следите за состоянием подов Task-proxy во время обновления:

```bash
kubectl get pods -n <namespace> -w | grep task-proxy
```

Task-proxy проходит через следующие состояния обновления:

#|
|| Статус | Описание ||
|| `ContainerCreating` | Создаются новые поды с новой версией ||
|| `Running` | Новые поды успешно запущены ||
|| `Terminating` | Старые поды завершаются ||
|#

### 4. Проверьте результат

Убедитесь, что Task-proxy успешно обновился:

```bash
helm list -n <namespace> | grep task-proxy
```

Пример вывода:
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    2           2026-05-28 12:42:06.910572 +0300 MSK   deployed    task-proxy-chart-0.3.0
```

Проверьте состояние подов:

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Пример вывода:
```
task-proxy-6655cf875b-dmwcf                                  2/2     Running            0             4m20s
```

Проверьте логи:

```bash
kubectl logs -n <namespace> <task-proxy-pod-name> --tail=20
```

## Откат на предыдущую версию {#rollback}

Выполните откат Task-proxy на предыдущую версию без потери данных.

#### Шаги для отката

1. Проверьте историю обновлений

```bash
helm history task-proxy -n <namespace>
```

Пример вывода
```
REVISION	UPDATED                 	STATUS    	CHART           	APP VERSION	DESCRIPTION
1       	Thu May 28 12:29:52 2026	superseded	task-proxy-chart-0.2.2	           	Install complete
2       	Thu May 28 12:42:06 2026	deployed  	task-proxy-chart-0.3.0	           	Upgrade complete
```

2. Откатитесь на предыдущую версию

```bash
helm rollback task-proxy -n <namespace>
```

Пример вывода
```
Rollback was a success! Happy Helming!
```

3. Проверьте результат

```bash
helm list -n <namespace> | grep task-proxy
```

Пример вывода
```
NAME            NAMESPACE   REVISION    UPDATED                                 STATUS      CHART                   APP VERSION
task-proxy      ytsaurus    3           2026-05-28 13:11:01.812794 +0300 MSK   deployed    task-proxy-chart-0.2.2
```

4. Проверьте состояние подов

```bash
kubectl get pods -n <namespace> | grep task-proxy
```

Пример вывода
```
task-proxy-d9fcb9485-txx2k                             2/2     Running            0             2m3s
```

5. Проверьте логи

```bash
kubectl logs -n <namespace> <task-proxy-pod-name> --tail=5
```

{% note info "Примечание" %}

При откате Helm автоматически создаёт новые поды с предыдущей версией чарта, а старые поды удаляются. Task-proxy успешно запускается и выполняет свои функции после отката.

{% endnote %}
