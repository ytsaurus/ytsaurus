# Обновление CHYT

В этой статье описан процесс обновления CHYT. Обновление выполняется с помощью оператора Kubernetes.

## Важная информация перед обновлением {#important}

При обновлении CHYT кластер {{product-name}} продолжает работать в обычном режиме, все операции с данными доступны.

Во время обновления CHYT происходят следующие изменения:

- Кластер {{product-name}} продолжает работать в штатном режиме;
- Серверные компоненты {{product-name}} не перезапускаются;
- Доступ к данным через {{product-name}} API сохраняется;
- CHYT-клики временно недоступны во время обновления;
- Новая версия устанавливается отдельным ресурсом рядом с текущей и не заменяет её, поэтому на кластере одновременно доступны [несколько версий CHYT](#multiple-versions);
- Версией по умолчанию для клик становится последний созданный ресурс.

Перед обновлением CHYT проверьте совместимость версий CHYT, {{product-name}} и Strawberry {% if audience == "public" %}в [таблице совместимости](../../../admin-guide/compatibility.md).{% endif %}

{% note warning "Важно" %}

Проверьте, что на кластере установлен и настроен `strawberry controller`. CHYT требует Strawberry для работы.

{% endnote %}

{% if audience == "public" %}Доступные версии CHYT и изменения в них описаны в [релизах CHYT](../../../admin-guide/releases.md#chyt.){% endif %}

## Подготовка к обновлению {#before-update}

#### 1. Проверьте текущую версию CHYT

Проверьте текущую версию CHYT в спецификации:

```bash
kubectl get chyt -n <namespace> -o yaml | grep image
```

Пример вывода:
```yaml
image: ghcr.io/ytsaurus/chyt:2.17.3
```

#### 2. Проверьте состояние CHYT

Проверьте, что CHYT находится в состоянии `Finished`:

```bash
kubectl get chyt -n <namespace>
```

Пример вывода:
```
NAME         RELEASESTATUS
<chyt-name>  Finished
```

Если CHYT не находится в состоянии `Finished`, дождитесь завершения текущих операций.

#### 3. Сохраните резервную копию спецификации CHYT

Сохраните текущую спецификацию CHYT:

```bash
kubectl get chyt -n <namespace> -o yaml > chyt-backup.yaml
```

## Обновление CHYT {#update-process}

Оператор не обновляет уже созданный ресурс CHYT. Новую версию добавляют отдельным ресурсом с собственным именем, не трогая существующий. Версией по умолчанию для клик становится последний созданный ресурс с `makeDefault: true` и после установки они автоматически перейдут на новую версию.

### 1. Подготовьте спецификацию новой версии CHYT

За основу удобно взять текущую спецификацию CHYT:

```bash
kubectl get chyt <chyt-name> -n <namespace> -o yaml
```

Создайте файл `chyt-<new-version>.yaml` с новым именем ресурса в поле `metadata.name`, новой версией образа в поле `image` и `makeDefault: true`, чтобы клики по умолчанию использовали новую версию:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: <new-chyt-name>  # Имя, отличное от существующего ресурса
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/chyt:<new-version>
  makeDefault: true
  ytsaurus:
    name: <ytsaurus-name>
```

{% cut "Пример спецификации для версии 2.18.0" %}

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: chyt-2-18-0
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/chyt:2.18.0
  makeDefault: true
  ytsaurus:
    name: <ytsaurus-name>
```

{% endcut %}

### 2. Примените спецификацию

Примените спецификацию новой версии:

```bash
kubectl apply -f chyt-<new-version>.yaml -n <namespace>
```

Оператор создаст новый ресурс CHYT и загрузит артефакты новой версии в Кипарис. Существующий ресурс и его артефакты остаются на месте.

### 3. Следите за обновлением

Следите за состоянием CHYT во время обновления:

```bash
kubectl get chyt -n <namespace> -w
```

CHYT проходит через следующие состояния обновления:

#|
|| **Статус** | **Описание** ||
|| `CreatingUser` | Оператор создаёт пользователя в Кипарисе ||
|| `UploadingIntoCypress` | Оператор загружает файлы в Кипарис ||
|| `CreatingChPublicClique` | Оператор создаёт публичную клику ||
|| `Finished` | Обновление завершено ||
|#

### 4. Проверьте результат

Убедитесь, что новый ресурс CHYT создан и достиг статуса `Finished`. Существующий ресурс при этом сохраняется:

```bash
kubectl get chyt -n <namespace>
```

Пример вывода:
```
NAME            RELEASESTATUS
<chyt-name>     Finished
<new-chyt-name> Finished
```

Проверьте версию образа:

```bash
kubectl get chyt -n <namespace> -o yaml | grep image
```

Пример вывода:
```yaml
image: ghcr.io/ytsaurus/chyt:2.18.0
```

### Несколько версий CHYT на одном кластере {#multiple-versions}

Описанный выше процесс добавляет новую версию CHYT как отдельный ресурс, не затрагивая существующий. Так на одном кластере {{product-name}} сосуществуют несколько версий CHYT — каждая со своим образом. Версию по умолчанию для клик задаёт поле `makeDefault`: если `makeDefault: true` указано у нескольких ресурсов, дефолтной становится версия из последнего созданного ресурса. Прежний ресурс при этом менять не нужно — он перестаёт быть версией по умолчанию автоматически.

### Удаление старой версии CHYT {#remove-old-version}

Когда старая версия CHYT больше не нужна, удалите её ресурс и, при необходимости, освободите место в Кипарисе.

1. Удалите ресурс CHYT:

   ```bash
   kubectl delete chyt <old-chyt-name> -n <namespace>
   ```

2. Удалите бинарные артефакты старой версии из Кипариса через веб-интерфейс {{product-name}}. Откройте каталоги `//sys/bin/ytserver-clickhouse` и `//sys/bin/clickhouse-trampoline`, найдите файлы старой версии по дате создания и удалите их из контекстного меню.


