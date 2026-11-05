# Обновление SPYT

В этой статье описано, как обновить SPYT с помощью оператора Kubernetes.

## Что важно знать перед обновлением {#important}

При обновлении SPYT кластер {{product-name}} продолжает работать в обычном режиме, все операции с данными доступны.

Во время обновления SPYT происходят следующие изменения:

- Кластер {{product-name}} работает в штатном режиме;
- Серверные компоненты {{product-name}} не перезапускаются;
- Доступ к данным через {{product-name}} API сохранён;
- Новая версия устанавливается отдельным ресурсом рядом с текущей и не заменяет её, поэтому на кластере одновременно доступны [несколько версий SPYT](#multiple-versions).

Перед обновлением проверьте совместимость версий SPYT, Spark, Java, Scala и Python {% if audience == "public" %}в [таблице совместимости](../../../user-guide/data-processing/spyt/overview.md#spyt-compatibility).{% endif %}


{% if audience == "public" %}Доступные версии SPYT и изменения в них описаны в [релизах SPYT](../../../admin-guide/releases.md#spyt).{% endif %}

## Подготовка к обновлению {#before-update}

#### 1. Проверьте текущую версию SPYT

Проверьте текущую версию SPYT в спецификации:

```bash
kubectl get spyt -n <namespace> -o yaml | grep image
```

Пример вывода:
```yaml
image: ghcr.io/ytsaurus/spyt:2.8.0
```

#### 2. Проверьте состояние SPYT

Проверьте, что SPYT находится в состоянии `Finished`:

```bash
kubectl get spyt -n <namespace>
```

Пример вывода:
```
NAME            RELEASESTATUS
<spyt-name>     Finished
```

Если SPYT не находится в состоянии `Finished`, дождитесь завершения текущих операций.

#### 3. Сохраните резервную копию спецификации SPYT

Сохраните текущую спецификацию SPYT:

```bash
kubectl get spyt -n <namespace> -o yaml > spyt-backup.yaml
```

## Обновление SPYT {#update-process}

Оператор не обновляет уже созданный ресурс SPYT при повторном `kubectl apply`. Поэтому новую версию SPYT добавляют как отдельный ресурс с собственным именем, не трогая существующий. Обе версии сосуществуют на кластере, их артефакты хранятся в Кипарисе рядом, а старую версию можно удалить позже.

### 1. Подготовьте спецификацию новой версии SPYT

За основу удобно взять текущую спецификацию SPYT:

```bash
kubectl get spyt <spyt-name> -n <namespace> -o yaml
```

Создайте файл `spyt-<new-version>.yaml` с новым именем ресурса в поле `metadata.name` и новой версией образа в поле `image`:

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: <new-spyt-name>  # Имя, отличное от существующего ресурса
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/spyt:<new-version>
  ytsaurus:
    name: <ytsaurus-name>
```

{% cut "Пример спецификации для версии 2.9.0" %}

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Spyt
metadata:
  name: spyt-2-9-0
  namespace: <namespace>
spec:
  image: ghcr.io/ytsaurus/spyt:2.9.0
  ytsaurus:
    name: <ytsaurus-name>
```

{% endcut %}

### 2. Примените спецификацию

Примените спецификацию новой версии:

```bash
kubectl apply -f spyt-<new-version>.yaml -n <namespace>
```

Оператор создаст новый ресурс SPYT и загрузит артефакты новой версии в Кипарис. Существующий ресурс и его артефакты остаются на месте.

### 3. Следите за обновлением

Следите за состоянием SPYT во время обновления:

```bash
kubectl get spyt -n <namespace> -w
```

SPYT проходит через следующие состояния обновления:

#|
|| **Статус** | **Описание** ||
|| `CreatingUser` | Оператор создаёт пользователя в Кипарисе ||
|| `UploadingIntoCypress` | Оператор загружает файлы в Кипарис ||
|| `Finished` | Система завершила обновление ||
|#

### 4. Проверьте результат

Убедитесь, что новый ресурс SPYT создан и достиг статуса `Finished`. Существующий ресурс при этом сохраняется:

```bash
kubectl get spyt -n <namespace>
```

Пример вывода:
```
NAME            RELEASESTATUS
<spyt-name>     Finished
<new-spyt-name> Finished
```

Проверьте версию образа:

```bash
kubectl get spyt -n <namespace> -o yaml | grep image
```

Пример вывода:
```yaml
image: ghcr.io/ytsaurus/spyt:2.9.0
```

### Несколько версий SPYT на одном кластере {#multiple-versions}

Описанный выше процесс добавляет новую версию SPYT как отдельный ресурс, не затрагивая существующий. Так на одном кластере {{product-name}} сосуществуют несколько версий SPYT — это позволяет разным командам использовать разные версии одновременно.

Артефакты SPYT и Spark находятся в Кипарисе по пути `//home/spark/`. Оператор не удаляет старые артефакты, поэтому версии не мешают друг другу.

### Удаление старой версии SPYT {#remove-old-version}

Когда старая версия SPYT больше не нужна, удалите её ресурс и при необходимости, освободите место в Кипарисе.

1. Удалите ресурс SPYT:

   ```bash
   kubectl delete spyt <old-spyt-name> -n <namespace>
   ```

2. Удалите артефакты старой версии из Кипариса через веб-интерфейс {{product-name}}. Для каждой версии SPYT артефакты хранятся в двух узлах — с дистрибутивом и с конфигурацией. Откройте каталоги `//home/spark/spyt/releases` и `//home/spark/conf/releases`, найдите узлы с номером ненужной версии и удалите их из контекстного меню.


