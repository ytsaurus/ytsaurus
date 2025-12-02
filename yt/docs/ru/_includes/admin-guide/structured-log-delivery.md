Kubernetes-оператор {{product-name}}, начиная с версии 0.28.0, поддерживает поставку структурированных логов мастера, настраиваемую непосредственно в спецификации кластера.

Поставка осуществляется при помощи sidecar, в котором запущен [timbertruck](https://github.com/ytsaurus/ytsaurus/tree/main/yt/admin/timbertruck).

Для его работы необходимо выполнить следующие действия:

1. На кластере запустить HTTP Proxy и Queue Agent.

2. Включить запись нужного структурированного лога мастера:

```yaml
spec:
  primaryMasters:
    structuredLoggers:
      - name: access
        minLogLevel: info
        category: Access
        format: json
        rotationPolicy:
          maxTotalSizeToKeep: 5_000_000_000
          rotationPeriodMilliseconds: 900000
```

3. Указать локация типа `Logs`:

```yaml
spec:
  primaryMasters:
    locations:
      - locationType: Logs
        path: /yt/master-logs
    volumeMounts:
      - name: master-logs
        mountPath: /yt/master-logs
    volumeClaimTemplates:
      - metadata:
          name: master-logs
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 5Gi
```

4. Указать образ для `timbertruck`:

{% note warning %}

При применении этих изменений будет запущено полное обновление кластера с даунтаймом, как и при обычном обновлении мастеров.

{% endnote %}

```yaml
spec:
  primaryMasters:
    timbertruck:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
```

5. Применить изменения.

```
kubectl apply -f your_config.yaml -n <namespace>
```

#### Штатная работа сайдкара

- Созданы очереди под каждый из структурированных логов:

```bash
yt exists //sys/admin/logs/master-access/producer
yt exists //sys/admin/logs/master-access/queue
```

- В `producer` имеется строчка с информацией о поставке с мастеров:

```bash
yt select-rows "SELECT session_id FROM [//sys/admin/logs/master-access/producer]" --format json
```

Пример ожидаемого вывода
```json
{
  "session_id": "ms-0.masters.ytsaurus-dev.svc.cluster.local:/yt/master-logs/timbertruck/master-access/staging/2025-12-01T11:07:46_ino:46696359.access.log.json"
}
```

- Настроен экспорт очереди `//sys/admin/logs/master-access/queue` в статические таблицы::

```bash
yt get //sys/admin/logs/master-access/queue/@static_export_config
```

Ожидается настроенный экспорт в существующую ноду:
```yson
{
    "default" = {
        "export_directory" = "//sys/admin/logs/export/master-access";
        "export_ttl" = 1209600000;
        "export_period" = 1800000;
        "export_name" = "default";
    };
}
```

#### Поиск проблем

Чтобы посмотреть логи сайдкара, выполните:

```bash
kubectl -n <namespace> logs ms-0 -c timbertruck
```
