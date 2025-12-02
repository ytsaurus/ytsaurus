## Загрузка снапшотов в Кипарис { #uploading-snapshots-to-cypress }

Для работы некоторых частей системы необходимы снапшоты мастера.

Для этого в k8s-операторе {{product-name}}, начиная с версии 0.28.0, поддержана загрузка снапшотов мастера в Кипарис. Для этого используется специальный сайдкар-контейнер — [Hydra Persistence Uploader](https://github.com/ytsaurus/ytsaurus/tree/main/yt/docker/sidecars/hydra_persistence_uploader).

{% note warning %}

Сайдкар будет загружать в Кипарис следующие файлы, которые будут потреблять дисковое пространство кластера:

- Бинарник мастера (с учетом репликации минимум ~15 ГБ)
- Снапшоты мастера

Убедитесь, что в аккаунте `sys` достаточно свободного места для хранения этих данных.

{% endnote %}

В спецификации кластера в разделе `primaryMasters` необходимо указать образ для `hydraPersistenceUploader`:

{% note info %}

При применении этих изменений будет запущено полное обновление кластера с даунтаймом, как и при обычном обновлении мастеров.

{% endnote %}

```yaml
spec:
  primaryMasters:
    hydraPersistenceUploader:
      image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
```

### Штатная работа сайдкара

- Статус контейнера с названием `hydra-persistence-uploader` соответствует `Running`.
  
```
kubectl -n <namespace> get pod ms-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="hydra-persistence-uploader")].state}'
```

- Бинарник мастера загружен в `//sys/admin/snapshots/meta`:

```
yt list //sys/admin/snapshots/meta | grep master_binary
```

- Снапшоты и changelogs мастера загружены в `//sys/admin/snapshots/<cell_id>/snapshots`:

```
yt list //sys/admin/snapshots/1/snapshots
yt list //sys/admin/snapshots/1/changelogs
```

### Поиск проблем

Чтобы посмотреть логи сайдкара, выполните:

```
kubectl -n <namespace> logs ms-0 -c hydra-persistence-uploader
```
