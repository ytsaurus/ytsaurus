# Настройка локаций

Для работы кластера {{product-name}} его компонентам требуется рабочее место на дисках — для хранения персистентных, временных и отладочных данных. Пути до каталогов в файловой системе, предназначенных для работы {{product-name}}, задаются в статических конфигах компонент, генерируемых k8s-оператором. В спецификации {{product-name}} для указания данных путей используется секция Locations. Типы локаций соответствуют компонентам. Для выделения томов под локации используются стандартные понятия Kubernetes: `volumeMounts`, `volumes` (для выделения неперсистентных вольюмов) и `volumeClaimTemplates` (для выделения персистентных вольюмов).

В случае использования k8s в облаке как правило необходимо указать специфичный для конкретного облака `storageClassName` в `spec` в шаблоне заказа дисков `volumeClaimTemplates`. Например, в случае [AWS](https://docs.aws.amazon.com/eks/latest/userguide/create-storage-class.html) в качестве `storageClassName` стоит использовать `auto-ebs-sc`.

## Типы локаций { #location_types }

### MasterChangelogs, MasterSnapshots { #location_master }
 Используются для хранения данных мастеров, т.е. всех метаданных кластера. Для любых инсталляций (кроме совсем небольших) локации этих типов необходимо размещать на персистентных вольюмах. Необходимый объём вольюмов зависит от количества метаданных и нагрузки на кластер. Переполнение локаций приведёт к недоступности кластера, рекомендуется выделять место с запасом и мониторить свободное место. Типичный размер для продакшн инсталляций — сотни гигабайт.

 Для того чтобы обеспечить производительность в продакшн-инсталляциях, рекомендуется размещать `MasterChangelogs` на отдельных и быстрых (например NVME) томах — латентность записи журналов непосредственно влияет на латентность мутирующих запросов к мастеру.

 У каждого инстанса мастера может быть (и должна быть) ровно одна локация типа `MasterChangelogs` и одна локация типа `MasterSnapshots`.

### ChunkCache, ImageCache, Slots { #locations_exec_nodes }
Используются exec-нодами в процессе запуска джобов, содержащих пользовательский код (Map, Reduce, Vanilla, в том числе CHYT и SPYT). `ChunkCache` локации нужны для менеджмента и кеширования бинарных артефактов, например, исполняемых файлов или вспомогательных словарей. `ImageCeche` локации нужны для кэширования образов контейнеров в зависимости от настройки среды исполнения операций. `Slots` локации нужны для выделения временного рабочего пространства (sandbox, scratch space) при запуске пользовательских процессов. У одной exec-ноды должна быть как минимум одна `ChunkCache` локация и как минимум одна `Slots` локация. При выделении нескольких `ChunkCache` или `Slots` локаций у одной ноды, exec-нода будет стараться балансировать нагрузку по ним.

Без угрозы для надёжности данных, для `ChunkCache`, `ImageCache` и `Slots` локаций можно использовать неперсистентные тома. Типичные размеры локаций — 10-50 GB для `ChunkCache`, 5-200 GB для `Slots` и `ImageCache`.

### ChunkStore { #location_data_nodes }
Используются data-нодами для хранения чанков. Для любых инсталляций (кроме совсем небольших) локации указанных типов необходимо размещать на персистентных вольюмах. Объём данных локаций определяет суммарную ёмкость кластера. При построении multi-tiered storage (содержащего диски разного типа, например HDD и SSD) требуется указывать параметр `medium` в описании локации. По умолчанию локация будет отнесена к медиуму с именем `default`.

Минимальный размер локации для тестов — 10 GB, минимальный размер локации для рабочей инсталляции — 100 GB. У каждой data-ноды должна быть как минимум одна `ChunkStore` локация.

### Logs { #location_logs }
Используются всеми компонентами для хранения логов. Указывается опционально, если не задано — логи пишутся в `/var/log` внутри контейнера. Без угрозы для надёжности данных можно использовать неперсистентные тома, но такой выбор может усложнить отладку в случае переезда или пересоздания подов. Типичные размеры локаций для продакшн инсталляций от 50-200 GB.

У каждого инстанса может быть не более одной локации типа `Logs`.

## Примеры спецификаций { #spec_examples }

### Пример настройки томов и локаций для мастеров { #spec_example_masters }

```yaml
primaryMasters:
  # Other master parameters.

  volumeClaimTemplates:
    - metadata:
        name: master-changelogs
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 200Gi
    - metadata:
        name: master-snapshots
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 200Gi

  volumes:
    # Non-persistent volume for debug logs.
    - name: master-logs
      emptyDir:
        sizeLimit: 100Gi

  volumeMounts:
    - name: master-changelogs
      mountPath: /yt/master-changelogs
    - name: master-snapshots
      mountPath: /yt/master-snapshots
    - name: master-logs
      mountPath: /yt/master-logs

  locations:
    - locationType: MasterChangelogs
      path: /yt/master-changelogs
    - locationType: MasterSnapshots
      path: /yt/master-snapshots
    - locationType: Logs
      path: /yt/master-logs
```

### Пример настройки томов и локаций для data-нод { #spec_example_data_nodes }

```yaml
dataNodes:
  volumeClaimTemplates:
    - metadata:
        name: chunk-store-hdd
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 300Gi
    - metadata:
        name: chunk-store-ssd
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 300Gi
    - metadata:
        name: node-logs
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 300Gi

  volumeMounts:
    - name: chunk-store-hdd
      mountPath: /yt/node-chunk-store-hdd
    - name: chunk-store-ssd
      mountPath: /yt/node-chunk-store-ssd
    - name: node-logs
      mountPath: /yt/node-logs

  locations:
    - locationType: ChunkStore
      path: /yt/node-chunk-store-hdd
      medium: default
    - locationType: ChunkStore
      path: /yt/node-chunk-store-ssd
      medium: ssd_blobs
    - locationType: Logs
      path: /yt/node-logs
```

## Настройка томов для кластеров k8s без Dynamic Volume Provisioning { #volumes_without_dvp }
Для разметки дисков и создания персистентных томов на кластерах без динамической провизии вольюмов можно использовать два подхода:
 * использование вольюмов типа `hostPath`;
 * ручное создание персистентных томов с указанием [claimRef](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/preexisting-pd?authuser=0#pv_to_statefulset).

### Использование hostPath вольюмов { #host_path_volumes }
Пример конфигурации для гомогенного набора хостов с дисками, смонтированными в директории ```/yt/chunk-store-hdd-1```, ```/yt/chunk-store-hdd-2``` и ```/yt/chunk-store-ssd-1```.

```yaml
dataNodes:
  volumes:
    - name: chunk-store-hdd-1
      hostPath:
        path: /yt/chunk-store-hdd-1
    - name: chunk-store-hdd-2
      hostPath:
        path: /yt/chunk-store-hdd-2
    - name: chunk-store-ssd-1
      hostPath:
        path: /yt/chunk-store-ssd-1

  volumeMounts:
    - name: chunk-store-hdd-1
      mountPath: /yt/node-chunk-store-hdd-1
    - name: chunk-store-hdd-2
      mountPath: /yt/node-chunk-store-hdd-2
    - name: chunk-store-ssd-1
      mountPath: /yt/node-chunk-store-ssd-1

  locations:
    - locationType: ChunkStore
      # Location path can be a nested path of a volume mount.
      path: /yt/node-chunk-store-hdd-1/chunk_store
      medium: default
    - locationType: ChunkStore
      path: /yt/node-chunk-store-hdd-2/chunk_store
      medium: default
    - locationType: ChunkStore
      path: /yt/node-chunk-store-ssd-1/chunk_store
      medium: ssd_blobs
      # Place logs onto the first hdd disk, along with chunk store. Different locations may possibly share the same volume.
    - locationType: Logs
      path: /yt/node-chunk-store-hdd-1/logs
```

