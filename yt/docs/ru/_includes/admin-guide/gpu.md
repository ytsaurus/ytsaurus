# Поддержка Nvidia GPU

Для добавления в кластер {{product-name}} хостов с GPU и их дальнейшего использования необходимо правильно сконфигурировать вычислительные ноды.

{% note info "Предупреждение" %}

Поддержка GPU появилась начиная с {{product-name}} версии 25.2 и оператора версии 0.27.0.

{% endnote %}

## Настройка k8s-кластера

Необходимо сделать два шага:

1. Установить драйвера Nvidia на ноды k8s-кластера.

В итоге драйвер должен быть примонтирован в exec-ноды {{product-name}}; то есть exec-нода должна видеть nvidia устройства в `/dev`.

2. Установите [GPU Operator](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/getting-started.html) от Nvidia.

Убедитесь, что оператор успешно установлен и на GPU-нодах k8s-кластера доступен ресурс `nvidia.com/gpu`.

## Спецификация YTsaurus

Добавьте в спецификацию в раздел `execNodes` группу нод с поддежкой GPU.

У данной группы необходимо указать специальный `entrypointWrapper`, включить nvidia runtime и указать `nvidia.com/gpu` ресурс в запросе ресурсов.

Пример конфига для группы из одной ноды с одной GPU:
```yaml
execNodes:
- instanceCount: 1
  tags:
    - gpu
  jobEnvironment:
    cri:
      entrypointWrapper:
      - tini
      - --
      - /usr/bin/gpuagent_runner.sh
    runtime:
      nvidia: {}
  privileged: true
  jobResources:
    requests:
      limits:
        cpu: 16
        memory: 128Gi
        nvidia.com/gpu: 1
      requests:
        cpu: 16
        nvidia.com/gpu: 1
```

После применения спецификации {{product-name}}, убедитесь, что нода успешно поднялась и что на ней доступны GPU. Доступность GPU можно проверить либо в UI кластера, посмотрев на ресурсы ноды, либо с помощью CLI:

```bash
yt get //sys/exec_nodes/<node_address>/@resource_limits/gpu
```

## Настройка дерева пулов

Для эффективной работы планировщика крайне желательно, чтобы ресурсы на exec-нодах кластера и в джобах операций были в достаточной степени гомогенны. Поэтому ноды с GPU следует выделять в отдельное дерево пулов.

Для этого выполните следующую команду:
```bash
yt create scheduler_pool_tree --attributes '{name=gpu; config={nodes_filter=gpu; main_resource=gpu;}}'
```

Теперь можно запустить тестовую операцию и проверить, что в джобе доступен `nvidia-smi`:
```bash
yt vanilla --tasks '{task={job_count=1; command="nvidia-smi 1>&2"; gpu_limit=1;};}' --spec '{pool_trees=[gpu];}'
```
