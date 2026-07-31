# Worker Groups в {{product-name}} Flow

## Описание {#desc}

Worker Groups &mdash; это механизм группировки [воркеров](../../../flow/concepts/glossary.md#worker) и привязки [компьютейшенов](../../../flow/concepts/glossary.md#stream-and-computation) к определённым группам воркеров. Это позволяет:

- Изолировать выполнение различных компьютейшенов на разных наборах воркеров.
- Настраивать параметры балансировки отдельно для каждой группы.
- Оптимизировать использование ресурсов в гетерогенных кластерах.

## Настройка Worker Groups {#setup}

### Конфигурация воркеров {#worker-config}

Воркеры назначаются в группы через переменную окружения `YT_FLOW_WORKER_GROUPS`. Значение &mdash; список имён групп через запятую.

Примеры:

```bash
# Воркер принадлежит одной группе
export YT_FLOW_WORKER_GROUPS="gpu"

# Воркер принадлежит нескольким группам
export YT_FLOW_WORKER_GROUPS="cpu,memory-intensive"

# Воркер без групп (по умолчанию)
# YT_FLOW_WORKER_GROUPS не установлена или установлена пустая строка
```

{% note info %}

Если переменная не установлена или установлена пустая строка, воркер считается принадлежащим к группе по умолчанию и может выполнять компьютейшены без указанной группы.

{% endnote %}

### Конфигурация компьютейшенов {#computation-config}

Компьютейшены привязываются к группе воркеров через параметр [спеки](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) `worker_group`.

Пример спеки компьютейшена:

```yson
{
    "computations" = {
        "my_computation" = {
            "computation_class_name" = "MyComputation";
            "worker_group" = "gpu";  # Этот компьютейшен будет выполняться только на воркерах группы "gpu"
            "input_stream_ids" = ["input_stream"];
            "output_stream_ids" = ["output_stream"];
        };
    };
}
```

Правила:

- Компьютейшен выполняется только на воркерах, у которых в `YT_FLOW_WORKER_GROUPS` указана соответствующая группа.
- Если `worker_group` не указан, компьютейшен может выполняться только на воркерах, у которых в `YT_FLOW_WORKER_GROUPS` не указана группа.
- Если нет доступных воркеров для указанной группы, компьютейшен не будет выполняться (см. [Компьютейшн не выполняется](#computation-does-not-run)).

## Настройки балансера для групп {#balancer-config}

### Общие настройки балансера {#common-balancer-config}

По умолчанию все группы воркеров используют общие настройки балансера, определённые в секции `job_manager` динамической спеки [пайплайна](../../../flow/concepts/glossary.md#pipeline):

```yson
{
    "job_manager" = {
        "use_cpu_aware_balancer" = %true;
        "rebalance_delay_after_pipeline_sync" = 30000;  # 30s в миллисекундах
        "rebalance_target_deviation" = 0.05;
        # ... другие параметры балансера
    };
}
```

### Переопределение настроек для группы {#group-override}

Настройки балансера можно переопределить для конкретной группы воркеров через параметр `worker_group_override` в секции `job_manager`:

```yson
{
    "job_manager" = {
        # Общие настройки балансера
        "use_cpu_aware_balancer" = %true;
        "rebalance_delay_after_pipeline_sync" = 30000;  # 30s в миллисекундах
        "rebalance_target_deviation" = 0.05;

        # Переопределение настроек для конкретных групп
        "worker_group_override" = {
            "gpu" = {
                # Для GPU-воркеров используем более агрессивную балансировку
                "rebalance_target_deviation" = 0.02;
                "rebalance_hot_mode_coeff" = 3.0;
                "rebalance_sync_period" = 5000;  # 5s в миллисекундах
            };

            "memory-intensive" = {
                # Для memory-intensive воркеров балансируем реже
                "rebalance_delay_after_pipeline_sync" = 60000;  # 60s в миллисекундах
                "rebalance_sync_period" = 20000;  # 20s в миллисекундах
            };
        };
    };
}
```

Формат `worker_group_override`:

- Ключ: имя группы воркеров (строка).
- Значение: настройки балансера (все параметры из [`TDynamicJobBalancerSpec`](../../../flow/concepts/spec.md#jobmanager)).

Полное описание параметров см. в [документации по JobManager](../../../flow/concepts/spec.md#jobmanager).

## Примеры использования {#examples}

### Пример 1: Разделение CPU и GPU вычислений {#example-1}

```yson
# Статическая спека пайплайна
{
    "computations" = {
        "preprocessing" = {
            "computation_class_name" = "PreprocessingComputation";
            # Без worker_group &mdash; выполняется на любых воркерах
        };

        "gpu_inference" = {
            "computation_class_name" = "GPUInferenceComputation";
            "worker_group" = "gpu";
        };

        "postprocessing" = {
            "computation_class_name" = "PostprocessingComputation";
            "worker_group" = "cpu";
        };
    };
}
# Динамическая спека пайплайна
{
    "job_manager" = {
        "worker_group_override" = {
            "gpu" = {
                # GPU-воркеры дорогие, балансируем аккуратнее
                "rebalance_target_deviation" = 0.01;
                "rebalance_sync_period" = 15000;  # 15s в миллисекундах
            };
        };
    };
}
```

Пример конфигурации воркеров:

```bash
# GPU-воркеры
export YT_FLOW_WORKER_GROUPS="gpu"

# CPU-воркеры
export YT_FLOW_WORKER_GROUPS="cpu"
```

### Пример 2: Изоляция критичных компьютейшенов {#example-2}

```yson
# Статическая спека пайплайна
{
    "computations" = {
        "critical_computation" = {
            "computation_class_name" = "CriticalComputation";
            "worker_group" = "critical";
        };

        "regular_computation" = {
            "computation_class_name" = "RegularComputation";
            # Без worker_group
        };
    };
}
# Динамическая спека пайплайна
{
    "job_manager" = {
        "minimum_worker_count" = 10;

        "worker_group_override" = {
            "critical" = {
                # Для критичных компьютейшенов &mdash; стабильная балансировка
                "rebalance_delay_after_pipeline_sync" = 120000;  # 120s в миллисекундах
                "async_balancing" = %false;
            };
        };
    };
}
```

### Пример 3: Воркер в нескольких группах {#example-3}

```bash
# Воркер может обрабатывать как CPU, так и memory-intensive задачи
export YT_FLOW_WORKER_GROUPS="cpu,memory-intensive"
```

```yson
# Статическая спека пайплайна
{
    "computations" = {
        "cpu_computation" = {
            "worker_group" = "cpu";
        };

        "memory_computation" = {
            "worker_group" = "memory-intensive";
        };

        # Оба компьютейшена могут выполняться на этом воркере
    };
}
```

## Мониторинг и отладка {#monitoring}

### Проверка групп воркеров {#checking-worker-groups}

Информация о группах воркеров доступна через yt интерфейс:

```bash
# Список активных воркеров и их групп
{{yt-cli}} --proxy=<proxy> flow get-flow-view <//home/path/to/pipeline> --view-path="/state/workers"
```

Пример вывода:

```yson
{
    "[2a02:6b8:c42:cec3:7800:18:6687:0]:81" = {
        "worker_groups" = [];  # группы не назначаны, дефолтная группа
        #... прочие поля
    };
    "[2a02:6b8:c42:d6ca:7800:18:22a7:0]:81" = {
        "worker_groups" = [
            "GPU_1";  # назначена конкретная группа
        ];
        #... прочие поля
    };
    #...
}

```

### Типичные проблемы {#troubleshooting}

#### Компьютейшен не выполняется {#computation-does-not-run}

Симптомы:

- На странице графа пайплайна все его стримы показывают `0 pcs/s 0 B/S`
- У узла с компьютейшеном указано `CPU Usage –` и `RAM Usage 0 B`
- На странице пайплайна `Computations/<Имя этого компьютейшена>` в таблице есть строки, но у всех них пусто в колонке `Worker`

Возможные причины:

- Опечатка в имени группы
- Нет доступных воркеров с указанной группой

Как исправить:

1. Проверьте правильность написания имени группы в спеке и переменной окружения (они должны совпадать с учетом регистра)
2. Проверьте наличие доступных воркеров с нужной группой через [{{yt-cli}}](#checking-worker-groups)
3. Просмотрите логи контроллера на предмет ошибок балансировки
4. Просмотрите логи воркеров этой группы на предмет ошибок в работе

## См. также

- [Основные понятия YT Flow](../../../flow/concepts/glossary.md)
- [Спецификация пайплайна](../../../flow/concepts/spec.md)
- [Балансировка нагрузки](../../../flow/concepts/spec.md#jobmanager)
