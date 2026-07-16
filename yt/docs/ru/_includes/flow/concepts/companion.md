# Companion в {{product-name}} Flow

Во Flow имеется возможность запуска пользовательского кода в отдельном процессе. Такой процесс называется процессом-компаньоном.

## Области применения компаньонов
### Используется сейчас

- Поддержка вычислений на языках, отличных от [C++](../../../flow/cpp/getting-started.md), таких как [Python](../../../flow/python/getting-started.md) и [Java и Kotlin](../../../flow/java/getting-started.md).

### Планы

- Изоляция пользовательского C++ кода от ядра Flow для лучшей обработки ошибок, возможности компилировать с разными флагами (например, для CUDA), и т.п.
- Горячее обновление пользовательского кода без остановки [пайплайна](../../../flow/concepts/glossary.md#pipeline).

## Схема работы {#schema}

При использовании компаньона [Computation](../../../flow/concepts/glossary.md#stream-and-computation) состоит из двух частей: специализированного Computation-a на стороне [Worker](../../../flow/concepts/glossary.md#worker)-a и облегчённого Computation-a на стороне компаньона.

{% note info %}

Вся бизнес-логика разрабатывается на выбранном языке программирования на стороне компаньона, а форма пайплайна по-прежнему конфигурируется через [спеку](../../../flow/concepts/spec.md). В этой схеме работы воркер становится инфраструктурным бинарником, который не зависит от логики пайплайна, то есть при использовании Python, Java или Kotlin пользовательский код на C++ писать **не нужно**.

{% endnote %}

Computation на стороне Worker-a собирает батч сообщений, обогащает его всей необходимой для обработки информацией (стейты, параметры, значения watermarks и т.п.) и отправляет его компаньону по gRPC локально в рамках одного хоста.

В дальнейшем планируется использовать и unix sockets.

![](../../../flow/images/companion_v1.svg)

Управление процессом-компаньоном осуществляется через [ресурс](../../../flow/concepts/glossary.md#resource) `CompanionManager`.

### Конфигурация

Пример объявления ресурса в статической спеке для Java:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
    "parameters" = {
        "timeout" = "10s";
        "jdk_bin_path" = "/app/ytflow/jdk/bin/java";
        "main_class" = "tech.ytsaurus.flow.examples.wordcount.NodeCompanionMain";
        "classpath" = "/app/ytflow/lib/*";
        "run_process" = %true;
    };
    "dependencies" = {};
};
```

Подробное описание всех параметров `TCompanionManagerParameters` см. в разделе [Конфигурация ресурса CompanionManager](../../../flow/java/computation.md#companion-manager).

Конфигурация Python-компаньона описана в разделе [Конфигурация ресурса CompanionManager (Python)](../../../flow/python/computation.md#companion-manager).

Пример объявления Computation в статической спеке:
```yson
"computations" = {
    "mapper" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(word)"; "type" = "uint64"; required = %true;};
            {"name" = "word"; "type" = "string";};
        ];
        "input_stream_ids" = ["words"];
        "output_stream_ids" = [];
        "required_resource_ids" = {
            "CompanionManager" = {
                "controller" = false;
                "worker" = true;
            };
        };
        "parameters" = {
            "internal_states" = ["word-state"];
        };
    };
};
```

Ключевое в данном примере – использование ресурса `CompanionManager` для запуска процесса-компаньона и специализированный класс Computation-a `NYT::NFlow::NCompanion::TTransformCompanionComputation`.

### Виды Computation-ов для работы с компаньонами

- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation`: Реализация [TSwiftMapComputation](../../../flow/concepts/computation.md#tswiftmapcomputation) делегирующая обработку данных процессу-компаньону.
- `NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation`: Реализация [TSwiftOrderedSourceComputation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation) делегирующая обработку данных процессу-компаньону.
- `NYT::NFlow::NCompanion::TTransformCompanionComputation`: Реализация [TTransformComputation](../../../flow/concepts/computation.md#ttransformcomputation) делегирующая обработку данных процессу-компаньону.

Подробнее о реализации пайплайнов с использованием компаньонов: [Java и Kotlin](../../../flow/java/getting-started.md), [Python](../../../flow/python/getting-started.md).

## См. также

- [Computation](../../../flow/concepts/computation.md)
- [Быстрый старт (Java)](../../../flow/java/getting-started.md)
- [Быстрый старт (Python)](../../../flow/python/getting-started.md)
