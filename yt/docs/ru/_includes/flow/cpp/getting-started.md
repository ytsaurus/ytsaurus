# Быстрый старт в {{product-name}} Flow (C++)

В этом разделе пошагово описано, как реализовать свой первый [пайплайн](../../../flow/concepts/glossary.md#pipeline) на C++ с использованием Flow. В качестве примера рассматривается задача подсчёта слов (word count): чтение текстовых сообщений из очереди, разбиение на слова и подсчёт вхождений каждого слова.

## Предварительные требования

- Чекаут [репозитория]({{source-root}}).
- Настроенный `ya make` (сборочная система).
- Ознакомление с [основными понятиями](../../../flow/concepts/glossary.md) Flow.

## Пошаговое руководство

### 1. Определите типы сообщений {#define-messages}

Для типизированной работы с сообщениями используйте `TYsonMessage` — специальный наследник `NYTree::TYsonStruct`. Каждый тип сообщения нужно зарегистрировать в глобальном реестре с помощью макроса `YT_FLOW_DEFINE_YSON_MESSAGE`.

```cpp
#include <yt/yt/flow/library/cpp/common/registry.h>

struct TWordMessage
    : public TYsonMessage
{
    std::string Word;

    REGISTER_YSON_STRUCT(TWordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("word", &TThis::Word)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);
```

Подробнее о конвертации сообщений см. раздел [Process function (C++)](../../../flow/cpp/process-functions.md).

### 2. Определите стейт {#define-state}

Если [компьютейшен](../../../flow/concepts/glossary.md#stream-and-computation) работает со [стейтом](../../../flow/concepts/glossary.md#state), определите класс-наследник от `TStateBase`:

```cpp
struct TWordCountState
    : public TStateBase
{
    i64 Count{};

    REGISTER_YSON_STRUCT(TWordCountState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("count", &TThis::Count)
            .Default(0);
    }
};
```

Подробнее о работе со стейтами см. [Работа со стейтами (C++)](../../../flow/cpp/state.md).

### 3. Реализуйте process function для [Source](../../../flow/concepts/glossary.md#source) {#implement-source}

Пользовательскую логику на C++ реализуйте только как [process function](../../../flow/cpp/process-functions.md). Для поэлементной обработки унаследуйтесь от `IProcessFunction` и реализуйте `ProcessMessage`:

```cpp
class TTextReadFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(context->ConvertToMessage(wordMessage));
        }
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction);
```

В спеке эту функцию исполняет встроенный `TProcessFunctionSourceComputation`. Он задаёт source-режим: выходные сообщения не материализуются в YT, сохраняется только метаинформация для восстановления. Подробнее см. [Process function](../../../flow/cpp/process-functions.md#how-it-works).

### 4. Реализуйте process function со стейтом {#implement-transform}

Для обработки данных со стейтом также используйте `IProcessFunction`. Для работы с внешними стейтами используйте `TSimpleExternalStateManager`:

```cpp
class TWordCountFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction);
```

Ключевые моменты:
- `TMutableStateKeyClient<TState>` — типизированный клиент внешнего стейта; параметры менеджера задаются в спеке `Computation` (см. ниже), а не в собственных `TParameters`/`TDynamicParameters`.
- `InitExternalStateClient(StateClient_, "/state")` — привязка клиента к external state manager'у с именем `"/state"`, объявленному в `external_state_managers` спеки.
- В спеке функцию исполняет встроенный `TProcessFunctionComputation`, который задаёт transform-режим и exactly-once коммит стейта.

### 5. Напишите main.cpp {#write-main}

Функция `main` связывает все компоненты вместе:

```cpp
#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TWordMessage>("words");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
```

Здесь:
- `Initialize(argc, argv)` — инициализация Flow runtime.
- `TSimpleSpecBuilder` — билдер, в котором регистрируются все типизированные потоки. Он автоматически выводит схемы из зарегистрированных `TYsonMessage`.
- `RegisterStream<TWordMessage>("words")` — регистрация потока `words` с типом сообщений `TWordMessage`.
- `TSimpleRunnerProgram` — стандартный runner, который берёт на себя запуск и управление компьютейшенами.

Полный исходный код примера:

{% code '/yt/yt/flow/examples/cpp/word_count/main.cpp' lang='cpp' %}

### 6. Опишите спеку пайплайна {#write-spec}

Спека описывает топологию пайплайна в формате YSON. Пример для word count:

```yson
{
    "spec" = {
        "computations" = {
            "reader" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionSourceComputation";
                "processing_function" = "NYT::NFlow::NExample::TTextReadFunction";
                "output_stream_ids" = ["words"];
                "source_streams" = {
                    "queue" = {
                        "source_class_name" = "NYT::NFlow::TQueueSource";
                        "parameters" = {
                            "queue_path" = "<cluster=cluster_name>//path/to/queue";
                            "consumer_path" = "<cluster=cluster_name>//path/to/consumer";
                        };
                    };
                };
            };
            "counter" = {
                "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
                "processing_function" = "NYT::NFlow::NExample::TWordCountFunction";
                "input_stream_ids" = ["words"];
                "output_stream_ids" = [];
                "group_by_schema" = [
                    {"name" = "hash"; "type" = "uint64"; "expression" = "farm_hash(word)";};
                    {"name" = "word"; "type" = "string";};
                ];
                "external_state_managers" = {
                    "/state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = {
                            "path" = "//path/to/state/table";
                        };
                    };
                };
            };
        };
    };
}
```

Заполнять секцию `streams` при использовании `TYsonMessage` и `TSimpleSpecBuilder` не требуется — информация о потоках будет выведена автоматически.

Подробнее о формате спеки см. [Spec & DynamicSpec](../../../flow/concepts/spec.md).

### 7. Соберите проект {#build}

Добавьте зависимости в `ya.make` вашего проекта и соберите:

```bash
ya make path/to/your/project
```

### 8. Создайте объекты в YT {#create-yt-objects}

Перед запуском необходимо создать:
- Входную очередь (если она ещё не существует).
- Таблицу стейтов (для `ExternalState`).
- Объект пайплайна с [внутренними таблицами Flow](../../../flow/concepts/glossary.md#inner-pipeline-tables).

{% if audience == "internal" %}Для создания объектов используйте утилиту [YtSync]({{yt-sync-docs}}/) (спецификация пайплайна описана [здесь]({{yt-sync-docs}}/pipeline_specification)).{% endif %}

### 9. Запустите и протестируйте {#run-and-test}

Запустите пайплайн и следите за его работой через UI {{product-name}} по пути вашего `pipeline`.

Детально про релизы и управление пайплайном можно прочитать в разделе [Релизы и управление пайплайном](../../../flow/devops/vanilla/releases.md#release-and-configure-basic-rules).

## См. также

- [Process function (C++)](../../../flow/cpp/process-functions.md)
- [Режимы Computation (C++)](../../../flow/cpp/computation.md)
- [Работа со стейтами (C++)](../../../flow/cpp/state.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)
- [Spec & DynamicSpec](../../../flow/concepts/spec.md)
{% if audience == "internal" %}- [Logbroker WaitClickJoin (C++)](../../../flow/cpp/examples/lb_wait_click_join.md){% endif %}
