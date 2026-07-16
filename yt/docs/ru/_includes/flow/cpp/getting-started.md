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

Подробнее о конвертации сообщений см. раздел [Computation (C++)](../../../flow/cpp/computation.md), секция «TYsonMessage».

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

### 3. Реализуйте [Source](../../../flow/concepts/glossary.md#source) Computation {#implement-source}

Для чтения данных из внешних источников наследуйтесь от `TSwiftOrderedSourceComputation`. В методе `DoProcessMessage` выполните преобразование входных сообщений:

```cpp
class TTextReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(ConvertToMessage(wordMessage));
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTextReader);
```

Обратите внимание: `TSwiftOrderedSourceComputation` не материализует сами сообщения в YT, сохраняя лишь метаинформацию для восстановления. Подробнее см. [Computation (C++)](../../../flow/cpp/computation.md#tswiftorderedsourcecomputation).

### 4. Реализуйте Transform Computation {#implement-transform}

Для обработки данных со стейтом наследуйтесь от `TTransformComputation`. Для работы с внешними стейтами используйте `TSimpleExternalStateManager`:

```cpp
class TWordCounter
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        const auto wordMessage = ConvertToYsonMessage<TWordMessage>(message);
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TWordCounter);
```

Ключевые моменты:
- `TMutableStateKeyClient<TState>` — типизированный клиент внешнего стейта; параметры менеджера задаются в спеке `Computation` (см. ниже), а не в собственных `TParameters`/`TDynamicParameters`.
- `InitExternalStateClient(StateClient_, "/state")` — привязка клиента к external state manager'у с именем `"/state"`, объявленному в `external_state_managers` спеки.
- `ConvertToYsonMessage<T>` — конвертация входных сообщений в типизированную структуру.

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
    computations = {
        text_reader = {
            computation_ref = "TTextReader";
            outputs = ["words"];
            sources = {
                source = {
                    type = "TQueueSource";
                    parameters = {
                        queue_path = "//path/to/input/queue";
                    };
                };
            };
            watermark_strategy = {
                watermark_generator = {
                    out_of_orderness_bound = "10s";
                };
            };
        };
        word_counter = {
            computation_ref = "TWordCounter";
            inputs = ["words"];
            group_by_schema = [
                {name = "hash"; type = "uint64"; expression = "farm_hash(word)"};
                {name = "word"; type = "string"};
            ];
            external_state_managers = {
                "/state" = {
                    external_state_manager_class_name = "NYT::NFlow::TSimpleExternalStateManager";
                    parameters = {
                        path = "//path/to/state/table";
                    };
                };
            };
        };
    };
    streams = {};
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

Детально про релизы и управление пайплайном можно прочитать в разделе [Релизы и управление пайплайном](../../../flow/release/basic-rules.md).

## См. также

- [Computation (C++)](../../../flow/cpp/computation.md)
- [Работа со стейтами (C++)](../../../flow/cpp/state.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Timers](../../../flow/concepts/timers.md)
- [Spec & DynamicSpec](../../../flow/concepts/spec.md)
{% if audience == "internal" %}- [Logbroker WaitClickJoin (C++)](../../../yandex-specific/flow/cpp/examples/lb_wait_click_join.md){% endif %}
