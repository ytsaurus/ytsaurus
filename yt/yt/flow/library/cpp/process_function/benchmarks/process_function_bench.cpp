#include <yt/yt/flow/library/cpp/process_function/testing/process_function_benchmark.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/string/split.h>

#include <functional>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;

////////////////////////////////////////////////////////////////////////////////

constexpr int MessageCount = 10000;

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

//! Splits the "text" column into words.
class TSplitFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).Split(' ').SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(context->ConvertToMessage(wordMessage));
        }
    }
};

//! Emits one word per timer.
class TTimerFunction
    : public IProcessFunction
{
public:
    void ProcessTimer(
        const TInputTimerConstPtr& /*timer*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto word = New<TWordMessage>();
        word->Word = "timer";
        output->AddMessage(context->ConvertToMessage(word));
    }
};

//! Emits one word per visit.
class TVisitFunction
    : public IProcessFunction
{
public:
    void ProcessVisit(
        const TInputVisitConstPtr& /*visit*/,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto word = New<TWordMessage>();
        word->Word = "visit";
        output->AddMessage(context->ConvertToMessage(word));
    }
};

//! Whole-batch counterpart of TSplitFunction.
class TBatchSplitFunction
    : public IBatchProcessFunction
{
public:
    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& message : input->GetMessages()) {
            auto text = GetColumnValue<std::string>(message, "text");
            for (const auto& word : StringSplitter(text).Split(' ').SkipEmpty()) {
                auto wordMessage = New<TWordMessage>();
                wordMessage->Word = word;
                output->AddMessage(context->ConvertToMessage(wordMessage));
            }
        }
    }
};

//! Per-key counterpart of TSplitFunction.
class TKeyedSplitFunction
    : public IKeyedBatchProcessFunction
{
public:
    void ProcessKey(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        for (const auto& message : input->GetMessages()) {
            auto text = GetColumnValue<std::string>(message, "text");
            for (const auto& word : StringSplitter(text).Split(' ').SkipEmpty()) {
                auto wordMessage = New<TWordMessage>();
                wordMessage->Word = word;
                output->AddMessage(context->ConvertToMessage(wordMessage));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Runtime context with a "words" output stream.
IRuntimeContextPtr MakeWordContext()
{
    return TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
}

//! |count| identical single-column text messages.
std::vector<TInputMessageConstPtr> MakeTextMessages(int count)
{
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=text;type=string}])")));
    return MakeBenchmarkMessages("input", schema, count, [] (TMessageBuilder& builder, int /*index*/) {
        builder.Payload().Set(std::string("hello world flow benchmark"), "text");
    });
}

////////////////////////////////////////////////////////////////////////////////

// Per-method benchmark bodies. Each builds its process function and input at benchmark start (via
// the captured factory), then hands the timed loop to the framework driver. BENCHMARK_CAPTURE
// binds a concrete function and input size to each, naming the case accordingly.

void BM_ProcessMessage(benchmark::State& state, const std::function<IProcessFunctionPtr()>& makeFunction, int count)
{
    auto context = MakeWordContext();
    auto function = makeFunction();
    auto messages = MakeTextMessages(count);
    BenchmarkProcessMessages(state, function, messages, context);
}

void BM_ProcessTimer(benchmark::State& state, const std::function<IProcessFunctionPtr()>& makeFunction, int count)
{
    auto context = MakeWordContext();
    auto function = makeFunction();
    auto timers = MakeBenchmarkTimers(count);
    BenchmarkProcessTimers(state, function, timers, context);
}

void BM_ProcessVisit(benchmark::State& state, const std::function<IProcessFunctionPtr()>& makeFunction, int count)
{
    auto context = MakeWordContext();
    auto function = makeFunction();
    auto visits = MakeBenchmarkVisits("input", count);
    BenchmarkProcessVisits(state, function, visits, context);
}

void BM_Process(benchmark::State& state, const std::function<IBatchProcessFunctionPtr()>& makeFunction, int count)
{
    auto context = MakeWordContext();
    auto function = makeFunction();
    auto input = MakeBenchmarkInput(MakeTextMessages(count));
    BenchmarkProcess(state, function, input, context);
}

void BM_ProcessKey(benchmark::State& state, const std::function<IKeyedBatchProcessFunctionPtr()>& makeFunction, int count)
{
    auto context = MakeWordContext();
    auto function = makeFunction();
    auto input = MakeBenchmarkInput(MakeTextMessages(count));
    BenchmarkProcessKey(state, function, input, context);
}

////////////////////////////////////////////////////////////////////////////////

// clang-format off

BENCHMARK_CAPTURE(BM_ProcessMessage, split, [] { return New<TSplitFunction>(); }, MessageCount)->Threads(1)->Threads(4);
BENCHMARK_CAPTURE(BM_ProcessTimer, timer, [] { return New<TTimerFunction>(); }, MessageCount)->Threads(1)->Threads(4);
BENCHMARK_CAPTURE(BM_ProcessVisit, visit, [] { return New<TVisitFunction>(); }, MessageCount)->Threads(1)->Threads(4);
BENCHMARK_CAPTURE(BM_Process, batch_split, [] { return New<TBatchSplitFunction>(); }, MessageCount)->Threads(1)->Threads(4);
BENCHMARK_CAPTURE(BM_ProcessKey, keyed_split, [] { return New<TKeyedSplitFunction>(); }, MessageCount)->Threads(1)->Threads(4);

// Capturing a different input size registers another named case with no extra boilerplate.
BENCHMARK_CAPTURE(BM_ProcessMessage, split_1k, [] { return New<TSplitFunction>(); }, 1000)->Threads(1);

// clang-format on

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
