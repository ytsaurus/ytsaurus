#include <yt/yt/flow/library/cpp/multiplexer/multiplexer_process_function.h>

#include <yt/yt/flow/library/cpp/process_function/testing/unittest.h>

#include <benchmark/benchmark.h>

namespace NYT::NFlow {
namespace {

using namespace NTesting;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int TimerCount = 10000;

class TBenchmarkMultiplexerProcessFunction
    : public TMultiplexerProcessFunction<i64>
{
public:
    using TMultiplexerProcessFunction::TMultiplexerProcessFunction;

private:
    std::optional<TKey> FetchBatch(
        const TKey& /*key*/,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& /*endOffsetInclusive*/,
        i64 /*limit*/,
        TStateAccessor<i64>& /*userState*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        ui64 offset = startOffsetExclusive
            ? startOffsetExclusive->Underlying()[0].Data.Uint64
            : 0;
        return MakeKey<ui64>(offset + 1);
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TBenchmarkMultiplexerProcessFunction,
    TEmptyProcessFunctionParameters,
    TDynamicMultiplexerParameters);

void BM_ProcessTimers(benchmark::State& state)
{
    TTestStateEnvironment environment;
    auto parameters = New<TDynamicMultiplexerParameters>();
    parameters->TimerPeriod = TDuration::Seconds(10);
    parameters->BatchSize = 1000;
    auto context = TTestRuntimeContextBuilder()
        .SetProcessingFunction<TBenchmarkMultiplexerProcessFunction>()
        .SetCurrentTimestamp(TSystemTimestamp(100))
        .SetDynamicParameters(parameters)
        .Build();
    auto harness =
        TProcessFunctionTestHarness::Create<TBenchmarkMultiplexerProcessFunction>(
        environment,
        context);

    std::vector<TInputMessageConstPtr> messages;
    std::vector<TInputTimerConstPtr> timers;
    messages.reserve(TimerCount);
    timers.reserve(TimerCount);
    auto messageSchema = New<TTableSchema>();
    for (int index = 0; index < TimerCount; ++index) {
        auto key = MakeKey<ui64>(index);
        messages.push_back(MakeTestMessage("input", key, messageSchema));
        timers.push_back(MakeTestTimer(key, TSystemTimestamp(1)));
    }
    harness.RunEpoch(std::move(messages), {}, {});

    for (auto _ : state) {
        harness.RunEpoch({}, timers, {});
    }
    state.SetItemsProcessed(state.iterations() * TimerCount);
}

BENCHMARK(BM_ProcessTimers);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
