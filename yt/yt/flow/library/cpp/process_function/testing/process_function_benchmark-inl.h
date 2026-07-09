#pragma once

#ifndef PROCESS_FUNCTION_BENCHMARK_INL_H_
    #error "Direct inclusion of this file is not allowed, include process_function_benchmark.h"
    // For the sake of sane code completion.
    #include "process_function_benchmark.h"
#endif

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Shared benchmark loop: runs |body| once per iteration against a fresh output collector and
//! reports |itemCount| processed items per iteration.
template <class TBody>
void RunBenchmark(benchmark::State& state, ssize_t itemCount, const TBody& body)
{
    ssize_t total = 0;
    for (auto _ : state) {
        auto output = New<TRecordingOutputCollector>();
        body(output);
        benchmark::DoNotOptimize(output);
        total += itemCount;
    }
    state.SetItemsProcessed(total);
}

inline ssize_t InputItemCount(const IInputContextPtr& input)
{
    return std::ssize(input->GetMessages()) + std::ssize(input->GetTimers()) + std::ssize(input->GetVisits());
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline std::vector<TInputMessageConstPtr> MakeBenchmarkMessages(
    const TStreamId& streamId,
    const NTableClient::TTableSchemaPtr& schema,
    int count,
    const std::function<void(TMessageBuilder& builder, int index)>& init)
{
    std::vector<TInputMessageConstPtr> messages;
    messages.reserve(count);
    for (int index = 0; index < count; ++index) {
        TMessageBuilder::TInitFunction perMessageInit;
        if (init) {
            perMessageInit = [&init, index] (TMessageBuilder& builder) {
                init(builder, index);
            };
        }
        messages.push_back(MakeTestMessage(streamId, MakeKey<ui64>(index), schema, perMessageInit));
    }
    return messages;
}

inline std::vector<TInputTimerConstPtr> MakeBenchmarkTimers(int count)
{
    std::vector<TInputTimerConstPtr> timers;
    timers.reserve(count);
    for (int index = 0; index < count; ++index) {
        timers.push_back(MakeTestTimer(MakeKey<ui64>(index), TSystemTimestamp(index + 1)));
    }
    return timers;
}

inline std::vector<TInputVisitConstPtr> MakeBenchmarkVisits(const TStreamId& streamId, int count)
{
    std::vector<TInputVisitConstPtr> visits;
    visits.reserve(count);
    for (int index = 0; index < count; ++index) {
        visits.push_back(MakeTestVisit(MakeKey<ui64>(index), streamId));
    }
    return visits;
}

inline IInputContextPtr MakeBenchmarkInput(
    std::vector<TInputMessageConstPtr> messages,
    std::vector<TInputTimerConstPtr> timers,
    std::vector<TInputVisitConstPtr> visits)
{
    return New<TInputContext>(std::move(messages), std::move(timers), std::move(visits));
}

////////////////////////////////////////////////////////////////////////////////

inline void BenchmarkProcessMessages(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputMessageConstPtr>& messages,
    const IRuntimeContextPtr& context)
{
    NDetail::RunBenchmark(state, std::ssize(messages), [&] (const IOutputCollectorPtr& output) {
        for (const auto& message : messages) {
            function->ProcessMessage(message, output, context);
        }
    });
}

inline void BenchmarkProcessTimers(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputTimerConstPtr>& timers,
    const IRuntimeContextPtr& context)
{
    NDetail::RunBenchmark(state, std::ssize(timers), [&] (const IOutputCollectorPtr& output) {
        for (const auto& timer : timers) {
            function->ProcessTimer(timer, output, context);
        }
    });
}

inline void BenchmarkProcessVisits(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputVisitConstPtr>& visits,
    const IRuntimeContextPtr& context)
{
    NDetail::RunBenchmark(state, std::ssize(visits), [&] (const IOutputCollectorPtr& output) {
        for (const auto& visit : visits) {
            function->ProcessVisit(visit, output, context);
        }
    });
}

inline void BenchmarkProcess(
    benchmark::State& state,
    const IBatchProcessFunctionPtr& function,
    const IInputContextPtr& input,
    const IRuntimeContextPtr& context)
{
    NDetail::RunBenchmark(state, NDetail::InputItemCount(input), [&] (const IOutputCollectorPtr& output) {
        function->Process(input, output, context);
    });
}

inline void BenchmarkProcessKey(
    benchmark::State& state,
    const IKeyedBatchProcessFunctionPtr& function,
    const IInputContextPtr& input,
    const IRuntimeContextPtr& context)
{
    NDetail::RunBenchmark(state, NDetail::InputItemCount(input), [&] (const IOutputCollectorPtr& output) {
        function->ProcessKey(input, output, context);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
