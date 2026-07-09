#pragma once

// Header-only benchmark helpers for process functions; include only from G_BENCHMARK modules.

#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/client/table_client/public.h>

#include <functional>
#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////
// Input builders.

//! Builds |count| index-keyed input messages on |streamId|; |init| fills each payload.
std::vector<TInputMessageConstPtr> MakeBenchmarkMessages(
    const TStreamId& streamId,
    const NTableClient::TTableSchemaPtr& schema,
    int count,
    const std::function<void(TMessageBuilder& builder, int index)>& init = {});

//! Builds |count| index-keyed input timers.
std::vector<TInputTimerConstPtr> MakeBenchmarkTimers(int count);

//! Builds |count| index-keyed input visits on |streamId|.
std::vector<TInputVisitConstPtr> MakeBenchmarkVisits(const TStreamId& streamId, int count);

//! Bundles messages, timers and visits into one input context.
IInputContextPtr MakeBenchmarkInput(
    std::vector<TInputMessageConstPtr> messages,
    std::vector<TInputTimerConstPtr> timers = {},
    std::vector<TInputVisitConstPtr> visits = {});

////////////////////////////////////////////////////////////////////////////////
// Per-method drivers. Each runs one process-function method over its input once per benchmark
// iteration, feeds a fresh output collector, and reports the processed item count.

//! Runs ProcessMessage over |messages|.
void BenchmarkProcessMessages(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputMessageConstPtr>& messages,
    const IRuntimeContextPtr& context);

//! Runs ProcessTimer over |timers|.
void BenchmarkProcessTimers(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputTimerConstPtr>& timers,
    const IRuntimeContextPtr& context);

//! Runs ProcessVisit over |visits|.
void BenchmarkProcessVisits(
    benchmark::State& state,
    const IProcessFunctionPtr& function,
    const std::vector<TInputVisitConstPtr>& visits,
    const IRuntimeContextPtr& context);

//! Runs the whole-batch Process over |input|.
void BenchmarkProcess(
    benchmark::State& state,
    const IBatchProcessFunctionPtr& function,
    const IInputContextPtr& input,
    const IRuntimeContextPtr& context);

//! Runs the per-key ProcessKey over |input|.
void BenchmarkProcessKey(
    benchmark::State& state,
    const IKeyedBatchProcessFunctionPtr& function,
    const IInputContextPtr& input,
    const IRuntimeContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting

#define PROCESS_FUNCTION_BENCHMARK_INL_H_
#include "process_function_benchmark-inl.h"
#undef PROCESS_FUNCTION_BENCHMARK_INL_H_
