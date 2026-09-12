#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/computation/computation_tracer.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NFlow {
namespace {

using namespace std::literals::chrono_literals;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TEST(TComputationTracerTest, ExplicitKindsAcrossEpochs)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TTraceContextGuard waiting(tracer->CreateEpochPartTraceContext("PreviousWait", EEpochPartKind::Waiting));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(2));
        {
            TTraceContextGuard outer(tracer->CreateEpochPartTraceContext("AnyName", EEpochPartKind::Waiting));
            {
                TTraceContextGuard inner(tracer->CreateEpochPartTraceContext("Input.Fetch", EEpochPartKind::Processing));
                TDelayedExecutor::WaitForDuration(20ms);
            }
            TDelayedExecutor::WaitForDuration(20ms);
        }
    }
    auto timing = tracer->GetPartStatesByKind();
    auto live = tracer->GetPartStates();
    EXPECT_EQ(timing[EEpochPartKind::Waiting].TotalDuration, live.at("AnyName").TotalDuration + live.at("PreviousWait").TotalDuration);
    EXPECT_EQ(timing[EEpochPartKind::Processing].TotalDuration,
        live.at("Input.Fetch").TotalDuration + live.at("Unknown").TotalDuration);
    EXPECT_GT(live.at("PreviousWait").TotalDuration, TDuration::Zero());
    EXPECT_GT(timing[EEpochPartKind::Processing].TotalDuration, TDuration::Zero());
}

TEST(TComputationTracerTest, ReadingTotalsDoesNotConsumeOrResetThem)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        TTraceContextGuard init(tracer->CreateInitTraceContext());
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto init = tracer->GetPartStatesByKind();
    EXPECT_EQ(init[EEpochPartKind::Processing].TotalDuration, tracer->GetPartStates().at("Init").TotalDuration);
    EXPECT_EQ(init[EEpochPartKind::Waiting].TotalDuration, TDuration::Zero());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TTraceContextGuard processing(tracer->CreateEpochPartTraceContext("Work"));
        auto first = tracer->GetPartStatesByKind();
        TDelayedExecutor::WaitForDuration(20ms);
        auto second = tracer->GetPartStatesByKind();
        EXPECT_GE(first[EEpochPartKind::Processing].TotalDuration, init[EEpochPartKind::Processing].TotalDuration);
        EXPECT_GE(second[EEpochPartKind::Processing].TotalDuration - first[EEpochPartKind::Processing].TotalDuration, 20ms);
        TTraceContextGuard waiting(tracer->CreateEpochPartTraceContext("Backoff", EEpochPartKind::Waiting));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto beforeNextEpoch = tracer->GetPartStatesByKind();
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(2));
        auto next = tracer->GetPartStatesByKind();
        EXPECT_GE(next[EEpochPartKind::Processing].TotalDuration, beforeNextEpoch[EEpochPartKind::Processing].TotalDuration);
        EXPECT_EQ(next[EEpochPartKind::Waiting].TotalDuration, beforeNextEpoch[EEpochPartKind::Waiting].TotalDuration);
    }
    auto totals = tracer->GetPartStatesByKind();
    auto repeated = tracer->GetPartStatesByKind();
    EXPECT_EQ(totals[EEpochPartKind::Processing].TotalDuration, repeated[EEpochPartKind::Processing].TotalDuration);
    EXPECT_EQ(totals[EEpochPartKind::Waiting].TotalDuration, repeated[EEpochPartKind::Waiting].TotalDuration);
    auto parts = tracer->GetPartStates();
    EXPECT_EQ(totals[EEpochPartKind::Processing].TotalDuration,
        parts.at("Init").TotalDuration + parts.at("Unknown").TotalDuration + parts.at("Work").TotalDuration);
    EXPECT_EQ(totals[EEpochPartKind::Waiting].TotalDuration, parts.at("Backoff").TotalDuration);
}

TEST(TComputationTracerTest, KindStatisticsKeepTotalsWhileDecayingWallTime)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto dynamicSpec = New<TDynamicPartitionTracerSpec>();
    dynamicSpec->WallTimeHalfDecayPeriod = TDuration::MilliSeconds(200);
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), dynamicSpec);
    EXPECT_TRUE(tracer->GetPartStatesByKind().empty());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto first = tracer->GetPartStatesByKind().at(EEpochPartKind::Processing);
    EXPECT_GT(first.WallTimeEma, TDuration::Zero());
    EXPECT_GE(first.MaxDuration, TDuration::MilliSeconds(20));
    TDelayedExecutor::WaitForDuration(20ms);
    auto second = tracer->GetPartStatesByKind().at(EEpochPartKind::Processing);
    EXPECT_EQ(first.TotalDuration, second.TotalDuration);
    EXPECT_EQ(first.MaxDuration, second.MaxDuration);
    EXPECT_LT(second.WallTimeEma, first.WallTimeEma);
    auto decay = std::pow(0.5, (second.WallTimeUpdateTime - first.WallTimeUpdateTime) / dynamicSpec->WallTimeHalfDecayPeriod);
    EXPECT_NEAR(second.WallTimeEma / first.WallTimeEma, decay, 1e-3);
}

TEST(TComputationTracerTest, InheritedKindsAndOverrides)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TTraceContextGuard outer(tracer->CreateEpochPartTraceContext("Wait", EEpochPartKind::Waiting));
        {
            TTraceContextGuard inherited(tracer->CreateEpochPartTraceContext("Inherited"));
            TDelayedExecutor::WaitForDuration(20ms);
            {
                TTraceContextGuard processing(tracer->CreateEpochPartTraceContext("Override", EEpochPartKind::Processing));
                TTraceContextGuard child(tracer->CreateEpochPartTraceContext("ProcessingChild"));
                TDelayedExecutor::WaitForDuration(20ms);
            }
            TDelayedExecutor::WaitForDuration(20ms);
        }
        TTraceContextGuard sibling(tracer->CreateEpochPartTraceContext("WaitingSibling", std::nullopt));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto times = tracer->GetPartStatesByKind();
    auto parts = tracer->GetPartStates();
    EXPECT_EQ(times[EEpochPartKind::Waiting].TotalDuration,
        parts.at("Wait").TotalDuration + parts.at("Inherited").TotalDuration + parts.at("WaitingSibling").TotalDuration);
    EXPECT_EQ(times[EEpochPartKind::Processing].TotalDuration,
        parts.at("Unknown").TotalDuration + parts.at("Override").TotalDuration + parts.at("ProcessingChild").TotalDuration);
    EXPECT_GT(parts.at("Inherited").TotalDuration, TDuration::Zero());
    EXPECT_GT(parts.at("ProcessingChild").TotalDuration, TDuration::Zero());
}

TEST(TComputationTracerTest, FiberWaitOnAvailableWorkIsProcessing)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TTraceContextGuard fetch(tracer->CreateEpochPartTraceContext("Input.Fetch"));
        {
            TTraceContextGuard io(tracer->CreateEpochPartTraceContext("Read"));
            WaitFor(TDelayedExecutor::MakeDelayed(20ms)).ThrowOnError();
        }
        {
            TTraceContextGuard localThrottle(tracer->CreateEpochPartTraceContext("LocalThrottle"));
            TDelayedExecutor::WaitForDuration(20ms);
        }
    }
    auto times = tracer->GetPartStatesByKind();
    auto parts = tracer->GetPartStates();
    EXPECT_EQ(times[EEpochPartKind::Waiting].TotalDuration, TDuration::Zero());
    EXPECT_EQ(times[EEpochPartKind::Processing].TotalDuration,
        parts.at("Unknown").TotalDuration + parts.at("Input.Fetch").TotalDuration +
            parts.at("Read").TotalDuration + parts.at("LocalThrottle").TotalDuration);
    EXPECT_GE(parts.at("Read").TotalDuration, 20ms);
    EXPECT_GE(parts.at("LocalThrottle").TotalDuration, 20ms);
}

TEST(TComputationTracerTest, RepeatedNameInheritsPerSpan)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(1));
        TTraceContextGuard processing(tracer->CreateEpochPartTraceContext("SameName"));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto first = tracer->GetPartStatesByKind();
    auto previous = tracer->GetPartStates();
    EXPECT_EQ(first[EEpochPartKind::Waiting].TotalDuration, TDuration::Zero());
    EXPECT_GT(previous.at("SameName").TotalDuration, TDuration::Zero());
    {
        TTraceContextGuard epoch(tracer->StartEpochTraceContext(2));
        TTraceContextGuard waiting(tracer->CreateEpochPartTraceContext("Wait", EEpochPartKind::Waiting));
        TTraceContextGuard inherited(tracer->CreateEpochPartTraceContext("SameName"));
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto second = tracer->GetPartStatesByKind();
    auto parts = tracer->GetPartStates();
    EXPECT_EQ(second[EEpochPartKind::Waiting].TotalDuration,
        parts.at("Wait").TotalDuration + parts.at("SameName").TotalDuration - previous.at("SameName").TotalDuration);
    EXPECT_EQ(second[EEpochPartKind::Processing].TotalDuration - first[EEpochPartKind::Processing].TotalDuration, parts.at("Unknown").TotalDuration - previous.at("Unknown").TotalDuration);
}

TEST(TComputationTracerTest, InheritsFromSelectedParentNotLastActivePart)
{
    auto context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    auto tracer = CreateComputationTracer(context, New<TComputationSpec>(), New<TDynamicPartitionTracerSpec>());
    {
        auto epoch = tracer->StartEpochTraceContext(1);
        TTraceContextFinishGuard epochFinish(epoch);
        TTraceContextFinishGuard waiting(tracer->CreateEpochPartTraceContext("Wait", EEpochPartKind::Waiting));
        TTraceContextGuard foreign(TTraceContext::NewRoot("Unrelated"));
        auto child = tracer->CreateEpochPartTraceContext("DefaultProcessing");
        EXPECT_EQ(child->GetParentSpanId(), epoch->GetSpanId());
        TTraceContextGuard childGuard(child);
        TDelayedExecutor::WaitForDuration(20ms);
    }
    auto times = tracer->GetPartStatesByKind();
    auto parts = tracer->GetPartStates();
    EXPECT_EQ(times[EEpochPartKind::Waiting].TotalDuration, parts.at("Wait").TotalDuration);
    EXPECT_EQ(times[EEpochPartKind::Processing].TotalDuration, parts.at("Unknown").TotalDuration + parts.at("DefaultProcessing").TotalDuration);
    EXPECT_GT(parts.at("DefaultProcessing").TotalDuration, TDuration::Zero());
}

TEST(TComputationTracerTest, Simple)
{
    GetInstant(); // Takes 50ms to initialize at first call.

    TComputationContextPtr context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    context->Profiler = TProfiler{};
    TComputationSpecPtr spec = New<TComputationSpec>();
    TDynamicPartitionTracerSpecPtr dynamicSpec = New<TDynamicPartitionTracerSpec>();
    auto tracer = CreateComputationTracer(context, spec, dynamicSpec);
    {
        auto initTraceContextGuard = TTraceContextGuard(tracer->CreateInitTraceContext());
        TDelayedExecutor::WaitForDuration(200ms);
    }
    {
        TTraceContextGuard epochTraceGuard(tracer->StartEpochTraceContext(10));
        {
            TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("WaitInput", EEpochPartKind::Waiting));
            TDelayedExecutor::WaitForDuration(100ms);
        }
        {
            TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("Map"));
            TDelayedExecutor::WaitForDuration(100ms);
            {
                TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("SubMap"));
                TDelayedExecutor::WaitForDuration(100ms);
            }
            TDelayedExecutor::WaitForDuration(100ms);
        }
    }
    {
        TTraceContextGuard epochTraceGuard(tracer->StartEpochTraceContext(11));
        {
            TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("Finish"));
            TDelayedExecutor::WaitForDuration(300ms);
        }
    }

// Do not test timings with sanitizers.
#if !defined(_san_enabled_)
    static const TDuration threshold = 50ms;
    static auto near = [] (TDuration expected) {
        return [=] (TDuration actual) {
            return (expected - threshold < actual) && (actual < expected + threshold);
        };
    };

    auto states = tracer->GetPartStates();
    EXPECT_PRED1(near(200ms), states["Init"].TotalDuration);
    EXPECT_PRED1(near(100ms), states["WaitInput"].TotalDuration);
    EXPECT_PRED1(near(200ms), states["Map"].TotalDuration);
    EXPECT_PRED1(near(100ms), states["SubMap"].TotalDuration);
    EXPECT_PRED1(near(300ms), states["Finish"].TotalDuration);

    EXPECT_PRED1(near(300ms), states["Finish"].MaxDuration);
#endif
}

#if !defined(_san_enabled_)
TEST(TComputationTracerTest, WallTime)
{
    GetInstant(); // Takes 50ms to initialize at first call.

    TComputationContextPtr context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    context->Profiler = TProfiler{};
    TComputationSpecPtr spec = New<TComputationSpec>();
    auto tracer = CreateComputationTracer(context, spec, New<TDynamicPartitionTracerSpec>());

    auto tracerDynamicSpec = New<TDynamicPartitionTracerSpec>();
    tracerDynamicSpec->WallTimeHalfDecayPeriod = 200ms;
    tracer->Reconfigure(tracerDynamicSpec);

    {
        TTraceContextGuard traceGuard(tracer->CreateInitTraceContext());
        TDelayedExecutor::WaitForDuration(200ms);
    }

    {
        auto epochTraceContext = tracer->StartEpochTraceContext(10);

        auto states = tracer->GetPartStates();
        EXPECT_LE(100ms, states["Init"].WallTimeEma);
        EXPECT_LE(states["Init"].WallTimeEma, 200ms);

        TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("WaitInput", EEpochPartKind::Waiting));
        TDelayedExecutor::WaitForDuration(200ms);

        states = tracer->GetPartStates();
        // Test decay of old.
        EXPECT_LE(50ms, states["Init"].WallTimeEma);
        EXPECT_LE(states["Init"].WallTimeEma, 100ms);

        // New.
        EXPECT_LE(100ms, states["WaitInput"].WallTimeEma);
        EXPECT_LE(states["WaitInput"].WallTimeEma, 200ms);
    }
}
#endif

TEST(TComputationTracerTest, RepeatedPartName)
{
    GetInstant();

    TComputationContextPtr context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    context->Profiler = TProfiler{};
    TComputationSpecPtr spec = New<TComputationSpec>();
    auto tracer = CreateComputationTracer(context, spec, New<TDynamicPartitionTracerSpec>());

    {
        TTraceContextGuard epochTraceGuard(tracer->StartEpochTraceContext(10));
        for (int i = 0; i < 3; ++i) {
            TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("Accounting"));
            TDelayedExecutor::WaitForDuration(100ms);
        }
    }

#if !defined(_san_enabled_)
    static const TDuration threshold = 50ms;
    auto states = tracer->GetPartStates();
    EXPECT_LT(300ms - threshold, states["Accounting"].TotalDuration);
    EXPECT_LT(states["Accounting"].TotalDuration, 300ms + threshold);
#endif
}

TEST(TComputationTracerTest, Hierarchy)
{
    TComputationContextPtr context = New<TComputationContext>();
    context->Partition = New<TPartition>();
    context->Profiler = TProfiler{};
    TComputationSpecPtr spec = New<TComputationSpec>();
    TDynamicPartitionTracerSpecPtr dynamicSpec = New<TDynamicPartitionTracerSpec>();
    auto tracer = CreateComputationTracer(context, spec, dynamicSpec);

    for ([[maybe_unused]] int k = 0; k < 2; ++k) {
        auto epochTraceContext = tracer->StartEpochTraceContext(10);
        EXPECT_EQ(epochTraceContext->GetSpanName(), "YTFlow.Worker.Computation.Epoch");
        TTraceContextGuard epochTraceGuard(epochTraceContext);
        for ([[maybe_unused]] int i = 0; i < 2; ++i) {
            auto traceContext = tracer->CreateEpochPartTraceContext("Sync");
            EXPECT_EQ(traceContext->GetParentSpanId(), epochTraceContext->GetSpanId());
            EXPECT_EQ(traceContext->GetSpanName(), "YTFlow.Worker.Computation.Epoch.Sync");
            TTraceContextGuard traceGuard(traceContext);
            for ([[maybe_unused]] int j = 0; j < 2; ++j) {
                auto childTraceContext = tracer->CreateEpochPartTraceContext("A");
                EXPECT_EQ(childTraceContext->GetParentSpanId(), traceContext->GetSpanId());
                EXPECT_EQ(childTraceContext->GetSpanName(), "YTFlow.Worker.Computation.Epoch.Sync.A");
                TTraceContextGuard traceGuard(childTraceContext);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
