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
            TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("WaitInput"));
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

        TTraceContextGuard traceGuard(tracer->CreateEpochPartTraceContext("WaitInput"));
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
