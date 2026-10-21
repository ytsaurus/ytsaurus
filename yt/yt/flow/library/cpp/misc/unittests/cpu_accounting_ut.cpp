#include <yt/yt/flow/library/cpp/misc/cpu_accounting.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/system/datetime.h>

#include <stdexcept>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TDuration BurnCpu()
{
    ui64 start = ThreadCPUTime();
    while (ThreadCPUTime() - start < 10'000) {
        DoNotOptimizeAway(start);
    }
    return TDuration::MicroSeconds(ThreadCPUTime() - start);
}

TEST(TCpuAccountingTest, NestedCallsChargeCpuOnceAcrossSuspension)
{
    auto queue = New<TActionQueue>();
    auto resumeQueue = New<TActionQueue>();
    auto result = WaitFor(BIND([invoker = queue->GetInvoker(), resumeInvoker = resumeQueue->GetInvoker()] {
        ui64 reportedMicroseconds = 0;
        auto consume = [&reportedMicroseconds] (ui64 delta) {
            reportedMicroseconds += delta;
        };
        auto measured = TDuration::Zero();
        auto inner = [&] {
            measured += BurnCpu();
            auto resume = NewPromise<void>();
            invoker->Invoke(BIND([resume] {
                resume.Set();
            }));
            WaitFor(resume.ToFuture(), resumeInvoker).ThrowOnError();
            EXPECT_EQ(GetCurrentInvoker(), resumeInvoker.Get());
            EXPECT_GT(reportedMicroseconds, 0u);
            measured += BurnCpu();
        };
        auto innerCpuTime = TDuration::Zero();
        auto outer = [&] {
            measured += BurnCpu();
            innerCpuTime = RunWithCpuAccounting(inner);
            measured += BurnCpu();
        };
        RunWithCpuAccounting(outer, consume);
        return std::pair{
            measured,
            TDuration::MicroSeconds(reportedMicroseconds) + innerCpuTime};
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ValueOrThrow();

    EXPECT_GE(result.second, result.first * 0.9);
    EXPECT_LE(result.second, result.first * 1.25 + TDuration::MilliSeconds(5));
}

TEST(TCpuAccountingTest, ReportsFinalCpuBeforeExceptionEscapes)
{
    ui64 reportedMicroseconds = 0;
    auto consume = [&reportedMicroseconds] (ui64 delta) {
        reportedMicroseconds += delta;
    };
    auto callback = [] {
        BurnCpu();
        throw std::runtime_error("Callback failed");
    };

    ui64 start = ThreadCPUTime();
    EXPECT_THROW(RunWithCpuAccounting(callback, consume), std::runtime_error);
    EXPECT_GE(reportedMicroseconds, 10'000u);
    EXPECT_LE(reportedMicroseconds, ThreadCPUTime() - start);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
