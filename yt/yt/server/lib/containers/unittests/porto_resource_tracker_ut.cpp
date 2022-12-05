#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

#ifdef _linux_

#include <yt/yt/server/lib/containers/config.h>
#include <yt/yt/server/lib/containers/porto_executor.h>
#include <yt/yt/server/lib/containers/porto_resource_tracker.h>
#include <yt/yt/server/lib/containers/instance.h>

#include <util/system/platform.h>
#include <util/system/env.h>

namespace NYT::NContainers {
namespace {

using namespace NIO;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto TestUpdatePeriod = TDuration::MilliSeconds(10);

class TPortoTrackerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        if (GetEnv("SKIP_PORTO_TESTS") != "") {
            GTEST_SKIP();
        }
    }
};

static TString GetUniqueName()
{
    return "yt_porto_ut_" + ToString(TGuid::Create());
}

IPortoExecutorPtr CreatePortoExecutor()
{
    return CreatePortoExecutor(New<TPortoExecutorConfig>(), "default");
}

TPortoResourceTrackerPtr CreateSumPortoTracker(IPortoExecutorPtr executor, const TString& name)
{
    return New<TPortoResourceTracker>(
        GetPortoInstance(executor, name),
        TestUpdatePeriod,
        false
    );
}

TPortoResourceTrackerPtr CreateDeltaPortoTracker(IPortoExecutorPtr executor, const TString& name)
{
    return New<TPortoResourceTracker>(
        GetPortoInstance(executor, name),
        TestUpdatePeriod,
        true
    );
}

TEST_F(TPortoTrackerTest, ValidateSummaryPortoTracker)
{
    auto executor = CreatePortoExecutor();
    auto name = GetUniqueName();

    WaitFor(executor->CreateContainer(
        TRunnableContainerSpec {
            .Name = name,
            .Command = "sleep .1",
        }, true))
        .ThrowOnError();

    auto tracker = CreateSumPortoTracker(executor, name);

    auto firstStatistics = tracker->GetTotalStatistics();

    WaitFor(executor->StopContainer(name))
        .ThrowOnError();
    WaitFor(executor->SetContainerProperty(
        name,
        "command",
        "find /"))
        .ThrowOnError();
    WaitFor(executor->StartContainer(name))
        .ThrowOnError();
    Sleep(TDuration::MilliSeconds(500));

    auto secondStatistics = tracker->GetTotalStatistics();

    auto exitCode = WaitFor(executor->PollContainer(name))
        .ValueOrThrow();

    EXPECT_EQ(0, exitCode);

    WaitFor(executor->DestroyContainer(name))
        .ThrowOnError();
}

TEST_F(TPortoTrackerTest, ValidateDeltaPortoTracker)
{
    auto executor = CreatePortoExecutor();
    auto name = GetUniqueName();

    auto spec = TRunnableContainerSpec {
        .Name = name,
        .Command = "sleep .1",
    };

    WaitFor(executor->CreateContainer(spec, true))
        .ThrowOnError();

    auto tracker = CreateDeltaPortoTracker(executor, name);

    auto firstStatistics = tracker->GetTotalStatistics();

    WaitFor(executor->StopContainer(name))
        .ThrowOnError();
    WaitFor(executor->SetContainerProperty(
        name,
        "command",
        "find /"))
        .ThrowOnError();
    WaitFor(executor->StartContainer(name))
        .ThrowOnError();

    Sleep(TDuration::MilliSeconds(500));

    auto secondStatistics = tracker->GetTotalStatistics();
    auto exitCode = WaitFor(executor->PollContainer(name))
        .ValueOrThrow();

    EXPECT_EQ(0, exitCode);

    WaitFor(executor->DestroyContainer(name))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers

#endif
