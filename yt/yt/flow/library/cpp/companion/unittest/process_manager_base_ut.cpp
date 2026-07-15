#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/process_manager_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/process/process.h>

#include <stdexcept>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

//! Process manager that spawns a long-living dummy process and reports
//! health check results scripted by the test.
class TFakeProcessManager
    : public TProcessManagerBase
{
public:
    TFakeProcessManager(
        const IInvokerPtr& invoker,
        TDuration startupGracePeriod,
        std::function<TError(int healthCheckIndex)> healthCheckScript,
        bool spawnFailingProcess = false,
        bool invalidParameters = false,
        IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler())
        : TProcessManagerBase(
            invoker,
            /*companionClient*/ nullptr,
            TExponentialBackoffOptions{
                .InvocationCount = 10000,
                .MinBackoff = TDuration::MilliSeconds(20),
                .MaxBackoff = TDuration::MilliSeconds(20),
            },
            /*restartDelay*/ TDuration::MilliSeconds(10),
            /*healthCheckInterval*/ TDuration::MilliSeconds(50),
            startupGracePeriod,
            /*metricsCollectionInterval*/ TDuration::Hours(1),
            NLogging::TLogger("FakeProcessManager"),
            NProfiling::TProfiler(),
            statusProfiler)
        , StatusProfiler_(std::move(statusProfiler))
        , HealthCheckScript_(std::move(healthCheckScript))
        , SpawnFailingProcess_(spawnFailingProcess)
        , InvalidParameters_(invalidParameters)
    { }

    int GetIncarnationCount() const
    {
        return IncarnationCount_.load();
    }

    int GetHealthCheckCount() const
    {
        return HealthCheckCount_.load();
    }

    const IStatusProfilerPtr& GetStatusProfiler() const
    {
        return StatusProfiler_;
    }

protected:
    void ValidateParameters() const override
    {
        if (InvalidParameters_) {
            THROW_ERROR_EXCEPTION("Companion parameters are invalid");
        }
    }

    TIntrusivePtr<TProcessBase> CreateProcessIncarnation() override
    {
        ++IncarnationCount_;
        if (SpawnFailingProcess_) {
            // A process that exits immediately, emulating a companion that fails right after start.
            auto process = New<TSimpleProcess>("/bin/false", /*copyEnv*/ true);
            return process;
        }
        auto process = New<TSimpleProcess>("/bin/sleep", /*copyEnv*/ true);
        process->AddArgument("30");
        return process;
    }

    TFuture<void> HealthCheck() override
    {
        auto error = HealthCheckScript_(++HealthCheckCount_);
        return error.IsOK() ? OKFuture : MakeFuture<void>(error);
    }

private:
    const IStatusProfilerPtr StatusProfiler_;
    const std::function<TError(int healthCheckIndex)> HealthCheckScript_;
    const bool SpawnFailingProcess_ = false;
    const bool InvalidParameters_ = false;
    std::atomic<int> IncarnationCount_ = 0;
    std::atomic<int> HealthCheckCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TFakeProcessManager);

////////////////////////////////////////////////////////////////////////////////

class TProcessManagerBaseTest
    : public ::testing::Test
{
protected:
    NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("Test");
};

TEST_F(TProcessManagerBaseTest, NoRestartsOnHealthCheckFailuresWithinStartupGracePeriod)
{
    auto manager = New<TFakeProcessManager>(
        ActionQueue_->GetInvoker(),
        /*startupGracePeriod*/ TDuration::Minutes(1),
        [] (int /*healthCheckIndex*/) {
            return TError("Companion is not ready yet");
        });
    manager->Start();

    WaitForPredicate([&] {
        return manager->GetHealthCheckCount() >= 5;
    });
    EXPECT_EQ(1, manager->GetIncarnationCount());

    manager->Shutdown();
}

TEST_F(TProcessManagerBaseTest, SlowStartingCompanionRecoversWithinStartupGracePeriod)
{
    auto manager = New<TFakeProcessManager>(
        ActionQueue_->GetInvoker(),
        /*startupGracePeriod*/ TDuration::Minutes(1),
        [] (int healthCheckIndex) {
            // The companion answers only after a few failed checks, emulating a slow start.
            return healthCheckIndex >= 3 ? TError() : TError("Companion is not ready yet");
        });
    manager->Start();

    // The early failures happen within the grace period, so the companion recovers
    // without being restarted.
    WaitForPredicate([&] {
        return manager->GetHealthCheckCount() >= 6;
    });
    EXPECT_EQ(1, manager->GetIncarnationCount());

    manager->Shutdown();
}

TEST_F(TProcessManagerBaseTest, RestartsOnHealthCheckFailuresAfterStartupGracePeriod)
{
    auto manager = New<TFakeProcessManager>(
        ActionQueue_->GetInvoker(),
        /*startupGracePeriod*/ TDuration::Zero(),
        [] (int /*healthCheckIndex*/) {
            return TError("Companion is broken");
        });
    manager->Start();

    WaitForPredicate([&] {
        return manager->GetIncarnationCount() >= 3;
    });

    manager->Shutdown();
}

TEST_F(TProcessManagerBaseTest, ReportsRetryableErrorAfterStartFailure)
{
    // When the companion keeps failing to start, the manager surfaces a retryable error on the component status.
    auto manager = New<TFakeProcessManager>(
        ActionQueue_->GetInvoker(),
        /*startupGracePeriod*/ TDuration::Minutes(1),
        // Health checks never matter here: the process exits on its own immediately.
        [] (int /*healthCheckIndex*/) {
            return TError("Companion is not ready yet");
        },
        /*spawnFailingProcess*/ true);
    manager->Start();

    // The failure is published on the component status.
    WaitForPredicate([&] {
        const auto& errors = manager->GetStatusProfiler()->GetStatus().Errors;
        for (const auto& [name, error] : errors) {
            if (error.GetMessage().find("Companion process was stopped") != std::string::npos) {
                return true;
            }
        }
        return false;
    });

    manager->Shutdown();
}

TEST_F(TProcessManagerBaseTest, ThrowsOnInvalidParameters)
{
    auto manager = New<TFakeProcessManager>(
        ActionQueue_->GetInvoker(),
        /*startupGracePeriod*/ TDuration::Minutes(1),
        [] (int /*healthCheckIndex*/) {
            return TError("Companion is not ready yet");
        },
        /*spawnFailingProcess*/ false,
        /*invalidParameters*/ true);

    EXPECT_THROW(manager->Start(), std::exception);

    bool reported = false;
    const auto& errors = manager->GetStatusProfiler()->GetStatus().Errors;
    for (const auto& [name, error] : errors) {
        if (error.GetMessage().find("Companion process parameters are invalid") != std::string::npos) {
            reported = true;
            break;
        }
    }
    EXPECT_TRUE(reported);

    // The process is never spawned while the parameters are invalid.
    EXPECT_EQ(0, manager->GetIncarnationCount());

    manager->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSumProcessFamilyRssTest, SumsParentAndAllChildren)
{
    auto getRss = [] (int pid) -> i64 {
        switch (pid) {
            case 100:
                return 10;
            case 101:
                return 20;
            case 102:
                return 30;
            case 103:
                return 40;
            default:
                return 0;
        }
    };

    EXPECT_EQ(100, SumProcessFamilyRss(100, {101, 102, 103}, getRss));
}

TEST(TSumProcessFamilyRssTest, NoDescendantsReturnsParentOnly)
{
    auto getRss = [] (int /*pid*/) -> i64 {
        return 7;
    };

    EXPECT_EQ(7, SumProcessFamilyRss(42, {}, getRss));
}

TEST(TSumProcessFamilyRssTest, ParentPidInDescendantsListIsCountedOnce)
{
    auto getRss = [] (int pid) -> i64 {
        return pid == 42 ? 5 : 11;
    };

    // GetPidsUnderParent sometimes includes the parent itself.
    // Parent (5) + descendant 43 (11), parent must not double-count.
    EXPECT_EQ(16, SumProcessFamilyRss(42, {42, 43}, getRss));
}

TEST(TSumProcessFamilyRssTest, DeadChildIsSkipped)
{
    auto getRss = [] (int pid) -> i64 {
        if (pid == 101) {
            throw std::runtime_error("vanished");
        }
        return 10;
    };

    // Parent (10) + 102 (10); 101 throws and is skipped.
    EXPECT_EQ(20, SumProcessFamilyRss(100, {101, 102}, getRss));
}

TEST(TSumProcessFamilyRssTest, DeadParentStillSumsChildren)
{
    auto getRss = [] (int pid) -> i64 {
        if (pid == 100) {
            throw std::runtime_error("parent vanished");
        }
        return 7;
    };

    EXPECT_EQ(14, SumProcessFamilyRss(100, {101, 102}, getRss));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
