#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/system/tempfile.h>

#include <thread>

#if defined(__SANITIZE_THREAD__) || (defined(__has_feature) && __has_feature(thread_sanitizer))
constexpr int NUMBER_OF_ITERATIONS = 1'000;
#else
constexpr int NUMBER_OF_ITERATIONS = 1'000'000;
#endif

namespace NYT::NFlow {
namespace {

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TEST(TStatusProfilerTest, MapModify)
{
    IStatusProfilerPtr root = CreateSyncStatusProfiler();
    std::vector<IStatusProfilerPtr> children;
    THashMap<std::string, TError> allErrors;

    // Create children with prefixes.
    for (int i = 0; i < 5; ++i) {
        children.push_back(root->WithPrefix(Format("/children:%v", i)));
    }

    // Create error states (leaf nodes) and set errors.
    std::vector<IStatusErrorStatePtr> subchildren;
    for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 5; ++j) {
            IStatusErrorStatePtr subchild = children[i]->ErrorState(Format("/subchildren:%v", j));
            subchildren.push_back(subchild);
            if (j % 2) {
                subchild->SetError(TError(NYT::EErrorCode::OK, "Okay error"));
            } else {
                subchild->SetError(TError(NYT::EErrorCode::Generic, "Generic error"));
                allErrors[Format("/children:%v/subchildren:%v", i, j)] = TError(NYT::EErrorCode::Generic, "Generic error");
            }
        }
    }

    // Get status and verify errors.
    auto status = root->GetStatus();
    for (auto& [componentName, error] : allErrors) {
        ASSERT_TRUE(status.Errors.contains(componentName));
        ASSERT_TRUE(status.Errors[componentName].IsOK() == error.IsOK());
    }

    // Destroy error states for children[2] to test automatic cleanup.
    for (int j = 0; j < 5; ++j) {
        subchildren[2 * 5 + j].Reset();
    }

    // Verify that errors for destroyed nodes are removed.
    status = root->GetStatus();
    for (int j = 0; j < 5; j += 2) {
        ASSERT_FALSE(status.Errors.contains(Format("/children:2/subchildren:%v", j)));
    }
}

TEST(TStatusProfilerTest, Stress)
{
    IStatusProfilerPtr root = CreateSyncStatusProfiler();
    IStatusErrorStatePtr child;
    std::vector<std::thread> threads;
    child = root->ErrorState("child");

    std::thread t1([&root] () {
        for (int i = 0; i < NUMBER_OF_ITERATIONS; ++i) {
            root->GetStatus();
        }
    });

    std::thread t2([&root, &child] () {
        for (int i = 0; i < NUMBER_OF_ITERATIONS; ++i) {
            auto new_child = root->ErrorState("child");
            child = new_child;
        }
    });
    t1.join();
    t2.join();
}

TEST(TStatusProfilerTest, ErrorStateLifecycle)
{
    IStatusProfilerPtr root = CreateSyncStatusProfiler();
    auto errorState = root->ErrorState("component1");

    // Set an error and verify it is present.
    errorState->SetError(TError(NYT::EErrorCode::Generic, "Test error"));
    ASSERT_TRUE(root->GetStatus().Errors.contains("component1"));
    ASSERT_FALSE(root->GetStatus().Errors["component1"].IsOK());

    // Clear the error and verify it is cleared.
    errorState->ClearError();
    ASSERT_FALSE(root->GetStatus().Errors.contains("component1"));

    // Set error, destroy the error state and verify it is removed.
    errorState->SetError(TError(NYT::EErrorCode::Generic, "Test error"));
    errorState.Reset();
    ASSERT_FALSE(root->GetStatus().Errors.contains("component1"));
}

TEST(TStatusProfilerTest, ErrorStateTracking)
{
    IStatusProfilerPtr root = CreateSyncStatusProfiler();

    auto errorState = root->ErrorState("component");

    auto initialStatus = errorState->GetStatus();
    ASSERT_FALSE(initialStatus.IsOK.has_value());

    // Set error and check status.
    errorState->SetError(TError(NYT::EErrorCode::Generic, "Test error"));
    auto statusWithError = errorState->GetStatus();
    ASSERT_TRUE(statusWithError.IsOK.has_value());
    ASSERT_FALSE(statusWithError.IsOK.value());
    ASSERT_TRUE(statusWithError.LastStateChangeTime > initialStatus.LastStateChangeTime);

    Sleep(TDuration::MilliSeconds(1));

    // Clear error and check status.
    errorState->ClearError();
    auto statusWithClearedError = errorState->GetStatus();
    ASSERT_TRUE(statusWithClearedError.IsOK.has_value());
    ASSERT_TRUE(statusWithClearedError.IsOK.value());
    ASSERT_TRUE(statusWithClearedError.LastStateChangeTime > statusWithError.LastStateChangeTime);
}

////////////////////////////////////////////////////////////////////////////////

int CountLinesContaining(const TString& path, TStringBuf marker)
{
    if (!NFs::Exists(path)) {
        return 0;
    }
    TFileInput input(path);
    TString line;
    int count = 0;
    while (input.ReadLine(line)) {
        if (line.Contains(marker)) {
            ++count;
        }
    }
    return count;
}

// Local poll loop: linking test_framework's WaitForPredicate breaks the backtrace enricher test.
template <typename TPredicate>
bool PollUntil(TPredicate predicate)
{
    for (int i = 0; i < 600; ++i) {
        if (predicate()) {
            return true;
        }
        Sleep(TDuration::MilliSeconds(50));
    }
    return predicate();
}

const NLogging::TLogger BreakTestLogger("StatusProfilerBreakTest");
const NLogging::TLogger ThrottleTestLogger("StatusProfilerThrottleTest");
const NLogging::TLogger ReportTestLogger("StatusProfilerReportTest");

void ConfigureFileLogging(const NLogging::TLogger& logger, const TString& fileName)
{
    NLogging::TLogManager::Get()->Configure(
        NYTree::ConvertTo<NLogging::TLogManagerConfigPtr>(NYson::TYsonString(Format(R"({
            rules = [
                {
                    min_level = info;
                    include_categories = [ %v ];
                    writers = [ file ];
                };
            ];
            writers = {
                file = {
                    file_name = "%v";
                    type = "file";
                };
            };
        })",
        logger.GetCategory()->Name,
        fileName))),
        /*sync*/ true);
}

TEST(TStatusProfilerErrorLoggingTest, LogsBreakAndRecovery)
{
    TTempFileHandle logFile(MakeTempName(nullptr, "status_profiler_log"));
    ConfigureFileLogging(BreakTestLogger, logFile.Name());

    auto queue = New<NConcurrency::TActionQueue>("StatusLog");
    auto profiler = CreateStatusProfiler(
        queue->GetInvoker(),
        BreakTestLogger,
        TStatusProfilerLoggingOptions{
            .LogThrottle = TDuration::MilliSeconds(1),
            .ReportPeriod = TDuration::Hours(1),
        });
    auto errorState = profiler->WithPrefix("/computation")->ErrorState("/stall");

    errorState->SetError(TError("Deliberate stall"));
    EXPECT_TRUE(PollUntil([&] {
        NLogging::TLogManager::Get()->Synchronize();
        return CountLinesContaining(logFile.Name(), "became broken") >= 1 &&
            CountLinesContaining(logFile.Name(), "/computation/stall") >= 1;
    }));

    errorState->ClearError();
    EXPECT_TRUE(PollUntil([&] {
        NLogging::TLogManager::Get()->Synchronize();
        return CountLinesContaining(logFile.Name(), "Component recovered") >= 1;
    }));

    errorState.Reset();
    profiler.Reset();
}

TEST(TStatusProfilerErrorLoggingTest, ThrottlesRepeatedErrors)
{
    TTempFileHandle logFile(MakeTempName(nullptr, "status_profiler_log"));
    ConfigureFileLogging(ThrottleTestLogger, logFile.Name());

    auto queue = New<NConcurrency::TActionQueue>("StatusLog");
    auto profiler = CreateStatusProfiler(
        queue->GetInvoker(),
        ThrottleTestLogger,
        TStatusProfilerLoggingOptions{
            .LogThrottle = TDuration::Minutes(1),
            .ReportPeriod = TDuration::Hours(1),
        });
    auto errorState = profiler->ErrorState("/component");

    for (int i = 0; i < 50; ++i) {
        errorState->SetError(TError("Persistent error %v", i));
    }

    EXPECT_TRUE(PollUntil([&] {
        NLogging::TLogManager::Get()->Synchronize();
        return CountLinesContaining(logFile.Name(), "broken") >= 1;
    }));
    NLogging::TLogManager::Get()->Synchronize();
    // Only the first break survives the throttle; the other 49 re-sets are suppressed.
    EXPECT_EQ(CountLinesContaining(logFile.Name(), "broken"), 1);

    errorState.Reset();
    profiler.Reset();
}

TEST(TStatusProfilerErrorLoggingTest, BackgroundReport)
{
    TTempFileHandle logFile(MakeTempName(nullptr, "status_profiler_log"));
    ConfigureFileLogging(ReportTestLogger, logFile.Name());

    auto queue = New<NConcurrency::TActionQueue>("StatusLog");
    auto profiler = CreateStatusProfiler(
        queue->GetInvoker(),
        ReportTestLogger,
        TStatusProfilerLoggingOptions{
            .LogThrottle = TDuration::Minutes(1),
            .ReportPeriod = TDuration::MilliSeconds(50),
        });
    auto errorState = profiler->ErrorState("/component");
    errorState->SetError(TError("Stuck"));

    EXPECT_TRUE(PollUntil([&] {
        NLogging::TLogManager::Get()->Synchronize();
        return CountLinesContaining(logFile.Name(), "Component status report") >= 1 &&
            CountLinesContaining(logFile.Name(), "Status: Broken") >= 1 &&
            CountLinesContaining(logFile.Name(), "BreakCountInWindow: 1") >= 1;
    }));

    errorState.Reset();
    profiler.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
