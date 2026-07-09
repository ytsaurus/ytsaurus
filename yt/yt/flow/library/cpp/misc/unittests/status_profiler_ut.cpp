#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

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
    IStatusProfilerPtr root = CreateStatusProfiler();
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
    IStatusProfilerPtr root = CreateStatusProfiler();
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
    IStatusProfilerPtr root = CreateStatusProfiler();
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
    IStatusProfilerPtr root = CreateStatusProfiler();

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

} // namespace
} // namespace NYT::NFlow
