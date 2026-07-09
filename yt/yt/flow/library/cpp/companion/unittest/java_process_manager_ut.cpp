#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_singleton_state.h>
#include <yt/yt/flow/library/cpp/companion/config.h>
#include <yt/yt/flow/library/cpp/companion/java_process_manager.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <util/system/tempfile.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestJavaProcessManager
    : public TJavaProcessManager
{
public:
    using TJavaProcessManager::TJavaProcessManager;
    using TJavaProcessManager::ValidateParameters;
};

//! Installs a singleton config with the given knob value, constructs the manager
//! (which captures the config), then restores the previous singleton — also pinning
//! that the config is captured at construction time, not at validation time.
TIntrusivePtr<TTestJavaProcessManager> CreateManager(int companionProcessCount, const TString& jdkBinPath)
{
    auto previousConfig = GetCompanionExecutionConfig();
    auto config = New<TCompanionExecutionConfig>();
    config->CompanionProcessCount = companionProcessCount;
    SetCompanionExecutionConfig(config);
    auto manager = New<TTestJavaProcessManager>(
        GetSyncInvoker(),
        /*companionClient*/ nullptr,
        TExponentialBackoffOptions{},
        /*restartDelay*/ TDuration::Seconds(1),
        /*healthCheckInterval*/ TDuration::Seconds(1),
        /*startupGracePeriod*/ TDuration::Zero(),
        /*metricsCollectionInterval*/ TDuration::Seconds(1),
        NLogging::TLogger("Test"),
        NProfiling::TProfiler{},
        CreateStatusProfiler(),
        std::string(jdkBinPath),
        /*classpath*/ "/opt/app/*",
        /*mainClass*/ "com.example.Main");
    SetCompanionExecutionConfig(previousConfig);
    return manager;
}

TEST(TJavaProcessManagerValidateParametersTest, RejectsCompanionProcessCount)
{
    // Existing temp file, so validation reaches the knob check behind the JDK path check.
    TTempFileHandle jdkBin;
    auto manager = CreateManager(/*companionProcessCount*/ 4, jdkBin.Name());

    EXPECT_THROW_WITH_SUBSTRING(manager->ValidateParameters(), "companion_process_count");
}

TEST(TJavaProcessManagerValidateParametersTest, AcceptsZeroCompanionProcessCount)
{
    TTempFileHandle jdkBin;
    auto manager = CreateManager(/*companionProcessCount*/ 0, jdkBin.Name());

    EXPECT_NO_THROW(manager->ValidateParameters());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
