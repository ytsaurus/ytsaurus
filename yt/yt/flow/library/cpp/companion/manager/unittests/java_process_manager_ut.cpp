#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/client/companion_singleton_state.h>
#include <yt/yt/flow/library/cpp/companion/client/config.h>
#include <yt/yt/flow/library/cpp/companion/manager/java_process_manager.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/library/process/process.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/fs.h>

#include <util/system/tempfile.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestJavaProcessManager
    : public TJavaProcessManager
{
public:
    using TJavaProcessManager::CreateProcessIncarnation;
    using TJavaProcessManager::TJavaProcessManager;
    using TJavaProcessManager::ValidateParameters;
};

//! Installs a singleton config with the given knob value, constructs the manager
//! (which captures the config), then restores the previous singleton — also pinning
//! that the config is captured at construction time, not at validation time.
TIntrusivePtr<TTestJavaProcessManager> CreateManager(
    int companionProcessCount,
    const TString& jdkBinPath,
    const THashMap<std::string, std::string>& environment = {})
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
        CreateSyncStatusProfiler(),
        std::string(jdkBinPath),
        /*classpath*/ "/opt/app/*",
        /*mainClass*/ "com.example.Main",
        environment);
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

class TJavaProcessManagerEnvironmentTest
    : public ::testing::Test
{ };

TEST_W(TJavaProcessManagerEnvironmentTest, ReusesSavedEnvironmentForEveryIncarnation)
{
    auto previousInherited = TryGetEnv("YT_FLOW_TEST_INHERITED");
    auto previousOverride = TryGetEnv("YT_FLOW_TEST_OVERRIDE");
    Y_DEFER
    {
        if (previousInherited) {
            SetEnv("YT_FLOW_TEST_INHERITED", *previousInherited);
        } else {
            UnsetEnv("YT_FLOW_TEST_INHERITED");
        }
        if (previousOverride) {
            SetEnv("YT_FLOW_TEST_OVERRIDE", *previousOverride);
        } else {
            UnsetEnv("YT_FLOW_TEST_OVERRIDE");
        }
    };
    SetEnv("YT_FLOW_TEST_INHERITED", "inherited=value");
    SetEnv("YT_FLOW_TEST_OVERRIDE", "parent-value");

    TTempFileHandle jdkBin;
    {
        TFileOutput script(jdkBin.Name());
        script << "#!/bin/sh\nprintf '%s\\n%s\\n%s\\n%s' \"$PATH\" \"$YT_FLOW_TEST_INHERITED\" "
                  "\"$YT_FLOW_TEST_OVERRIDE\" \"$YT_FLOW_COMPANION_CONFIG\"\n";
        script.Finish();
    }
    jdkBin.Close();
    ASSERT_TRUE(NFs::SetExecutable(jdkBin.Name(), true));
    THashMap<std::string, std::string> environment{
        {"PATH", "saved-options"},
        {"YT_FLOW_TEST_OVERRIDE", "spec-value"},
        {"YT_FLOW_COMPANION_CONFIG", "stale-config"},
    };
    auto manager = CreateManager(/*companionProcessCount*/ 0, jdkBin.Name(), environment);
    environment["PATH"] = "changed-after-construction";

    for (int incarnation = 0; incarnation < 2; ++incarnation) {
        auto config = New<TCompanionExecutionConfig>();
        config->SessionId = ToString(incarnation);
        auto process = manager->CreateProcessIncarnation(config);
        EXPECT_EQ(std::string::npos, process->GetCommandLine().find("saved-options"));
        auto reader = process->GetStdOutReader();
        auto finished = process->Spawn();
        auto buffer = TSharedMutableRef::Allocate(4_KB, {.InitializeStorage = false});
        std::string output;
        while (auto size = NConcurrency::WaitFor(reader->Read(buffer)).ValueOrThrow()) {
            output.append(buffer.Begin(), size);
        }
        NConcurrency::WaitFor(finished).ThrowOnError();
        const std::string expectedEnvironment = "saved-options\ninherited=value\nspec-value\n";
        ASSERT_TRUE(output.starts_with(expectedEnvironment));
        auto restored = NYTree::ConvertTo<TCompanionExecutionConfigPtr>(
            NYson::TYsonString(TStringBuf(output).SubStr(expectedEnvironment.size())));
        EXPECT_EQ(config->SessionId, restored->SessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
