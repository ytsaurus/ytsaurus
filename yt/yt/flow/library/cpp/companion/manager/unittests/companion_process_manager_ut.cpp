#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/client/config.h>
#include <yt/yt/flow/library/cpp/companion/manager/companion_entrypoint.h>
#include <yt/yt/flow/library/cpp/companion/manager/companion_process_manager.h>

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

class TTestCompanionProcessManager
    : public TCompanionProcessManager
{
public:
    using TCompanionProcessManager::CreateProcessIncarnation;
    using TCompanionProcessManager::TCompanionProcessManager;
};

TIntrusivePtr<TTestCompanionProcessManager> CreateManager(
    const std::string& executable,
    const THashMap<std::string, std::string>& environment)
{
    auto entrypoint = New<TCompanionEntrypoint>();
    entrypoint->Executable = executable;
    entrypoint->Env = environment;
    return New<TTestCompanionProcessManager>(
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
        entrypoint);
}

class TCompanionProcessManagerEnvironmentTest
    : public ::testing::Test
{ };

TEST_W(TCompanionProcessManagerEnvironmentTest, ReusesSavedEnvironmentForEveryIncarnation)
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

    TTempFileHandle executable;
    {
        TFileOutput script(executable.Name());
        script << "#!/bin/sh\nprintf '%s\\n%s\\n%s\\n%s\\n%s' \"$PATH\" \"$YT_FLOW_TEST_INHERITED\" "
                  "\"$YT_FLOW_TEST_OVERRIDE\" \"$YT_FLOW_TEST_ADDITIONAL\" \"$YT_FLOW_COMPANION_CONFIG\"\n";
        script.Finish();
    }
    executable.Close();
    ASSERT_TRUE(NFs::SetExecutable(executable.Name(), true));
    THashMap<std::string, std::string> environment{
        {"PATH", "spec-path"},
        {"YT_FLOW_TEST_OVERRIDE", "spec-value"},
        {"YT_FLOW_TEST_ADDITIONAL", "extra=value"},
        {"YT_FLOW_COMPANION_CONFIG", "stale-config"},
    };
    auto manager = CreateManager(executable.Name(), environment);
    environment["PATH"] = "changed-after-construction";

    for (int incarnation = 0; incarnation < 2; ++incarnation) {
        auto config = New<TCompanionExecutionConfig>();
        config->SessionId = ToString(incarnation);
        auto process = manager->CreateProcessIncarnation(config);
        EXPECT_EQ(std::string::npos, process->GetCommandLine().find("spec-path"));
        auto reader = process->GetStdOutReader();
        auto finished = process->Spawn();
        auto buffer = TSharedMutableRef::Allocate(4_KB, {.InitializeStorage = false});
        std::string output;
        while (auto size = NConcurrency::WaitFor(reader->Read(buffer)).ValueOrThrow()) {
            output.append(buffer.Begin(), size);
        }
        NConcurrency::WaitFor(finished).ThrowOnError();
        const std::string expectedEnvironment = "spec-path\ninherited=value\nspec-value\nextra=value\n";
        ASSERT_TRUE(output.starts_with(expectedEnvironment));
        auto restored = NYTree::ConvertTo<TCompanionExecutionConfigPtr>(
            NYson::TYsonString(TStringBuf(output).SubStr(expectedEnvironment.size())));
        EXPECT_EQ(config->SessionId, restored->SessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
