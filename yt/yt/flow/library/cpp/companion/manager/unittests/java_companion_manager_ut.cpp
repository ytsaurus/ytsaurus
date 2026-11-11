#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/client/companion_singleton_state.h>
#include <yt/yt/flow/library/cpp/companion/client/config.h>
#include <yt/yt/flow/library/cpp/companion/manager/companion_entrypoint.h>
#include <yt/yt/flow/library/cpp/companion/manager/java_companion_manager.h>
#include <yt/yt/flow/library/cpp/companion/manager/process_manager_base.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJavaCompanionManagerParametersTest, DefaultsAreSane)
{
    auto params = New<TJavaCompanionManagerParameters>();
    EXPECT_TRUE(params->JdkBinPath.empty());
    EXPECT_TRUE(params->Classpath.empty());
    EXPECT_TRUE(params->MainClass.empty());
    // Generic entrypoint must be initialized even when convenience fields are not.
    ASSERT_TRUE(params->Entrypoint);
    EXPECT_TRUE(params->Entrypoint->Executable.empty());
    EXPECT_TRUE(params->Entrypoint->Args.empty());
}

TEST(TJavaCompanionManagerParametersTest, ParseConvenienceForm)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "jdk_bin_path" = "/opt/jdk/bin";
        "classpath" = "/opt/app/*";
        "main_class" = "com.example.Main";
        "entrypoint" = {"env" = {"OPTION" = "persisted";};};
    })"));
    auto params = NYTree::ConvertTo<TJavaCompanionManagerParametersPtr>(yson);
    EXPECT_EQ("/opt/jdk/bin", params->JdkBinPath);
    EXPECT_EQ("/opt/app/*", params->Classpath);
    EXPECT_EQ("com.example.Main", params->MainClass);
    // Java companions also use the environment from the generic entrypoint.
    ASSERT_TRUE(params->Entrypoint);
    EXPECT_EQ("persisted", params->Entrypoint->Env.at("OPTION"));
    EXPECT_TRUE(params->Entrypoint->Executable.empty());
    EXPECT_TRUE(params->Entrypoint->Args.empty());
}

TEST(TJavaCompanionManagerParametersTest, ParseGenericEntrypointForm)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "entrypoint" = {
            "executable" = "/opt/jdk/bin/java";
            "args" = ["-cp"; "/opt/app/*"; "com.example.Main"];
        };
    })"));
    auto params = NYTree::ConvertTo<TJavaCompanionManagerParametersPtr>(yson);
    // Convenience fields stay at defaults (empty).
    EXPECT_TRUE(params->JdkBinPath.empty());
    EXPECT_TRUE(params->Classpath.empty());
    EXPECT_TRUE(params->MainClass.empty());
    ASSERT_TRUE(params->Entrypoint);
    EXPECT_EQ("/opt/jdk/bin/java", params->Entrypoint->Executable);
    ASSERT_EQ(3u, params->Entrypoint->Args.size());
    EXPECT_EQ("-cp", params->Entrypoint->Args[0]);
    EXPECT_EQ("/opt/app/*", params->Entrypoint->Args[1]);
    EXPECT_EQ("com.example.Main", params->Entrypoint->Args[2]);
}

class TTestJavaCompanionManager
    : public TJavaCompanionManager
{
public:
    using TJavaCompanionManager::CreateProcessManager;
    using TJavaCompanionManager::TJavaCompanionManager;
};

class TJavaCompanionManagerTest
    : public ::testing::Test
{ };

TEST_W(TJavaCompanionManagerTest, PassesResourceEnvironmentToChild)
{
    auto previousConfig = GetCompanionExecutionConfig();
    auto previousValue = TryGetEnv("YT_FLOW_TEST_JAVA_FACTORY_ENV");
    Y_DEFER
    {
        SetCompanionExecutionConfig(previousConfig);
        if (previousValue) {
            SetEnv("YT_FLOW_TEST_JAVA_FACTORY_ENV", *previousValue);
        } else {
            UnsetEnv("YT_FLOW_TEST_JAVA_FACTORY_ENV");
        }
    };
    SetEnv("YT_FLOW_TEST_JAVA_FACTORY_ENV", "parent-value");

    auto port = NTesting::GetFreePort();
    auto config = New<TCompanionExecutionConfig>();
    config->Port = static_cast<int>(port);
    SetCompanionExecutionConfig(config);

    TTempFileHandle jdkBin;
    TTempFileHandle output;
    {
        TFileOutput script(jdkBin.Name());
        // The last Java argument is the main class; the fake JDK treats it as an output path.
        script << "#!/bin/sh\nfor arg do output=\"$arg\"; done\n"
                  "printf '%s\\n' \"$YT_FLOW_TEST_JAVA_FACTORY_ENV\" > \"$output\"\n";
        script.Finish();
    }
    jdkBin.Close();
    ASSERT_TRUE(NFs::SetExecutable(jdkBin.Name(), true));

    auto parameters = New<TJavaCompanionManagerParameters>();
    parameters->JdkBinPath = std::string(jdkBin.Name());
    parameters->Classpath = "/test/classpath";
    parameters->MainClass = std::string(output.Name());
    parameters->Entrypoint->Env["YT_FLOW_TEST_JAVA_FACTORY_ENV"] = "resource-value";

    auto actionQueue = New<NConcurrency::TActionQueue>("JavaCompanionManagerTest");
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("java_companion");
    context->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    context->ResourceSpec = New<TResourceSpec>();
    context->ResourceSpec->ResourceClassName = "NYT::NFlow::NCompanion::TJavaCompanionManager";
    context->ResourceSpec->Parameters = NYTree::ConvertTo<NYTree::IMapNodePtr>(parameters);
    context->Invoker = actionQueue->GetInvoker();
    context->Logger = NLogging::TLogger("JavaCompanionManagerTest");
    context->StatusProfiler = CreateSyncStatusProfiler();

    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = NYTree::ConvertTo<TDynamicResourceSpecPtr>(
        NYson::TYsonString(TStringBuf("{parameters={}}")));

    auto manager = New<TTestJavaCompanionManager>(context, dynamicContext);
    auto processManager = manager->CreateProcessManager();
    Y_DEFER
    {
        processManager->Shutdown();
    };
    processManager->Start();

    std::string actual;
    WaitForPredicate([&] {
        actual = TFileInput(output.Name()).ReadAll();
        return actual.ends_with('\n');
    });
    EXPECT_EQ("resource-value\n", actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
