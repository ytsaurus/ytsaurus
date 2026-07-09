#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/companion_entrypoint.h>
#include <yt/yt/flow/library/cpp/companion/java_companion_manager.h>

#include <yt/yt/core/ytree/convert.h>

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
        "run_process" = %true;
        "jdk_bin_path" = "/opt/jdk/bin";
        "classpath" = "/opt/app/*";
        "main_class" = "com.example.Main";
    })"));
    auto params = NYTree::ConvertTo<TJavaCompanionManagerParametersPtr>(yson);
    EXPECT_TRUE(params->RunProcess);
    EXPECT_EQ("/opt/jdk/bin", params->JdkBinPath);
    EXPECT_EQ("/opt/app/*", params->Classpath);
    EXPECT_EQ("com.example.Main", params->MainClass);
    // Generic entrypoint stays at defaults (empty).
    ASSERT_TRUE(params->Entrypoint);
    EXPECT_TRUE(params->Entrypoint->Executable.empty());
    EXPECT_TRUE(params->Entrypoint->Args.empty());
}

TEST(TJavaCompanionManagerParametersTest, ParseGenericEntrypointForm)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "run_process" = %true;
        "entrypoint" = {
            "executable" = "/opt/jdk/bin/java";
            "args" = ["-cp"; "/opt/app/*"; "com.example.Main"];
        };
    })"));
    auto params = NYTree::ConvertTo<TJavaCompanionManagerParametersPtr>(yson);
    EXPECT_TRUE(params->RunProcess);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
