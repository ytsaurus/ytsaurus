#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/companion_entrypoint.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionEntrypointTest, DefaultsAreSane)
{
    auto entrypoint = New<TCompanionEntrypoint>();
    EXPECT_TRUE(entrypoint->Executable.empty());
    EXPECT_TRUE(entrypoint->Args.empty());
    EXPECT_TRUE(entrypoint->Env.empty());
}

TEST(TCompanionEntrypointTest, ParseFullEntrypointFromYson)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "executable" = "/path/to/java";
        "args" = ["-cp"; "/cp"; "com.example.Main"];
        "env" = {"FOO" = "bar"};
    })"));
    auto entrypoint = NYTree::ConvertTo<TCompanionEntrypointPtr>(yson);
    EXPECT_EQ("/path/to/java", entrypoint->Executable);
    ASSERT_EQ(3u, entrypoint->Args.size());
    EXPECT_EQ("-cp", entrypoint->Args[0]);
    EXPECT_EQ("/cp", entrypoint->Args[1]);
    EXPECT_EQ("com.example.Main", entrypoint->Args[2]);
    ASSERT_EQ(1u, entrypoint->Env.size());
    EXPECT_EQ("bar", entrypoint->Env.at("FOO"));
}

TEST(TCompanionEntrypointTest, ParseEntrypointWithMinimalFields)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "executable" = "/path/to/companion";
    })"));
    auto entrypoint = NYTree::ConvertTo<TCompanionEntrypointPtr>(yson);
    EXPECT_EQ("/path/to/companion", entrypoint->Executable);
    EXPECT_TRUE(entrypoint->Args.empty());
    EXPECT_TRUE(entrypoint->Env.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
