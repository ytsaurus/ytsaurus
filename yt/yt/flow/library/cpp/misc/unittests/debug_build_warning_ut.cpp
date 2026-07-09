#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TIsSlowBuildTypeTest, DebugIsSlow)
{
    EXPECT_TRUE(IsSlowBuildType("debug"));
}

TEST(TIsSlowBuildTypeTest, ReleaseIsNotSlow)
{
    EXPECT_FALSE(IsSlowBuildType("release"));
}

TEST(TIsSlowBuildTypeTest, CaseInsensitive)
{
    EXPECT_TRUE(IsSlowBuildType("Debug"));
    EXPECT_FALSE(IsSlowBuildType("RELEASE"));
}

TEST(TIsSlowBuildTypeTest, SanitizerDisplayNamesAreSlow)
{
    // CurrentBuildTypeDisplayName surfaces these in place of the build type;
    // describe consumes that display name from node_info, so the predicate must
    // recognize them too.
    EXPECT_TRUE(IsSlowBuildType("ASAN"));
    EXPECT_TRUE(IsSlowBuildType("TSAN"));
    EXPECT_TRUE(IsSlowBuildType("MSAN"));
    EXPECT_TRUE(IsSlowBuildType("UBSAN"));
}

TEST(TIsSlowBuildTypeTest, ValgrindIsSlow)
{
    EXPECT_TRUE(IsSlowBuildType("valgrind"));
    EXPECT_TRUE(IsSlowBuildType("valgrind-release"));
}

TEST(TIsSlowBuildTypeTest, CoverageIsSlow)
{
    EXPECT_TRUE(IsSlowBuildType("coverage"));
}

TEST(TIsSlowBuildTypeTest, PerformanceOkBuildTypesAreNotSlow)
{
    // The rest of the `ya make --build=` options.
    EXPECT_FALSE(IsSlowBuildType("profile"));
    EXPECT_FALSE(IsSlowBuildType("relwithdebinfo"));
    EXPECT_FALSE(IsSlowBuildType("minsizerel"));
    EXPECT_FALSE(IsSlowBuildType("gprof"));
    EXPECT_FALSE(IsSlowBuildType("fastdebug"));
    EXPECT_FALSE(IsSlowBuildType("debugnoasserts"));
}

TEST(TIsSlowBuildTypeTest, EmptyIsNotSlow)
{
    // Old nodes that predate the build_type node_info field report empty.
    EXPECT_FALSE(IsSlowBuildType(""));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TIsSlowBuildWarningSuppressedTest, UnsetKeepsWarning)
{
    EXPECT_FALSE(IsSlowBuildWarningSuppressed(""));
}

TEST(TIsSlowBuildWarningSuppressedTest, OneSuppresses)
{
    EXPECT_TRUE(IsSlowBuildWarningSuppressed("1"));
}

TEST(TIsSlowBuildWarningSuppressedTest, TrueSuppresses)
{
    EXPECT_TRUE(IsSlowBuildWarningSuppressed("true"));
}

TEST(TIsSlowBuildWarningSuppressedTest, ZeroKeepsWarning)
{
    // The message advertises "=1 to silence"; a user exporting =0 to keep the
    // warning must not silence it.
    EXPECT_FALSE(IsSlowBuildWarningSuppressed("0"));
}

TEST(TIsSlowBuildWarningSuppressedTest, FalseKeepsWarning)
{
    EXPECT_FALSE(IsSlowBuildWarningSuppressed("false"));
}

TEST(TIsSlowBuildWarningSuppressedTest, GarbageKeepsWarning)
{
    EXPECT_FALSE(IsSlowBuildWarningSuppressed("junk"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSlowBuildWarningMessageTest, ContainsReasonRecommendationAndEnvVar)
{
    auto message = SlowBuildWarningMessage("ASAN");

    EXPECT_THAT(message, testing::HasSubstr("YT Flow"));
    EXPECT_THAT(message, testing::HasSubstr("ASAN"));
    EXPECT_THAT(message, testing::HasSubstr("ya make -r"));
    EXPECT_THAT(message, testing::HasSubstr("relwithdebinfo"));
    EXPECT_THAT(message, testing::HasSubstr("profile"));
    EXPECT_THAT(message, testing::HasSubstr("YT_FLOW_SUPPRESS_DEBUG_BUILD_WARNING"));
}

TEST(TSlowBuildWarningMessageTest, DebugReasonAppearsInMessage)
{
    EXPECT_THAT(SlowBuildWarningMessage("debug"), testing::HasSubstr("debug"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSlowBuildSanitizerReasonTest, MatchesCompileTimeMacros)
{
    auto reason = SlowBuildSanitizerReason();
#if defined(_asan_enabled_)
    EXPECT_EQ(reason, TStringBuf("ASAN"));
#elif defined(_tsan_enabled_)
    EXPECT_EQ(reason, TStringBuf("TSAN"));
#elif defined(_msan_enabled_)
    EXPECT_EQ(reason, TStringBuf("MSAN"));
#elif defined(_ubsan_enabled_)
    EXPECT_EQ(reason, TStringBuf("UBSAN"));
#else
    EXPECT_TRUE(reason.empty());
#endif
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCurrentBuildTypeDisplayNameTest, ReportsNonEmpty)
{
    // node_info consumers treat an empty build type as "old binary, field missing",
    // so the current binary must always report something.
    EXPECT_FALSE(CurrentBuildTypeDisplayName().empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSlowBuildPipelineMessageTextTest, MentionsBuildTypeAndRebuildHint)
{
    auto text = SlowBuildPipelineMessageText("ASAN");
    EXPECT_THAT(text, testing::HasSubstr("ASAN"));
    EXPECT_THAT(text, testing::HasSubstr("controller"));
    EXPECT_THAT(text, testing::HasSubstr("ya make -r"));
    // Pipeline UI text intentionally omits the suppression env-var hint.
    EXPECT_THAT(text, testing::Not(testing::HasSubstr("YT_FLOW_SUPPRESS_DEBUG_BUILD_WARNING")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
