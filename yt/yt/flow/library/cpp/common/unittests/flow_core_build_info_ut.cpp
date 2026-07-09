#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_core_build_info.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowCoreBuildInfoTest, IsCached)
{
    // Singleton: same instance on every call.
    const auto& first = GetFlowCoreBuildInfo();
    const auto& second = GetFlowCoreBuildInfo();
    EXPECT_EQ(first.Get(), second.Get());
}

TEST(TFlowCoreBuildInfoTest, EmptyIsEmpty)
{
    auto empty = New<TFlowCoreBuildInfo>();
    EXPECT_TRUE(empty->IsEmpty());
}

TEST(TFlowCoreBuildInfoTest, NonEmptyIsNotEmpty)
{
    auto info = New<TFlowCoreBuildInfo>();
    info->CommitHash = "abc123";
    EXPECT_FALSE(info->IsEmpty());
}

TEST(TFlowCoreBuildInfoTest, NonZeroSvnRevisionIsNotEmpty)
{
    auto info = New<TFlowCoreBuildInfo>();
    info->SvnRevision = 123456;
    EXPECT_FALSE(info->IsEmpty());
}

TEST(TFlowCoreBuildInfoTest, NonEmptyBuildHostIsNotEmpty)
{
    auto info = New<TFlowCoreBuildInfo>();
    info->BuildHost = "build-host.example.com";
    EXPECT_FALSE(info->IsEmpty());
}

TEST(TFlowCoreBuildInfoTest, NonZeroBuildTimestampIsNotEmpty)
{
    auto info = New<TFlowCoreBuildInfo>();
    info->BuildTimestamp = 1700000000;
    EXPECT_FALSE(info->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExtractScmFieldTest, ReadsIndentedFields)
{
    constexpr std::string_view scm =
        "Arc info:\n"
        "    Branch: users/foo/some-feature\n"
        "    Commit: 1234567890abcdef\n"
        "    Author: foo\n"
        "    Summary: YTFLOW: Add VCS info to describe\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Branch"), "users/foo/some-feature");
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Commit"), "1234567890abcdef");
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Author"), "foo");
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Summary"), "YTFLOW: Add VCS info to describe");
}

TEST(TExtractScmFieldTest, ReadsUnindentedFields)
{
    constexpr std::string_view scm =
        "Branch: trunk\n"
        "Author: bar\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Branch"), "trunk");
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Author"), "bar");
}

TEST(TExtractScmFieldTest, ReadsLastLineWithoutNewline)
{
    constexpr std::string_view scm = "    Summary: trailing line without newline";
    EXPECT_EQ(
        NDetail::ExtractScmField(scm, "Summary"),
        "trailing line without newline");
}

TEST(TExtractScmFieldTest, StripsCarriageReturn)
{
    constexpr std::string_view scm = "    Branch: main\r\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Branch"), "main");
}

TEST(TExtractScmFieldTest, ReturnsEmptyForMissingKey)
{
    constexpr std::string_view scm =
        "    Branch: main\n"
        "    Author: foo\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Tag"), "");
    EXPECT_EQ(NDetail::ExtractScmField(scm, ""), "");
}

TEST(TExtractScmFieldTest, ReturnsEmptyForEmptyInput)
{
    EXPECT_EQ(NDetail::ExtractScmField("", "Author"), "");
}

TEST(TExtractScmFieldTest, DoesNotMatchPrefixedKey)
{
    // "Author" must not be matched by "AuthorOther".
    constexpr std::string_view scm = "    AuthorOther: x\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Author"), "");
}

TEST(TExtractScmFieldTest, ReturnsEmptyValueWhenColonHasNothingAfter)
{
    constexpr std::string_view scm = "    Tag:\n";
    EXPECT_EQ(NDetail::ExtractScmField(scm, "Tag"), "");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExtractAsciiPrefixTest, PureAscii)
{
    auto [prefix, truncated] = NDetail::ExtractAsciiPrefix("YTFLOW-368: Fix something");
    EXPECT_EQ(prefix, "YTFLOW-368: Fix something");
    EXPECT_FALSE(truncated);
}

TEST(TExtractAsciiPrefixTest, TruncatesAtFirstNonAsciiByte)
{
    // "YTFLOW-368: Дашборд" -> ASCII prefix "YTFLOW-368: ", then
    // Cyrillic Д starts at the first high byte 0xD0.
    constexpr std::string_view input = "YTFLOW-368: \xD0\x94\xD0\xB0\xD1\x88\xD0\xB1\xD0\xBE\xD1\x80\xD0\xB4";
    auto [prefix, truncated] = NDetail::ExtractAsciiPrefix(input);
    EXPECT_EQ(prefix, "YTFLOW-368: ");
    EXPECT_TRUE(truncated);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
