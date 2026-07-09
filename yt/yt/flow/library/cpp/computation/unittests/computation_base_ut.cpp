#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf VisitorDrivenClass = "NYT::NFlow::TStaticTableKeyVisitorJoiner";
constexpr TStringBuf LookupJoinerClass = "NYT::NFlow::TSimpleExternalStateJoiner";

TKeyVisitorStreamSpecPtr MakeStream(std::optional<THashSet<std::string>> externalNames)
{
    auto spec = New<TKeyVisitorStreamSpec>();
    spec->ExternalNames = std::move(externalNames);
    return spec;
}

TExternalStateJoinerSpecPtr MakeJoiner(TStringBuf className)
{
    auto spec = New<TExternalStateJoinerSpec>();
    spec->ExternalStateJoinerClassName = className;
    return spec;
}

TComputationSpecPtr MakeSpec(
    THashMap<TStreamId, TKeyVisitorStreamSpecPtr> streams,
    THashMap<std::string, TExternalStateJoinerSpecPtr> joiners)
{
    auto spec = New<TComputationSpec>();
    spec->KeyVisitorStreams = std::move(streams);
    spec->ExternalStateJoiners = std::move(joiners);
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyVisitorJoinerBindingsTest, RejectsSameJoinerInTwoStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"joiner"})},
        },
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_THROW(ValidateKeyVisitorJoinerBindings(*spec), std::exception);
}

TEST(TKeyVisitorJoinerBindingsTest, AcceptsSingleStream)
{
    auto spec = MakeSpec(
        {{TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})}},
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, AcceptsDifferentJoinersInDifferentStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"j1"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"j2"})},
        },
        {
            {"j1", MakeJoiner(VisitorDrivenClass)},
            {"j2", MakeJoiner(VisitorDrivenClass)},
        });
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresNonVisitorDrivenJoiner)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"lookup"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"lookup"})},
        },
        {{"lookup", MakeJoiner(LookupJoinerClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresNamesWithoutJoiner)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"manager"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"manager"})},
        },
        {});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresUnregisteredJoinerClass)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"joiner"})},
        },
        {{"joiner", MakeJoiner("NYT::NFlow::TUnknownJoiner")}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresScanAllStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(std::nullopt)},
            {TStreamId("s2"), MakeStream(std::nullopt)},
        },
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
