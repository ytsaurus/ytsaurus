#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NChaosClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool Equal(const TReplicationProgress& lhs, const TReplicationProgress& rhs)
{
    if (lhs.UpperKey != rhs.UpperKey || lhs.Segments.size() != rhs.Segments.size()) {
        return false;
    }
    for (const auto& [lhs, rhs] : Zip(lhs.Segments, rhs.Segments)) {
        if (lhs.Timestamp != rhs.Timestamp || lhs.LowerKey != rhs.LowerKey) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TUpdateReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        const char*>>
{ };

TEST_P(TUpdateReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    auto progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& update = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<1>(params)));
    const auto& expected = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<2>(params)));

    UpdateReplicationProgress(&progress, update);

    EXPECT_TRUE(Equal(progress, expected))
        << "progress: " << std::get<0>(params) << std::endl
        << "update: " << std::get<1>(params) << std::endl
        << "expected: " << std::get<2>(params) << std::endl
        << "actual: " << ConvertToYsonString(progress, EYsonFormat::Text).AsStringBuf() << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TUpdateReplicationProgressTest,
    TUpdateReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1u}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1u}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1u}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[2]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[2]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[2]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=2}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1};{lower_key=[3];timestamp=3}];upper_key=[4]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=2};{lower_key=[3];timestamp=3};{lower_key=[4];timestamp=2}];upper_key=[<type=max>#]}")
));

////////////////////////////////////////////////////////////////////////////////

class TCompareReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        bool>>
{ };

TEST_P(TCompareReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    const auto& progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& other = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<1>(params)));
    bool expected = std::get<2>(params);

    bool result = IsReplicationProgressGreaterOrEqual(progress, other);

    EXPECT_EQ(result, expected)
        << "progress: " << std::get<0>(params) << std::endl
        << "other: " << std::get<1>(params) << std::endl
        << "expected: " << expected << std::endl
        << "actual: " << result << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TCompareReplicationProgressTest,
    TCompareReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0};];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=2}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=2}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=0};{lower_key=[3];timestamp=1}];upper_key=[4]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[3];timestamp=0}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            false)
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
