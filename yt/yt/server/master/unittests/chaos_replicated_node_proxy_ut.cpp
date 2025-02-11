#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/chaos_server/helpers.h>

namespace NYT::NChaosServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedNodeProxyTest
    : public ::testing::Test
{ };

TEST_F(TChaosReplicatedNodeProxyTest, GetMinimalTabletCountTest)
{
    std::vector<TErrorOr<int>> tabletCounts;
    ASSERT_FALSE(GetMinimalTabletCount(tabletCounts).IsOK());

    tabletCounts.emplace_back(TError("Timeout"));
    ASSERT_FALSE(GetMinimalTabletCount(tabletCounts).IsOK());

    tabletCounts.emplace_back(TError("Timeout"));
    ASSERT_FALSE(GetMinimalTabletCount(tabletCounts).IsOK());

    tabletCounts.emplace_back(1);
    auto result = GetMinimalTabletCount(tabletCounts);
    ASSERT_TRUE(result.IsOK());
    ASSERT_EQ(result.Value(), 1);

    tabletCounts.emplace_back(2);
    result = GetMinimalTabletCount(tabletCounts);
    ASSERT_TRUE(result.IsOK());
    ASSERT_EQ(result.Value(), 1);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
