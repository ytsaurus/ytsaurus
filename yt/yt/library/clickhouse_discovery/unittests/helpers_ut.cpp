#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/clickhouse_discovery/helpers.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NClickHouseServer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TTestClickHouseHelpers, FindMaxIncarnation)
{
    THashMap<TString, IAttributeDictionaryPtr> instances;
    instances["instance_1"] = ConvertToAttributes(THashMap<TString, int>{
        {"clique_incarnation", 1},
    });
    instances["instance_2"] = ConvertToAttributes(THashMap<TString, int>{
        {"clique_incarnation", 2},
    });
    instances["instance_3"] = ConvertToAttributes(THashMap<TString, int>{
        {"clique_incarnation", 2},
    });
    instances["instance_4"] = ConvertToAttributes(THashMap<TString, int>{});

    auto filteredInstances = FilterInstancesByIncarnation(instances);
    EXPECT_EQ(filteredInstances.size(), 2u);
    EXPECT_FALSE(filteredInstances.contains("instance_1"));
    EXPECT_TRUE(filteredInstances.contains("instance_2"));
    EXPECT_TRUE(filteredInstances.contains("instance_3"));
    EXPECT_FALSE(filteredInstances.contains("instance_4"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClickHouseServer
