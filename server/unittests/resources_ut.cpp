#include <yt/core/test_framework/framework.h>

#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/resource_capacities.h>

namespace NYP::NServer::NCluster::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDiskResourceTest, Exclusive)
{
    TString storageClass;

    TDiskVolumePolicyList allPolicies{
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_QUOTA,
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_EXCLUSIVE};

    TDiskResource diskResource1(
        storageClass,
        /* supportedPolicies */ allPolicies,
        /* totalCapacities */ MakeDiskCapacities(10, 10, 10),
        /* used */ false,
        /* usedExclusively */ false,
        /* allocatedCapacities */ MakeDiskCapacities(0, 0, 0));

    TDiskResource diskResource2(diskResource1);

    // Exclusive before non-exclusive within disk1.
    EXPECT_TRUE(diskResource1.TryAllocate(
        /* exclusive */ true,
        storageClass,
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_EXCLUSIVE,
        MakeDiskCapacities(1, 1, 1)));

    EXPECT_FALSE(diskResource1.TryAllocate(
        /* exclusive */ false,
        storageClass,
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_QUOTA,
        MakeDiskCapacities(1, 1, 1)));

    // Exclusive after non-exclusive within disk2.
    EXPECT_TRUE(diskResource2.TryAllocate(
        /* exclusive */ false,
        storageClass,
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_QUOTA,
        MakeDiskCapacities(1, 1, 1)));

    EXPECT_FALSE(diskResource2.TryAllocate(
        /* exclusive */ true,
        storageClass,
        NClient::NApi::NProto::EDiskVolumePolicy::DVP_EXCLUSIVE,
        MakeDiskCapacities(1, 1, 1)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NCluster::NTests
