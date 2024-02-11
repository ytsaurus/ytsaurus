#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobResourcesTest, CanSatisfyDiskQuotaRequests)
{
    EXPECT_TRUE(CanSatisfyDiskQuotaRequests(/*available*/ {10}, /*requests*/ {1, 2, 3, 4}));
    EXPECT_FALSE(CanSatisfyDiskQuotaRequests(/*available*/ {10}, /*requests*/ {1, 2, 3, 5}));

    EXPECT_TRUE(CanSatisfyDiskQuotaRequests(/*available*/ {10, 20}, /*requests*/ {10, 10, 10}));
    EXPECT_TRUE(CanSatisfyDiskQuotaRequests(/*available*/ {10, 20}, /*requests*/ {5, 15, 10}));
    EXPECT_FALSE(CanSatisfyDiskQuotaRequests(/*available*/ {10, 20}, /*requests*/ {15, 15}));

    // Greedy algorithm is not able to solve this case.
    EXPECT_FALSE(CanSatisfyDiskQuotaRequests(/*available*/ {10, 15}, /*requests*/ {8, 7, 5, 5}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
