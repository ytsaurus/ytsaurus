#include <yt/yt/server/controller_agent/universal_monitoring_descriptor_manager.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TOperationId MakeOperationId(ui64 p0, ui64 p1)
{
    return TOperationId(TGuid(p0, p1));
}

TEST(TUniversalMonitoringDescriptorManagerTest, RemoveOperationWithReleasedDescriptors)
{
    TUniversalMonitoringDescriptorManager manager(1);
    auto operationId = MakeOperationId(7, 8);

    ASSERT_TRUE(manager.TryAcqireMonitoringDescriptor(operationId));
    ASSERT_TRUE(manager.TryReleaseMonitoringDescriptor(operationId));
    ASSERT_EQ(manager.GetSize(), 0);

    ASSERT_TRUE(manager.TryRemoveOperation(operationId));
    ASSERT_FALSE(manager.TryRemoveOperation(operationId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
