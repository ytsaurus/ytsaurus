#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/job_monitoring_index_manager.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TOperationId MakeOperationId(ui64 p0, ui64 p1)
{
    return TOperationId(TGuid(p0, p1));
}

TEST(TJobMonitoringIndexManager, Simple)
{
    TJobMonitoringIndexManager manager(5);
    ASSERT_EQ(manager.GetMaxSize(), 5);

    TOperationId operationId1 = MakeOperationId(7, 8);
    TOperationId operationId2 = MakeOperationId(7, 9);
    TOperationId operationId3 = MakeOperationId(7, 10);

    auto i1 = manager.TryAddIndex(operationId1);
    ASSERT_EQ(manager.GetSize(), 1);
    ASSERT_EQ(manager.GetResidualCapacity(), 4);
    auto i2 = manager.TryAddIndex(operationId1);
    ASSERT_EQ(manager.GetSize(), 2);
    ASSERT_EQ(manager.GetResidualCapacity(), 3);
    auto i3 = manager.TryAddIndex(operationId2);
    ASSERT_EQ(manager.GetSize(), 3);
    ASSERT_EQ(manager.GetResidualCapacity(), 2);
    // Note that jobId4 is skipped.
    auto i5 = manager.TryAddIndex(operationId3);
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_EQ(manager.GetResidualCapacity(), 1);
    auto i6 = manager.TryAddIndex(operationId3);
    ASSERT_EQ(manager.GetSize(), 5);
    ASSERT_EQ(manager.GetResidualCapacity(), 0);

    ASSERT_TRUE(i1.has_value());
    ASSERT_TRUE(i2.has_value());
    ASSERT_TRUE(i3.has_value());
    ASSERT_TRUE(i5.has_value());
    ASSERT_TRUE(i6.has_value());

    ASSERT_EQ(*i1, 0);
    ASSERT_EQ(*i2, 1);
    ASSERT_EQ(*i3, 2);
    ASSERT_EQ(*i5, 3);
    ASSERT_EQ(*i6, 4);

    auto tryI4 = manager.TryAddIndex(operationId3);
    ASSERT_FALSE(tryI4.has_value());
    ASSERT_EQ(manager.GetSize(), 5);

    ASSERT_TRUE(manager.TryRemoveIndex(operationId1, *i1));
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_FALSE(manager.TryRemoveIndex(operationId1, *i1));
    ASSERT_EQ(manager.GetSize(), 4);
    // Try remove missing index.
    ASSERT_FALSE(manager.TryRemoveIndex(operationId2, -1));

    ASSERT_TRUE(manager.TryRemoveOperation(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);
    ASSERT_FALSE(manager.TryRemoveOperation(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);
}

TEST(TJobMonitoringIndexManager, Large)
{
    constexpr int Count = 10000;
    TJobMonitoringIndexManager manager(Count);

    auto operationId = MakeOperationId(7, 8);

    for (int i = 0; i < Count; ++i) {
        auto index = manager.TryAddIndex(operationId);
        ASSERT_TRUE(index.has_value());
        ASSERT_EQ(*index, i);
        ASSERT_EQ(manager.GetSize(), i + 1);
    }

    for (int i = 0; i < Count; ++i) {
        ASSERT_TRUE(manager.TryRemoveIndex(operationId, i));
        ASSERT_EQ(manager.GetSize(), Count - i - 1);
        ASSERT_FALSE(manager.TryRemoveIndex(operationId, i));
        ASSERT_EQ(manager.GetSize(), Count - i - 1);
    }

    for (int i = 0; i < Count; ++i) {
        auto index = manager.TryAddIndex(operationId);
        ASSERT_EQ(*index, i);
        ASSERT_EQ(manager.GetSize(), i + 1);
    }

    ASSERT_TRUE(manager.TryRemoveOperation(operationId));
    ASSERT_EQ(manager.GetSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJobProxy
