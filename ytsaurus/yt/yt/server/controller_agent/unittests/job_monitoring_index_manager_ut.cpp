#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/job_monitoring_index_manager.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobMonitoringIndexManager, Simple)
{
    TJobMonitoringIndexManager manager(5);
    ASSERT_EQ(manager.GetMaxSize(), 5);

    TOperationId operationId1 = TOperationId(7, 8);
    TOperationId operationId2 = TOperationId(7, 9);
    TOperationId operationId3 = TOperationId(7, 10);

    TJobId jobId1 = TJobId(1, 1);
    TJobId jobId2 = TJobId(1, 2);
    TJobId jobId3 = TJobId(1, 3);
    TJobId jobId4 = TJobId(1, 4);
    TJobId jobId5 = TJobId(1, 5);
    TJobId jobId6 = TJobId(1, 6);

    auto i1 = manager.TryAddJob(operationId1, jobId1);
    ASSERT_EQ(manager.GetSize(), 1);
    ASSERT_EQ(manager.GetResidualCapacity(), 4);
    auto i2 = manager.TryAddJob(operationId1, jobId2);
    ASSERT_EQ(manager.GetSize(), 2);
    ASSERT_EQ(manager.GetResidualCapacity(), 3);
    auto i3 = manager.TryAddJob(operationId2, jobId3);
    ASSERT_EQ(manager.GetSize(), 3);
    ASSERT_EQ(manager.GetResidualCapacity(), 2);
    // Note that jobId4 is skipped.
    auto i5 = manager.TryAddJob(operationId3, jobId5);
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_EQ(manager.GetResidualCapacity(), 1);
    auto i6 = manager.TryAddJob(operationId3, jobId6);
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

    auto tryI4 = manager.TryAddJob(operationId3, jobId4);
    ASSERT_FALSE(tryI4.has_value());
    ASSERT_EQ(manager.GetSize(), 5);

    ASSERT_TRUE(manager.TryRemoveJob(operationId1, jobId1));
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_FALSE(manager.TryRemoveJob(operationId1, jobId1));
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_FALSE(manager.TryRemoveJob(operationId2, jobId4));

    ASSERT_TRUE(manager.TryRemoveOperationJobs(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);
    ASSERT_FALSE(manager.TryRemoveOperationJobs(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);

    auto newI1 = manager.TryAddJob(operationId1, jobId1);
    ASSERT_TRUE(newI1.has_value());
    ASSERT_EQ(*newI1, 0);
    ASSERT_EQ(manager.GetSize(), 3);

    auto i4 = manager.TryAddJob(operationId2, jobId4);
    ASSERT_TRUE(i4.has_value());
    ASSERT_EQ(*i4, 3);
    ASSERT_EQ(manager.GetSize(), 4);
}

TEST(TJobMonitoringIndexManager, Large)
{
    constexpr int Count = 10000;
    TJobMonitoringIndexManager manager(Count);

    auto operationId = TOperationId(7, 8);

    for (int i = 0; i < Count; ++i) {
        auto jobId = TJobId(1, i);
        auto index = manager.TryAddJob(operationId, jobId);
        ASSERT_TRUE(index.has_value());
        ASSERT_EQ(*index, i);
        ASSERT_EQ(manager.GetSize(), i + 1);
    }

    for (int i = 0; i < Count; ++i) {
        auto jobId = TJobId(1, i);
        ASSERT_TRUE(manager.TryRemoveJob(operationId, jobId));
        ASSERT_EQ(manager.GetSize(), Count - i - 1);
        ASSERT_FALSE(manager.TryRemoveJob(operationId, jobId));
        ASSERT_EQ(manager.GetSize(), Count - i - 1);
    }

    for (int i = 0; i < Count; ++i) {
        auto jobId = TJobId(1, i);
        auto index = manager.TryAddJob(operationId, jobId);
        ASSERT_EQ(*index, i);
        ASSERT_EQ(manager.GetSize(), i + 1);
    }

    ASSERT_TRUE(manager.TryRemoveOperationJobs(operationId));
    ASSERT_EQ(manager.GetSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJobProxy
