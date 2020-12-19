#include <yt/core/test_framework/framework.h>

#include <yt/server/controller_agent/job_monitoring_index_manager.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobMonitoringIndexManager, Simple)
{
    TJobMonitoringIndexManager manager;

    TOperationId operationId1 = TOperationId(7, 8);
    TOperationId operationId2 = TOperationId(7, 9);
    TOperationId operationId3 = TOperationId(7, 10);

    TJobId jobId1 = TJobId(1, 1);
    TJobId jobId2 = TJobId(1, 2);
    TJobId jobId3 = TJobId(1, 3);
    TJobId jobId4 = TJobId(1, 4);
    TJobId jobId5 = TJobId(1, 5);
    TJobId jobId6 = TJobId(1, 6);

    auto i1 = manager.AddJob(operationId1, jobId1);
    ASSERT_EQ(manager.GetSize(), 1);
    auto i2 = manager.AddJob(operationId1, jobId2);
    ASSERT_EQ(manager.GetSize(), 2);
    auto i3 = manager.AddJob(operationId2, jobId3);
    ASSERT_EQ(manager.GetSize(), 3);
    // Note that jobId4 is skipped.
    auto i5 = manager.AddJob(operationId3, jobId5);
    ASSERT_EQ(manager.GetSize(), 4);
    auto i6 = manager.AddJob(operationId3, jobId6);
    ASSERT_EQ(manager.GetSize(), 5);

    ASSERT_EQ(i1, 0);
    ASSERT_EQ(i2, 1);
    ASSERT_EQ(i3, 2);
    ASSERT_EQ(i5, 3);
    ASSERT_EQ(i6, 4);

    ASSERT_TRUE(manager.TryRemoveJob(operationId1, jobId1));
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_FALSE(manager.TryRemoveJob(operationId1, jobId1));
    ASSERT_EQ(manager.GetSize(), 4);
    ASSERT_FALSE(manager.TryRemoveJob(operationId2, jobId4));

    ASSERT_TRUE(manager.TryRemoveOperationJobs(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);
    ASSERT_FALSE(manager.TryRemoveOperationJobs(operationId3));
    ASSERT_EQ(manager.GetSize(), 2);

    auto newI1 = manager.AddJob(operationId1, jobId1);
    ASSERT_EQ(newI1, 0);
    ASSERT_EQ(manager.GetSize(), 3);

    auto i4 = manager.AddJob(operationId2, jobId4);
    ASSERT_EQ(i4, 3);
    ASSERT_EQ(manager.GetSize(), 4);
}

TEST(TJobMonitoringIndexManager, Large)
{
    TJobMonitoringIndexManager manager;

    constexpr int Count = 10000;

    auto operationId = TOperationId(7, 8);

    for (int i = 0; i < Count; ++i) {
        auto jobId = TJobId(1, i);
        ASSERT_EQ(manager.AddJob(operationId, jobId), i);
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
        ASSERT_EQ(manager.AddJob(operationId, jobId), i);
        ASSERT_EQ(manager.GetSize(), i + 1);
    }

    ASSERT_TRUE(manager.TryRemoveOperationJobs(operationId));
    ASSERT_EQ(manager.GetSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJobProxy
