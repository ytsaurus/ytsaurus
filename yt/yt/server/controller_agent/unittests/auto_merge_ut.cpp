#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/controllers/auto_merge_director.h>

namespace NYT::NControllerAgent::NControllers {
namespace {

using namespace ::testing;
using namespace NLogging;

///////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("AutoMergeTest");

TEST(TAutoMergeTest, SimpleScenario)
{
    TAutoMergeDirector director(
        20 /*maxIntermediateChunkCount*/,
        5 /*maxChunkCountPerMergeJob*/,
        Logger
    );

    // Suppose that we have a single output table.

    EXPECT_TRUE(director.CanScheduleTaskJob(5));
    director.OnTaskJobStarted(5);
    director.OnTaskJobFinished(5);
    // Actually there were 4 chunks produced instead of 5 (or maybe one of them was a large chunk).
    director.AccountMergeInputChunks(4);

    // There are currently 4 intermediate chunks in auto-merge task, but it is too early to merge them.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));

    EXPECT_TRUE(director.CanScheduleTaskJob(7));
    director.OnTaskJobStarted(7);
    director.OnTaskJobFinished(7);
    director.AccountMergeInputChunks(7);

    EXPECT_TRUE(director.CanScheduleTaskJob(6));
    director.OnTaskJobStarted(6);
    director.OnTaskJobFinished(6);
    // It may actually happen that the initial estimate was smaller than the actual chunk count.
    director.AccountMergeInputChunks(7);

    EXPECT_TRUE(director.ShouldScheduleMergeJob(17));
    director.OnMergeJobStarted();
    EXPECT_TRUE(director.ShouldScheduleMergeJob(12));
    director.OnMergeJobStarted();
    EXPECT_TRUE(director.ShouldScheduleMergeJob(7));
    director.OnMergeJobStarted();
    // 2 chunks is too small for our settings.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(2));

    // We currently have 4 + 7 + 6 = 17 intermediate chunks, so we can't schedule a job with 4 chunks.
    // On the other hand, there are currently merge jobs running, so everything is ok (no need in force-flush
    // mode).
    EXPECT_FALSE(director.CanScheduleTaskJob(4));

    director.OnMergeJobFinished(5);
    director.OnMergeJobFinished(5);
    director.OnMergeJobFinished(5);

    // Now it is possible to schedule the last task job.
    EXPECT_TRUE(director.CanScheduleTaskJob(4));
    director.OnTaskJobStarted(4);
    director.OnTaskJobFinished(4);
    director.AccountMergeInputChunks(2);

    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    director.OnTaskCompleted();
    // Now all the remaining chunks should be merged.
    EXPECT_TRUE(director.ShouldScheduleMergeJob(4));
    director.OnMergeJobStarted();
    director.OnMergeJobFinished(4);
}

///////////////////////////////////////////////////////////////////////////////

TEST(TAutoMergeTest, ForceFlush)
{
    TAutoMergeDirector director(
        20 /*maxIntermediateChunkCount*/,
        5 /*maxChunkCountPerMergeJob*/,
        Logger
    );

    // Suppose that we have three output tables.

    EXPECT_TRUE(director.CanScheduleTaskJob(12));
    director.OnTaskJobStarted(12);
    director.OnTaskJobFinished(12);
    director.AccountMergeInputChunks(12);

    // Suppose these 12 chunks are evenly distributed across 3 auto-merge tasks.
    // They will perform 3 following calls to check if they have to merge their chunks.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));

    EXPECT_FALSE(director.CanScheduleTaskJob(12));
    // We fail to schedule one more task job, and there are no
    // currently running merge jobs, so the force-flush mode becomes enabled.
    EXPECT_TRUE(director.ShouldScheduleMergeJob(4));
    director.OnMergeJobStarted();
    // One merge job is already running, so there is no need in scheduling
    // merge jobs with < 5 chunks any more. Force-flush mode becomes disabled.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    director.OnMergeJobFinished(4);

    // Now we are able to schedule the long-waiting job.
    EXPECT_TRUE(director.CanScheduleTaskJob(12));
}

///////////////////////////////////////////////////////////////////////////////

TEST(TAutoMergeTest, BypassMarginalJobs)
{
    TAutoMergeDirector director(
        20 /*maxIntermediateChunkCount*/,
        5 /*maxChunkCountPerMergeJob*/,
        Logger
    );

    // Suppose that we have three output tables.

    EXPECT_TRUE(director.CanScheduleTaskJob(12));
    director.OnTaskJobStarted(12);
    director.OnTaskJobFinished(12);
    director.AccountMergeInputChunks(12);

    // Suppose these 12 chunks are evenly distributed across 3 auto-merge tasks.
    // They will perform 3 following calls to check if they have to merge their chunks.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));

    // This job has no chance to fit in maxIntermediateChunkCount limit,
    // so we just pretend it never existed. The best way to solve problems, my favourite.
    EXPECT_TRUE(director.CanScheduleTaskJob(42));
    director.OnTaskJobStarted(12);

    // Force-flush mode is not enabled.
    EXPECT_FALSE(director.ShouldScheduleMergeJob(4));
}

///////////////////////////////////////////////////////////////////////////////

TEST(TAutoMergeTest, JobFailure)
{
    TAutoMergeDirector director(
        20 /*maxIntermediateChunkCount*/,
        5 /*maxChunkCountPerMergeJob*/,
        Logger
    );

    // Suppose that we have three output tables.

    EXPECT_TRUE(director.CanScheduleTaskJob(10));
    director.OnTaskJobStarted(10);
    // If task job fails, the only thing that should be done is an OnTaskJobFinished call with
    // a proper original estimate value.
    director.OnTaskJobFinished(10);

    EXPECT_FALSE(director.ShouldScheduleMergeJob(0));

    EXPECT_TRUE(director.CanScheduleTaskJob(10));
    director.OnTaskJobStarted(10);
    director.OnTaskJobFinished(10);
    director.AccountMergeInputChunks(10);

    EXPECT_TRUE(director.ShouldScheduleMergeJob(10));
    director.OnMergeJobStarted();
    // If merge job fails, the only thing that should be done is an OnMergeTaskFinished call with
    // zero argument.
    director.OnMergeJobFinished(0 /*unregisteredIntermediateChunkCount*/);

    EXPECT_TRUE(director.ShouldScheduleMergeJob(10));
    director.OnMergeJobStarted();
    director.OnMergeJobFinished(5);
    EXPECT_TRUE(director.ShouldScheduleMergeJob(10));
    director.OnMergeJobStarted();
    director.OnMergeJobFinished(5);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent::NControllers
