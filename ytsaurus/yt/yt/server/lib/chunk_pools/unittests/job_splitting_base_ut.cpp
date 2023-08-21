#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/chunk_pools/mock/chunk_pool.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

namespace NYT::NChunkPools {
namespace {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary CreateSummary(int splitJobCount, i64 readRowCount, bool isInterrupted)
{
    TCompletedJobSummary summary;
    auto statistics = std::make_shared<TStatistics>();
    statistics->AddSample(InputRowCountPath, readRowCount);
    summary.Statistics = std::move(statistics);
    summary.InterruptReason = isInterrupted ? EInterruptReason::JobSplit : EInterruptReason::None;
    summary.SplitJobCount = splitJobCount;
    return summary;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJobSplittingBaseTest, IntactBehavior)
{
    // This corresponds to what happens when derived class does not call
    // Completed and RegisterChildCookies. We must always allow job splitting
    // and not consume any additional memory.
    auto base = New<TTestJobSplittingBase>();
    EXPECT_TRUE(base->IsSplittable(0));
    EXPECT_TRUE(base->IsSplittable(std::numeric_limits<int>::max() / 2));
    EXPECT_EQ(base->GetMaxVectorSize(), 0ul);
}

TEST(TJobSplittingBaseTest, OnlyChildExpected)
{
    auto base = New<TTestJobSplittingBase>();

    EXPECT_TRUE(base->IsSplittable(0));

    // Recall that emptiness of job input does not affect anything for interrupted jobs.
    base->Completed(0, CreateSummary(/*splitJobCount*/ 1, /*readRowCount*/ 0, /*isInterrupted*/ true));
    base->RegisterChildCookies(0, {1});

    // Pool was asked for a single job, it produced a single job. There is no reason to
    // consider it unsplittable.
    EXPECT_TRUE(base->IsSplittable(1));
}

TEST(TJobSplittingBaseTest, OnlyChildUnexpected)
{
    auto base = New<TTestJobSplittingBase>();

    EXPECT_TRUE(base->IsSplittable(0));

    base->Completed(0, CreateSummary(/*splitJobCount*/ 2, /*readRowCount*/ 0, /*isInterrupted*/ true));
    base->RegisterChildCookies(0, {1});

    // Pool was asked to split job into two, still it produced a single job.
    EXPECT_FALSE(base->IsSplittable(1));
}

TEST(TJobSplittingBaseTest, EmptySiblingsPositive)
{
    auto base = New<TTestJobSplittingBase>();
    EXPECT_TRUE(base->IsSplittable(0));
    EXPECT_TRUE(base->IsSplittable(std::numeric_limits<int>::max() / 2));
    EXPECT_EQ(base->GetMaxVectorSize(), 0ul);

    base->Completed(0, CreateSummary(/*splitJobCount*/ 3, /*readRowCount*/ 0, /*isInterrupted*/ true));
    base->RegisterChildCookies(0, {1, 2, 3});
    EXPECT_TRUE(base->IsSplittable(1));
    EXPECT_TRUE(base->IsSplittable(2));
    EXPECT_TRUE(base->IsSplittable(3));

    base->Completed(1, CreateSummary(/*splitJobCount*/ 0, /*readRowCount*/ 0, /*isInterrupted*/ false));
    EXPECT_TRUE(base->IsSplittable(2));
    EXPECT_TRUE(base->IsSplittable(3));

    base->Completed(2, CreateSummary(/*splitJobCount*/ 0, /*readRowCount*/ 1, /*isInterrupted*/ false));
    EXPECT_TRUE(base->IsSplittable(3));

    // It turns out that job 0 was split in vain but it is already too late to do anything.
    base->Completed(3, CreateSummary(/*splitJobCount*/ 0, /*readRowCount*/ 0, /*isInterrupted*/ false));
}

TEST(TJobSplittingBaseTest, EmptySiblingsNegative)
{
    auto base = New<TTestJobSplittingBase>();
    EXPECT_TRUE(base->IsSplittable(0));
    EXPECT_TRUE(base->IsSplittable(std::numeric_limits<int>::max() / 2));
    EXPECT_EQ(base->GetMaxVectorSize(), 0ul);

    base->Completed(0, CreateSummary(/*splitJobCount*/ 3, /*readRowCount*/ 0, /*isInterrupted*/ true));
    base->RegisterChildCookies(0, {1, 2, 3});
    EXPECT_TRUE(base->IsSplittable(1));
    EXPECT_TRUE(base->IsSplittable(2));
    EXPECT_TRUE(base->IsSplittable(3));

    // Similar to previous test, but we were lucky with the order of job completion.
    base->Completed(1, CreateSummary(/*splitJobCount*/ 0, /*readRowCount*/ 0, /*isInterrupted*/ false));
    EXPECT_TRUE(base->IsSplittable(2));
    EXPECT_TRUE(base->IsSplittable(3));

    base->Completed(3, CreateSummary(/*splitJobCount*/ 0, /*readRowCount*/ 0, /*isInterrupted*/ false));
    // Both siblings turned out to be empty, thus this job is no more splittable.
    EXPECT_FALSE(base->IsSplittable(2));

    // Suppose we managed to start job 3 before we found out it should be unsplittable.
    base->Completed(2, CreateSummary(/*splitJobCount*/ 1, /*readRowCount*/ 10, /*isInterrupted*/ true));
    base->RegisterChildCookies(2, {4});
    // Unsplittability must be propagated to the rest of the job.
    EXPECT_FALSE(base->IsSplittable(4));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent

