#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/private.h>

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT {

using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TIntrusivePtr<NChunkClient::TInputChunk>& chunk, std::ostream* os)
{
    *os << ToString(chunk->GetChunkId());
}

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

TLogger GetTestLogger()
{
    const auto* testInfo =
        testing::UnitTest::GetInstance()->current_test_info();

    return ChunkPoolLogger()
        .WithTag("OperationId: %v, Name: %v::%v", TGuid::Create(), testInfo->name(), testInfo->test_suite_name())
        .WithMinLevel(ELogLevel::Trace);
}

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary SummaryWithSplitJobCount(
    TChunkStripeListPtr stripeList,
    int splitJobCount,
    std::optional<int> readRowCount)
{
    TCompletedJobSummary jobSummary;
    for (const auto& stripe : stripeList->Stripes) {
        std::copy(
            stripe->DataSlices.begin(),
            stripe->DataSlices.end(),
            std::back_inserter(jobSummary.UnreadInputDataSlices));
    }
    jobSummary.SplitJobCount = splitJobCount;
    jobSummary.InterruptionReason = EInterruptReason::JobSplit;
    if (readRowCount) {
        auto statistics = std::make_shared<TStatistics>();
        statistics->AddSample(InputRowCountPath, *readRowCount);
        jobSummary.Statistics = std::move(statistics);
    }
    return jobSummary;
}

////////////////////////////////////////////////////////////////////////////////

void CheckUnsuccessfulSplitMarksJobUnsplittable(IPersistentChunkPoolPtr chunkPool)
{
    EXPECT_EQ(1, chunkPool->GetJobCounter()->GetPending());
    auto cookie = chunkPool->Extract(TNodeId());
    EXPECT_EQ(0, cookie);
    EXPECT_TRUE(chunkPool->IsSplittable(cookie));
    auto stripeList = chunkPool->GetStripeList(cookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());

    auto splitJobCount = 3;
    auto readRowCount = 0;

    auto completedJobSummary = SummaryWithSplitJobCount(
        chunkPool->GetStripeList(cookie),
        splitJobCount,
        readRowCount);
    chunkPool->Completed(cookie, completedJobSummary);

    EXPECT_EQ(1, chunkPool->GetJobCounter()->GetPending());
    auto childCookie = chunkPool->Extract(TNodeId());
    EXPECT_EQ(1, childCookie);
    EXPECT_FALSE(chunkPool->IsSplittable(childCookie));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
