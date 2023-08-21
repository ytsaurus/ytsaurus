#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/private.h>

namespace NYT {

using namespace NControllerAgent;
using namespace NLogging;

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

    return ChunkPoolLogger
        .WithTag("OperationId: %v, Name: %v::%v", TGuid::Create(), testInfo->name(), testInfo->test_suite_name())
        .WithMinLevel(ELogLevel::Trace);
}

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary SummaryWithSplitJobCount(TChunkStripeListPtr stripeList, int splitJobCount)
{
    TCompletedJobSummary jobSummary;
    for (const auto& stripe : stripeList->Stripes) {
        std::copy(
            stripe->DataSlices.begin(),
            stripe->DataSlices.end(),
            std::back_inserter(jobSummary.UnreadInputDataSlices));
    }
    jobSummary.SplitJobCount = splitJobCount;
    jobSummary.InterruptReason = EInterruptReason::JobSplit;
    return jobSummary;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
