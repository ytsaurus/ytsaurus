#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/private.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYT {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using testing::UnitTest;

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TInputChunkPtr& chunk, std::ostream* os)
{
    *os << ToString(chunk->GetChunkId());
}

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary SummaryWithSplitJobCount(
    TChunkStripeListPtr stripeList,
    int splitJobCount,
    std::optional<int> readRowCount)
{
    TCompletedJobSummary jobSummary;
    for (const auto& stripe : stripeList->Stripes()) {
        std::copy(
            stripe->DataSlices().begin(),
            stripe->DataSlices().end(),
            std::back_inserter(jobSummary.UnreadInputDataSlices));
    }
    jobSummary.SplitJobCount = splitJobCount;
    jobSummary.InterruptionReason = EInterruptionReason::JobSplit;
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
    EXPECT_EQ(1u, stripeList->Stripes().size());

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

void TChunkPoolTestBase::SetUp()
{
    auto config = TLogManagerConfig::CreateStderrLogger(EnableDebugOutput ? ELogLevel::Trace : ELogLevel::Error);
    config->AbortOnAlert = true;
    TLogManager::Get()->Configure(config, /*sync*/ true);
}

TLogger TChunkPoolTestBase::GetTestLogger()
{
    const auto* testInfo = UnitTest::GetInstance()->current_test_info();

    return ChunkPoolLogger()
        .WithTag("OperationId: %v, Name: %v::%v", TGuid::Create(), testInfo->name(), testInfo->test_suite_name());
}

////////////////////////////////////////////////////////////////////////////////

TLegacyKey TSortedChunkPoolTestBase::BuildRow(std::vector<i64> values)
{
    auto row = RowBuffer_->AllocateUnversioned(values.size());
    for (int index = 0; index < std::ssize(values); ++index) {
        row[index] = MakeUnversionedInt64Value(values[index], index);
    }
    return row;
}

NTableClient::TKeyBound TSortedChunkPoolTestBase::BuildBound(const char* boolOperator, std::vector<i64> values)
{
    auto allowedStrings = {"<", "<=", ">", ">="};
    YT_VERIFY(Find(allowedStrings, boolOperator));
    return TKeyBound::FromRow(
        BuildRow(values),
        /*isInclusive*/ boolOperator[1] == '=',
        /*isUpper*/ boolOperator[0] == '<');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
