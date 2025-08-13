#pragma once

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/stream/null.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const NChunkClient::TInputChunkPtr& chunk, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

NControllerAgent::TCompletedJobSummary SummaryWithSplitJobCount(
    TChunkStripeListPtr stripeList,
    int splitJobCount,
    std::optional<int> readRowCount = {});

void CheckUnsuccessfulSplitMarksJobUnsplittable(IPersistentChunkPoolPtr chunkPool);

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolTestBase
    : public testing::Test
{
protected:
    void SetUp() override;

    static NLogging::TLogger GetTestLogger();

    //! A unit to measure all sizes in this file.
    static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
    static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

    static constexpr bool EnableDebugOutput = false;
    IOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
