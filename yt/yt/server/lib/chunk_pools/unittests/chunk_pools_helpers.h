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
    struct TExpectedCounter
    {
        i64 Total = 0;
        i64 Pending = 0;
        i64 Blocked = 0;
        i64 Running = 0;
        i64 Suspended = 0;
        i64 Completed = 0;
        i64 Failed = 0;
        i64 Aborted = 0;
        i64 Lost = 0;
    };

    void SetUp() override;

    void CheckCounter(const NControllerAgent::TConstProgressCounterPtr& actual, const TExpectedCounter& expected);

    static NLogging::TLogger GetTestLogger();

    //! A unit to measure all sizes in this file.
    static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
    static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

    // Use `ya test --test-param yt_log_level=trace` to override.
    // Use at least ELogLevel::Debug to enable Cdebug.
    static constexpr bool EnableDebugOutput = false;
    std::reference_wrapper<IOutputStream> Cdebug = Cnull;
};

class TSortedChunkPoolTestBase
    : public TChunkPoolTestBase
{
protected:
    NTableClient::TRowBufferPtr RowBuffer_ = New<NTableClient::TRowBuffer>();

    NTableClient::TLegacyKey BuildRow(std::vector<i64> values);

    //! Helper for building key bound. #boolOperator must be one of
    //! {"<", "<=", ">", ">="}.
    NTableClient::TKeyBound BuildBound(const char* boolOperator, std::vector<i64> values);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
