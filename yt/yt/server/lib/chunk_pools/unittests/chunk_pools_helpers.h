#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TIntrusivePtr<NChunkClient::TInputChunk>& chunk, std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger GetTestLogger();

////////////////////////////////////////////////////////////////////////////////

NControllerAgent::TCompletedJobSummary SummaryWithSplitJobCount(
    TChunkStripeListPtr stripeList,
    int splitJobCount,
    std::optional<int> readRowCount = {});

////////////////////////////////////////////////////////////////////////////////

void CheckUnsuccessfulSplitMarksJobUnsplittable(IPersistentChunkPoolPtr chunkPool);

////////////////////////////////////////////////////////////////////////////////

class TChunkSlice
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RO_PROPERTY(NChunkClient::TInputSliceLimit, UpperLimit);

    TChunkSlice(
        const NChunkClient::TInputChunkSlicePtr& chunkSlice,
        const NChunkClient::TLegacyDataSlicePtr& dataSlice,
        const NTableClient::TComparator& comparator);

private:
    NTableClient::TRowBufferPtr RowBuffer_;
};

bool IsNonEmptyIntersection(
    const TChunkSlice& lhs,
    const TChunkSlice& rhs,
    const NTableClient::TComparator& comparator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
