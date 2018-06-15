#include "chunk_stripe.h"


#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/job_size_adjuster.h>
#include <yt/server/controller_agent/progress_counter.h>
#include <yt/server/controller_agent/private.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NChunkPools {

using namespace NTableClient;
using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

void TChunkStripeStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, DataWeight);
    Persist(context, RowCount);
    Persist(context, MaxBlockSize);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe(bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{ }

TChunkStripe::TChunkStripe(TInputDataSlicePtr dataSlice, bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{
    DataSlices.emplace_back(std::move(dataSlice));
}

TChunkStripe::TChunkStripe(TChunkListId chunkListId, TBoundaryKeys boundaryKeys)
    : ChunkListId(chunkListId)
    , BoundaryKeys(boundaryKeys)
{ }

TChunkStripeStatistics TChunkStripe::GetStatistics() const
{
    TChunkStripeStatistics result;

    for (const auto& dataSlice : DataSlices) {
        result.DataWeight += dataSlice->GetDataWeight();
        result.RowCount += dataSlice->GetRowCount();
        ++result.ChunkCount;
        result.MaxBlockSize = std::max(result.MaxBlockSize, dataSlice->GetMaxBlockSize());
    }

    return result;
}

int TChunkStripe::GetChunkCount() const
{
    int result = 0;
    for (const auto& dataSlice : DataSlices) {
        result += dataSlice->GetChunkCount();
    }
    return result;
}

int TChunkStripe::GetTableIndex() const
{
    YCHECK(!DataSlices.empty());
    YCHECK(!DataSlices.front()->ChunkSlices.empty());
    return DataSlices.front()->ChunkSlices.front()->GetInputChunk()->GetTableIndex();
}

int TChunkStripe::GetInputStreamIndex() const
{
    YCHECK(!DataSlices.empty());
    return DataSlices.front()->InputStreamIndex;
}

void TChunkStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataSlices);
    Persist(context, WaitingChunkCount);
    Persist(context, Foreign);
    Persist(context, Solid);
    Persist(context, ChunkListId);
    Persist(context, BoundaryKeys);
}

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    TChunkStripeStatistics result;
    result.ChunkCount = lhs.ChunkCount + rhs.ChunkCount;
    result.DataWeight = lhs.DataWeight + rhs.DataWeight;
    result.RowCount = lhs.RowCount + rhs.RowCount;
    result.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return result;
}

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.DataWeight += rhs.DataWeight;
    lhs.RowCount += rhs.RowCount;
    lhs.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return lhs;
}

TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics)
{
    TChunkStripeStatistics sum;
    for (const auto& stat : statistics) {
        sum += stat;
    }
    return TChunkStripeStatisticsVector(1, sum);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeList::TChunkStripeList(int stripeCount)
    : Stripes(stripeCount)
{ }

TChunkStripeStatisticsVector TChunkStripeList::GetStatistics() const
{
    TChunkStripeStatisticsVector result;
    result.reserve(Stripes.size());
    for (const auto& stripe : Stripes) {
        result.push_back(stripe->GetStatistics());
    }
    return result;
}

TChunkStripeStatistics TChunkStripeList::GetAggregateStatistics() const
{
    TChunkStripeStatistics result;
    result.ChunkCount = TotalChunkCount;
    if (IsApproximate) {
        result.RowCount = TotalRowCount * ApproximateSizesBoostFactor;
        result.DataWeight = TotalDataWeight * ApproximateSizesBoostFactor;
    } else {
        result.RowCount = TotalRowCount;
        result.DataWeight = TotalDataWeight;
    }
    return result;
}

void TChunkStripeList::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripes);
    Persist(context, PartitionTag);
    Persist(context, IsApproximate);
    Persist(context, TotalDataWeight);
    Persist(context, LocalDataWeight);
    Persist(context, TotalRowCount);
    Persist(context, TotalChunkCount);
    Persist(context, LocalChunkCount);
    Persist(context, IsSplittable);
}

TChunkStripeList* TChunkStripeList::SetSplittable(bool splittable)
{
    IsSplittable = splittable;
    return this;
}

const TChunkStripeListPtr NullStripeList = New<TChunkStripeList>()->SetSplittable(true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
