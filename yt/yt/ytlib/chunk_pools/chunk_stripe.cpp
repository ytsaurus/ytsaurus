#include "chunk_stripe.h"

#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe(bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{ }

TChunkStripe::TChunkStripe(TLegacyDataSlicePtr dataSlice, bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{
    DataSlices.emplace_back(std::move(dataSlice));
}

TChunkStripe::TChunkStripe(const std::vector<TLegacyDataSlicePtr>& dataSlices)
{
    DataSlices.insert(DataSlices.end(), dataSlices.begin(), dataSlices.end());
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
        result.ChunkCount += dataSlice->GetChunkCount();
        result.ValueCount += dataSlice->GetValueCount();
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
    YT_VERIFY(!DataSlices.empty());
    YT_VERIFY(!DataSlices.front()->ChunkSlices.empty());
    return DataSlices.front()->ChunkSlices.front()->GetInputChunk()->GetTableIndex();
}

int TChunkStripe::GetInputStreamIndex() const
{
    YT_VERIFY(!DataSlices.empty());
    return DataSlices.front()->GetInputStreamIndex();
}

void TChunkStripe::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::DataSlices>("data_slices")();
    registrar.template Field<2, &TThis::WaitingChunkCount>("waiting_chunk_count")();
    registrar.template Field<3, &TThis::Foreign>("foreign")();
    registrar.template Field<4, &TThis::Solid>("solid")();
    registrar.template Field<5, &TThis::ChunkListId>("chunk_list_id")();
    registrar.template Field<6, &TThis::BoundaryKeys>("boundary_keys")();
    registrar.template Field<7, &TThis::PartitionTag>("partition_tag")();
}

PHOENIX_DEFINE_TYPE(TChunkStripe);

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
        result.ValueCount = TotalValueCount * ApproximateSizesBoostFactor;
        result.DataWeight = TotalDataWeight * ApproximateSizesBoostFactor;
    } else {
        result.RowCount = TotalRowCount;
        result.ValueCount = TotalValueCount;
        result.DataWeight = TotalDataWeight;
    }
    return result;
}

void TChunkStripeList::AddStripe(TChunkStripePtr stripe)
{
    auto statistics = stripe->GetStatistics();
    TotalChunkCount += statistics.ChunkCount;
    TotalDataWeight += statistics.DataWeight;
    TotalRowCount += statistics.RowCount;
    TotalValueCount += statistics.ValueCount;
    Stripes.emplace_back(std::move(stripe));
}

void TChunkStripeList::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::Stripes>("stripes")();
    registrar.template Field<2, &TThis::PartitionTag>("partition_tag")();
    registrar.template Field<3, &TThis::IsApproximate>("is_approximate")();
    registrar.template Field<4, &TThis::TotalDataWeight>("total_data_weight")();
    registrar.template Field<5, &TThis::LocalDataWeight>("local_data_weight")();
    registrar.template Field<6, &TThis::TotalRowCount>("total_row_count")();
    registrar.template Field<7, &TThis::TotalValueCount>("total_value_count")();
    registrar.template Field<8, &TThis::TotalChunkCount>("total_chunk_count")();
    registrar.template Field<9, &TThis::LocalChunkCount>("local_chunk_count")();
}

const TChunkStripeListPtr NullStripeList = New<TChunkStripeList>();

PHOENIX_DEFINE_TYPE(TChunkStripeList);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
