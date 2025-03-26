#include "chunk_stripe.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

namespace NYT::NChunkPools {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NTableClient;

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
        result.CompressedDataSize += dataSlice->GetCompressedDataSize();
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
    PHOENIX_REGISTER_FIELD(1, DataSlices);
    PHOENIX_REGISTER_FIELD(2, WaitingChunkCount);
    PHOENIX_REGISTER_FIELD(3, Foreign);
    PHOENIX_REGISTER_FIELD(4, Solid);
    PHOENIX_REGISTER_FIELD(5, ChunkListId);
    PHOENIX_REGISTER_FIELD(6, BoundaryKeys);
    PHOENIX_REGISTER_FIELD(7, PartitionTag);
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
        result.CompressedDataSize = TotalCompressedDataSize * ApproximateSizesBoostFactor;
    } else {
        result.RowCount = TotalRowCount;
        result.ValueCount = TotalValueCount;
        result.DataWeight = TotalDataWeight;
        result.CompressedDataSize = TotalCompressedDataSize;
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
    TotalCompressedDataSize += statistics.CompressedDataSize;
    Stripes.emplace_back(std::move(stripe));
}

void TChunkStripeList::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Stripes);
    PHOENIX_REGISTER_FIELD(2, PartitionTag);
    PHOENIX_REGISTER_FIELD(3, IsApproximate);
    PHOENIX_REGISTER_FIELD(4, TotalDataWeight);
    PHOENIX_REGISTER_FIELD(5, LocalDataWeight);
    PHOENIX_REGISTER_FIELD(6, TotalRowCount);
    PHOENIX_REGISTER_FIELD(7, TotalValueCount);
    PHOENIX_REGISTER_FIELD(8, TotalChunkCount);
    PHOENIX_REGISTER_FIELD(9, LocalChunkCount);
    PHOENIX_REGISTER_FIELD(10, TotalCompressedDataSize,
        .SinceVersion(ESnapshotVersion::MaxCompressedDataSizePerJob)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            for (const auto& stripe : this_->Stripes) {
                this_->TotalCompressedDataSize += stripe->GetStatistics().CompressedDataSize;
            }
        }));
}

const TChunkStripeListPtr NullStripeList = New<TChunkStripeList>();

PHOENIX_DEFINE_TYPE(TChunkStripeList);

void TPersistentChunkStripeStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, DataWeight);
    Persist(context, RowCount);
    Persist(context, ValueCount);
    Persist(context, MaxBlockSize);
    if (context.GetVersion() >= ESnapshotVersion::MaxCompressedDataSizePerJob) {
        Persist(context, CompressedDataSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
