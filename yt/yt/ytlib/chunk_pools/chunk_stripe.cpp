#include "chunk_stripe.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

namespace NYT::NChunkPools {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe(bool foreign)
    : Foreign(foreign)
{ }

TChunkStripe::TChunkStripe(TLegacyDataSlicePtr dataSlice, bool foreign)
    : Foreign(foreign)
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
    // COMPAT(apollo1321): Remove in 25.3.
    registrar
        .template VirtualField<4>("Solid", [] (TThis* /*this_*/, auto& context) {
            Load<bool>(context);
        })
        .BeforeVersion(ESnapshotVersion::DropSolidFromChunkStripe)();
    PHOENIX_REGISTER_FIELD(5, ChunkListId);
    PHOENIX_REGISTER_FIELD(6, BoundaryKeys);
    PHOENIX_REGISTER_FIELD(7, PartitionTag);
}

PHOENIX_DEFINE_TYPE(TChunkStripe);

////////////////////////////////////////////////////////////////////////////////

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

TChunkStripeStatisticsVector TChunkStripeList::GetPerStripeStatistics() const
{
    TChunkStripeStatisticsVector result;
    result.reserve(Stripes_.size());
    for (const auto& stripe : Stripes_) {
        result.push_back(stripe->GetStatistics());
    }
    return result;
}

TChunkStripeStatistics TChunkStripeList::GetAggregateStatistics() const
{
    TChunkStripeStatistics result = Statistics_;
    if (OverriddenRowCount_) {
        result.RowCount = *OverriddenRowCount_;
        result.DataWeight = *OverriddenDataWeight_;
    }

    return result;
}

void TChunkStripeList::AddStripe(TChunkStripePtr stripe)
{
    Statistics_ += stripe->GetStatistics();
    SliceCount_ += std::ssize(stripe->DataSlices);
    Stripes_.push_back(std::move(stripe));
}

void TChunkStripeList::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Stripes_);
    PHOENIX_REGISTER_FIELD(2, PartitionTag_);
    PHOENIX_REGISTER_FIELD(3, Approximate_);
    PHOENIX_REGISTER_FIELD(4, SliceCount_);
    PHOENIX_REGISTER_FIELD(5, Statistics_);
    PHOENIX_REGISTER_FIELD(6, OverriddenDataWeight_);
    PHOENIX_REGISTER_FIELD(7, OverriddenRowCount_);
}

void TChunkStripeList::Reserve(i64 size)
{
    Stripes_.reserve(size);
}

void TChunkStripeList::SetPartitionTag(int partitionTag)
{
    PartitionTag_ = partitionTag;
    OverriddenDataWeight_.reset();
    OverriddenRowCount_.reset();
}

void TChunkStripeList::SetPartitionTag(int partitionTag, i64 dataWeight, i64 rowCount)
{
    YT_VERIFY(dataWeight >= 0);
    YT_VERIFY(rowCount >= 0);

    PartitionTag_ = partitionTag;
    OverriddenDataWeight_ = dataWeight;
    OverriddenRowCount_ = rowCount;
}

std::optional<int> TChunkStripeList::GetPartitionTag() const
{
    return PartitionTag_;
}

PHOENIX_DEFINE_TYPE(TChunkStripeList);

const TChunkStripeListPtr NullStripeList = New<TChunkStripeList>();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
