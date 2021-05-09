#include "input_chunk.h"

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/query_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NQueryClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TInputChunkBase::TInputChunkBase(const NProto::TChunkSpec& chunkSpec)
    : ChunkId_(GetObjectIdFromDataSplit(chunkSpec))
    , TableIndex_(chunkSpec.table_index())
    , ErasureCodec_(FromProto<NErasure::ECodec>(chunkSpec.erasure_codec()))
    , TableRowIndex_(chunkSpec.table_row_index())
    , RangeIndex_(chunkSpec.range_index())
    , TabletIndex_(chunkSpec.tablet_index())
    , OverrideTimestamp_(chunkSpec.override_timestamp())
{
    SetReplicaList(FromProto<TChunkReplicaList>(chunkSpec.replicas()));

    const auto& chunkMeta = chunkSpec.chunk_meta();
    if (auto miscExt = FindProtoExtension<NProto::TMiscExt>(chunkMeta.extensions())) {
        TotalUncompressedDataSize_ = miscExt->uncompressed_data_size();

        // NB(psushin): we don't use overrides from master, since we can do the same estimates ourself.
        TotalDataWeight_ = miscExt->has_data_weight() && miscExt->data_weight() > 0
            ? miscExt->data_weight()
            : TotalUncompressedDataSize_;

        TotalRowCount_ = miscExt->row_count();

        CompressedDataSize_ = miscExt->compressed_data_size();

        MaxBlockSize_ = miscExt->has_max_block_size()
            ? miscExt->max_block_size()
            : DefaultMaxBlockSize;
        UniqueKeys_ = miscExt->unique_keys();
    }

    if (IsDynamicStore()) {
        // TODO(ifsmirnov): See YT-12212 for reasonable estimates.
        TableChunkFormat_ = ETableChunkFormat::SchemalessHorizontal;
        TotalDataWeight_ = 1;
        TotalRowCount_ = 1;
        CompressedDataSize_ = 1;
        MaxBlockSize_ = DefaultMaxBlockSize;
        UniqueKeys_ = IsSortedDynamicStore();
        TabletId_ = GetTabletIdFromDataSplit(chunkSpec);
    } else {
        YT_VERIFY(FromProto<EChunkType>(chunkMeta.type()) == EChunkType::Table);
        TableChunkFormat_ = CheckedEnumCast<ETableChunkFormat>(chunkMeta.format());
    }
}

TChunkReplicaList TInputChunkBase::GetReplicaList() const
{
    TChunkReplicaList replicas;

    replicas.reserve(MaxInputChunkReplicaCount);
    for (auto replica : Replicas_) {
        if (replica.GetNodeId() != InvalidNodeId) {
            replicas.push_back(replica);
        }
    }
    return replicas;
}

void TInputChunkBase::SetReplicaList(const TChunkReplicaList& replicas)
{
    Replicas_.fill(TChunkReplica());
    for (int index = 0; index < std::ssize(replicas); ++index) {
        auto replica = replicas[index];
        if (ErasureCodec_ == NErasure::ECodec::None) {
            if (index < MaxInputChunkReplicaCount) {
                Replicas_[index] = replica;
            }
        } else {
            int erasureIndex = replica.GetReplicaIndex();
            YT_VERIFY(erasureIndex < MaxInputChunkReplicaCount);
            Replicas_[erasureIndex] = replica;
        }
    }
}

bool TInputChunkBase::IsDynamicStore() const
{
    return IsSortedDynamicStore() || IsOrderedDynamicStore();
}

bool TInputChunkBase::IsSortedDynamicStore() const
{
    return TypeFromId(ChunkId_) == EObjectType::SortedDynamicTabletStore;
}

bool TInputChunkBase::IsOrderedDynamicStore() const
{
    return TypeFromId(ChunkId_) == EObjectType::OrderedDynamicTabletStore;
}

// Intentionally used.
void TInputChunkBase::CheckOffsets()
{
    static_assert(offsetof(TInputChunkBase, ChunkId_) == 0, "invalid offset");
    static_assert(offsetof(TInputChunkBase, Replicas_) == 16, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableIndex_) == 80, "invalid offset");
    static_assert(offsetof(TInputChunkBase, ErasureCodec_) == 84, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableRowIndex_) == 88, "invalid offset");
    static_assert(offsetof(TInputChunkBase, RangeIndex_) == 96, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableChunkFormat_) == 100, "invalid offset");
    static_assert(offsetof(TInputChunkBase, ChunkIndex_) == 104, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TabletIndex_) == 112, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TabletId_) == 120, "invalid offset");
    static_assert(offsetof(TInputChunkBase, OverrideTimestamp_) == 136, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TotalUncompressedDataSize_) == 144, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TotalRowCount_) == 152, "invalid offset");
    static_assert(offsetof(TInputChunkBase, CompressedDataSize_) == 160, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TotalDataWeight_) == 168, "invalid offset");
    static_assert(offsetof(TInputChunkBase, MaxBlockSize_) == 176, "invalid offset");
    static_assert(offsetof(TInputChunkBase, UniqueKeys_) == 184, "invalid offset");
    static_assert(offsetof(TInputChunkBase, ColumnSelectivityFactor_) == 192, "invalid offset");
    static_assert(sizeof(TInputChunkBase) == 200, "invalid sizeof");
}

////////////////////////////////////////////////////////////////////////////////

TInputChunk::TInputChunk(const NProto::TChunkSpec& chunkSpec, std::optional<int> keyColumnCount)
    : TInputChunkBase(chunkSpec)
    , LowerLimit_(chunkSpec.has_lower_limit()
        ? std::make_unique<TLegacyReadLimit>(chunkSpec.lower_limit())
        : nullptr)
    , UpperLimit_(chunkSpec.has_upper_limit()
        ? std::make_unique<TLegacyReadLimit>(chunkSpec.upper_limit())
        : nullptr)
    , BoundaryKeys_(FindBoundaryKeys(chunkSpec.chunk_meta(), keyColumnCount))
    , PartitionsExt_(HasProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions())
        ? std::make_unique<NTableClient::NProto::TPartitionsExt>(
            GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions()))
        : nullptr)
    , HeavyColumnarStatisticsExt_(HasProtoExtension<NTableClient::NProto::THeavyColumnStatisticsExt>(chunkSpec.chunk_meta().extensions())
        ? std::make_unique<NTableClient::NProto::THeavyColumnStatisticsExt>(
            GetProtoExtension<NTableClient::NProto::THeavyColumnStatisticsExt>(chunkSpec.chunk_meta().extensions()))
        : nullptr)
{
    if (IsSortedDynamicStore()) {
        BoundaryKeys_ = std::make_unique<TOwningBoundaryKeys>();
        BoundaryKeys_->MinKey = LowerLimit_ && LowerLimit_->HasLegacyKey() ? LowerLimit_->GetLegacyKey() : MinKey();
        BoundaryKeys_->MaxKey = UpperLimit_ && UpperLimit_->HasLegacyKey() ? UpperLimit_->GetLegacyKey() : MaxKey();
    }

    if (IsOrderedDynamicStore() && UpperLimit_ && UpperLimit_->HasRowIndex())
    {
        i64 lowerLimit = LowerLimit_ && LowerLimit_->HasRowIndex()
            ? LowerLimit_->GetRowIndex()
            : 0;
        TotalRowCount_ = std::max<i64>(0, UpperLimit_->GetRowIndex() - lowerLimit);
        TotalDataWeight_ = std::max(TotalDataWeight_, TotalRowCount_);
    }

    // TODO(max42): remove this after YT-14049.
    if (keyColumnCount && BoundaryKeys_) {
        if (BoundaryKeys_->MinKey.GetCount() > *keyColumnCount) {
            BoundaryKeys_->MinKey = GetKeyPrefix(BoundaryKeys_->MinKey, *keyColumnCount);
        }
        if (BoundaryKeys_->MaxKey.GetCount() > *keyColumnCount) {
            BoundaryKeys_->MaxKey = GetKeyPrefix(BoundaryKeys_->MaxKey, *keyColumnCount);
        }
    }
}

void TInputChunk::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, static_cast<TInputChunkBase&>(*this));
    Persist<TUniquePtrSerializer<>>(context, LowerLimit_);
    Persist<TUniquePtrSerializer<>>(context, UpperLimit_);
    Persist<TUniquePtrSerializer<>>(context, BoundaryKeys_);
    Persist<TUniquePtrSerializer<>>(context, PartitionsExt_);

    // COMPAT(gritukan)
    if (context.GetVersion() >= 300303) {
        Persist<TUniquePtrSerializer<>>(context, HeavyColumnarStatisticsExt_);
    }
}

size_t TInputChunk::SpaceUsed() const
{
    return
       sizeof(*this) +
       (LowerLimit_ ? LowerLimit_->SpaceUsed() : 0) +
       (UpperLimit_ ? UpperLimit_->SpaceUsed() : 0) +
       (BoundaryKeys_ ? BoundaryKeys_->SpaceUsed() : 0) +
       (PartitionsExt_ ? PartitionsExt_->SpaceUsed() : 0) +
       (HeavyColumnarStatisticsExt_ ? HeavyColumnarStatisticsExt_->SpaceUsed() : 0);
}

//! Returns |false| iff the chunk has nontrivial limits.
bool TInputChunk::IsCompleteChunk() const
{
    return
        (!LowerLimit_ || IsTrivial(*LowerLimit_)) &&
        (!UpperLimit_ || IsTrivial(*UpperLimit_));
}

//! Returns |true| iff the chunk is complete and is large enough.
bool TInputChunk::IsLargeCompleteChunk(i64 desiredChunkSize) const
{
    if (!IsCompleteChunk()) {
        return false;
    }

    // ChunkSequenceWriter may actually produce a chunk a bit smaller than desiredChunkSize,
    // so we have to be more flexible here.
    return 0.9 * CompressedDataSize_ >= desiredChunkSize;
}

void TInputChunk::ReleaseBoundaryKeys()
{
    BoundaryKeys_.reset();
}

void TInputChunk::ReleasePartitionsExt()
{
    PartitionsExt_.reset();
}

void TInputChunk::ReleaseHeavyColumnarStatisticsExt()
{
    HeavyColumnarStatisticsExt_.reset();
}

i64 TInputChunk::GetRowCount() const
{
    if (IsOrderedDynamicStore() && (!UpperLimit_ || !UpperLimit_->HasRowIndex())) {
        return 1;
    }

    i64 lowerRowIndex = LowerLimit_ && LowerLimit_->HasRowIndex()
        ? LowerLimit_->GetRowIndex()
        : 0;

    i64 upperRowIndex = UpperLimit_ && UpperLimit_->HasRowIndex()
        ? UpperLimit_->GetRowIndex()
        : TotalRowCount_;

    auto rowCount = std::max(0l, upperRowIndex - lowerRowIndex);
    YT_VERIFY(rowCount <= TotalRowCount_);
    return rowCount;
}

i64 TInputChunk::GetDataWeight() const
{
    auto rowCount = GetRowCount();
    auto rowSelectivityFactor = static_cast<double>(rowCount) / TotalRowCount_;
    return std::max<i64>(std::ceil(TotalDataWeight_ * ColumnSelectivityFactor_ * rowSelectivityFactor), rowCount);
}

i64 TInputChunk::GetUncompressedDataSize() const
{
    return ApplySelectivityFactors(TotalUncompressedDataSize_);
}

i64 TInputChunk::GetCompressedDataSize() const
{
    return ApplySelectivityFactors(CompressedDataSize_);
}

i64 TInputChunk::ApplySelectivityFactors(i64 dataSize) const
{
    auto rowCount = GetRowCount();
    auto rowSelectivityFactor = static_cast<double>(rowCount) / TotalRowCount_;
    i64 result;
    if (TableChunkFormat_ == ETableChunkFormat::UnversionedColumnar ||
        TableChunkFormat_ == ETableChunkFormat::VersionedColumnar)
    {
        result = std::ceil(dataSize * ColumnSelectivityFactor_ * rowSelectivityFactor);
    } else {
        result = std::ceil(dataSize * rowSelectivityFactor);
    }
    result = std::max<i64>(result, rowCount);
    return std::max<i64>(result, 1);
}

////////////////////////////////////////////////////////////////////////////////

//! ToProto is used to pass chunk specs to job proxy as part of TTableInputSpec.
void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkPtr& inputChunk)
{
    SetObjectId(chunkSpec, inputChunk->GetChunkId());

    ToProto(chunkSpec->mutable_replicas(), inputChunk->GetReplicaList());

    if (inputChunk->TableIndex_ >= 0) {
        chunkSpec->set_table_index(inputChunk->TableIndex_);
    }

    if (inputChunk->ErasureCodec_ != NErasure::ECodec::None) {
        chunkSpec->set_erasure_codec(ToProto<int>(inputChunk->ErasureCodec_));
    }

    if (inputChunk->TableRowIndex_ > 0) {
        chunkSpec->set_table_row_index(inputChunk->TableRowIndex_);
    }

    if (inputChunk->RangeIndex_ > 0) {
        chunkSpec->set_range_index(inputChunk->RangeIndex_);
    }

    if (inputChunk->ChunkIndex_ > 0) {
        chunkSpec->set_chunk_index(inputChunk->ChunkIndex_);
    }

    if (inputChunk->TabletIndex_ >= 0) {
        chunkSpec->set_tablet_index(inputChunk->TabletIndex_);
    }

    if (inputChunk->OverrideTimestamp_) {
        chunkSpec->set_override_timestamp(inputChunk->OverrideTimestamp_);
    }

    if (inputChunk->LowerLimit_) {
        ToProto(chunkSpec->mutable_lower_limit(), *inputChunk->LowerLimit_);
    }
    if (inputChunk->UpperLimit_) {
        ToProto(chunkSpec->mutable_upper_limit(), *inputChunk->UpperLimit_);
    }

    chunkSpec->mutable_chunk_meta()->set_type(ToProto<int>(EChunkType::Table));
    chunkSpec->mutable_chunk_meta()->set_format(ToProto<int>(inputChunk->TableChunkFormat_));
    chunkSpec->mutable_chunk_meta()->mutable_extensions();
}

TString ToString(const TInputChunkPtr& inputChunk)
{
    TString boundaryKeys;
    if (inputChunk->BoundaryKeys()) {
        boundaryKeys = Format(
            "MinKey: %v, MaxKey: %v",
            inputChunk->BoundaryKeys()->MinKey,
            inputChunk->BoundaryKeys()->MaxKey);
    }

    return Format(
        "{ChunkId: %v, Replicas: %v, TableIndex: %v, ErasureCodec: %v, TableRowIndex: %v, "
        "RangeIndex: %v, ChunkIndex: %v, TabletIndex: %v, TableChunkFormat: %v, UncompressedDataSize: %v, RowCount: %v, "
        "CompressedDataSize: %v, DataWeight: %v, MaxBlockSize: %v, LowerLimit: %v, UpperLimit: %v, "
        "BoundaryKeys: {%v}, PartitionsExt: {%v}}",
        inputChunk->GetChunkId(),
        inputChunk->GetReplicaList(),
        inputChunk->GetTableIndex(),
        inputChunk->GetErasureCodec(),
        inputChunk->GetTableRowIndex(),
        inputChunk->GetRangeIndex(),
        inputChunk->GetChunkIndex(),
        inputChunk->GetTabletIndex(),
        inputChunk->GetTableChunkFormat(),
        inputChunk->GetUncompressedDataSize(),
        inputChunk->GetRowCount(),
        inputChunk->GetCompressedDataSize(),
        inputChunk->GetDataWeight(),
        inputChunk->GetMaxBlockSize(),
        inputChunk->LowerLimit() ? std::make_optional(*inputChunk->LowerLimit()) : std::nullopt,
        inputChunk->UpperLimit() ? std::make_optional(*inputChunk->UpperLimit()) : std::nullopt,
        inputChunk->BoundaryKeys() ? boundaryKeys : "",
        inputChunk->PartitionsExt() ? inputChunk->PartitionsExt()->ShortDebugString() : "");
}

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(const TInputChunkPtr& inputChunk, bool checkParityParts)
{
    if (inputChunk->IsDynamicStore()) {
        // It is up to the reader to locate the dynamic store.
        return false;
    }
    return IsUnavailable(inputChunk->GetReplicaList(), inputChunk->GetErasureCodec(), checkParityParts);
}

TChunkId EncodeChunkId(const TInputChunkPtr& inputChunk, TNodeId nodeId)
{
    auto replicaIt = std::find_if(
        inputChunk->Replicas().begin(),
        inputChunk->Replicas().end(),
        [=] (TChunkReplica replica) {
            return static_cast<TNodeId>(replica.GetNodeId()) == nodeId;
        });
    YT_VERIFY(replicaIt != inputChunk->Replicas().end());

    TChunkIdWithIndexes chunkIdWithIndexes(
        inputChunk->GetChunkId(),
        replicaIt->GetReplicaIndex(),
        0 /* mediumIndex */);
    return EncodeChunkId(chunkIdWithIndexes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
