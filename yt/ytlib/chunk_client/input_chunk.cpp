#include "input_chunk.h"

#include <yt/core/erasure/codec.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/core/misc/numeric_helpers.h>


namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NNodeTrackerClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TInputChunkBase::TInputChunkBase(const NProto::TChunkSpec& chunkSpec)
    : ChunkId_(FromProto<TChunkId>(chunkSpec.chunk_id()))
    , TableIndex_(chunkSpec.table_index())
    , ErasureCodec_(NErasure::ECodec(chunkSpec.erasure_codec()))
    , TableRowIndex_(chunkSpec.table_row_index())
    , RangeIndex_(chunkSpec.range_index())
{
    SetReplicaList(FromProto<TChunkReplicaList>(chunkSpec.replicas()));

    const auto& chunkMeta = chunkSpec.chunk_meta();
    auto miscExt = GetProtoExtension<NProto::TMiscExt>(chunkMeta.extensions());

    TotalUncompressedDataSize_ = miscExt.uncompressed_data_size();

    // NB(psushin): we don't use overrides from master, since we can do the same estimates ourself.
    TotalDataWeight_ = miscExt.has_data_weight() && miscExt.data_weight() > 0
        ? miscExt.data_weight()
        : TotalUncompressedDataSize_;

    TotalRowCount_ = miscExt.row_count();

    CompressedDataSize_ = miscExt.compressed_data_size();

    MaxBlockSize_ = miscExt.has_max_block_size()
        ? miscExt.max_block_size()
        : DefaultMaxBlockSize;
    UniqueKeys_ = miscExt.unique_keys();

    YCHECK(EChunkType(chunkMeta.type()) == EChunkType::Table);
    TableChunkFormat_ = ETableChunkFormat(chunkMeta.version());
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
    for (int index = 0; index < replicas.size(); ++index) {
        auto replica = replicas[index];
        if (ErasureCodec_ == NErasure::ECodec::None) {
            if (index < MaxInputChunkReplicaCount) {
                Replicas_[index] = replica;
            }
        } else {
            int erasureIndex = replica.GetReplicaIndex();
            YCHECK(erasureIndex < MaxInputChunkReplicaCount);
            Replicas_[erasureIndex] = replica;
        }
    }
}

// Workaround for TSerializationDumpPodWriter.
TString ToString(const TInputChunkBase&)
{
    Y_UNREACHABLE();
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
    static_assert(offsetof(TInputChunkBase, ChunkIndex_) == 104, "invalid offsetof");
    static_assert(offsetof(TInputChunkBase, TotalUncompressedDataSize_) == 112, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TotalRowCount_) == 120, "invalid offset");
    static_assert(offsetof(TInputChunkBase, CompressedDataSize_) == 128, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TotalDataWeight_) == 136, "invalid offset");
    static_assert(offsetof(TInputChunkBase, MaxBlockSize_) == 144, "invalid offset");
    static_assert(offsetof(TInputChunkBase, UniqueKeys_) == 152, "invalid offset");
    static_assert(offsetof(TInputChunkBase, ColumnSelectivityFactor_) == 160, "invalid offset");
    static_assert(sizeof(TInputChunkBase) == 168, "invalid sizeof");
}

////////////////////////////////////////////////////////////////////////////////

TInputChunk::TInputChunk(const NProto::TChunkSpec& chunkSpec)
    : TInputChunkBase(chunkSpec)
    , LowerLimit_(chunkSpec.has_lower_limit()
        ? std::make_unique<TReadLimit>(chunkSpec.lower_limit())
        : nullptr)
    , UpperLimit_(chunkSpec.has_upper_limit()
        ? std::make_unique<TReadLimit>(chunkSpec.upper_limit())
        : nullptr)
    , BoundaryKeys_(FindBoundaryKeys(chunkSpec.chunk_meta()))
    , PartitionsExt_(HasProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions())
        ? std::make_unique<NTableClient::NProto::TPartitionsExt>(
            GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions()))
        : nullptr)
{ }

void TInputChunk::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, static_cast<TInputChunkBase&>(*this));
    Persist<TUniquePtrSerializer<>>(context, LowerLimit_);
    Persist<TUniquePtrSerializer<>>(context, UpperLimit_);
    Persist<TUniquePtrSerializer<>>(context, BoundaryKeys_);
    Persist<TUniquePtrSerializer<>>(context, PartitionsExt_);
}

size_t TInputChunk::SpaceUsed() const
{
    return
       sizeof(*this) +
       (LowerLimit_ ? LowerLimit_->SpaceUsed() : 0) +
       (UpperLimit_ ? UpperLimit_->SpaceUsed() : 0) +
       (BoundaryKeys_ ? BoundaryKeys_->SpaceUsed() : 0) +
       (PartitionsExt_ ? PartitionsExt_->SpaceUsed() : 0);
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

//! Release memory occupied by BoundaryKeys
void TInputChunk::ReleaseBoundaryKeys()
{
    BoundaryKeys_.reset();
}

//! Release memory occupied by PartitionsExt
void TInputChunk::ReleasePartitionsExt()
{
    PartitionsExt_.reset();
}

i64 TInputChunk::GetRowCount() const
{
    i64 lowerRowIndex = LowerLimit_ && LowerLimit_->HasRowIndex()
        ? LowerLimit_->GetRowIndex()
        : 0;

    i64 upperRowIndex = UpperLimit_ && UpperLimit_->HasRowIndex()
        ? UpperLimit_->GetRowIndex()
        : TotalRowCount_;

    auto rowCount = std::max(0l, upperRowIndex - lowerRowIndex);
    YCHECK(rowCount <= TotalRowCount_);
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
    auto rowCount = GetRowCount();
    auto rowSelectivityFactor = static_cast<double>(rowCount) / TotalRowCount_;
    i64 result;
    if (TableChunkFormat_ == ETableChunkFormat::UnversionedColumnar ||
        TableChunkFormat_ == ETableChunkFormat::VersionedColumnar)
    {
        result = std::ceil(TotalUncompressedDataSize_ * ColumnSelectivityFactor_ * rowSelectivityFactor);
    } else {
        result = std::ceil(TotalUncompressedDataSize_ * rowSelectivityFactor);
    }
    result = std::max<i64>(result, 1);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! ToProto is used to pass chunk specs to job proxy as part of TTableInputSpec.
void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkPtr& inputChunk, EDataSourceType dataSourceType)
{
    ToProto(chunkSpec->mutable_chunk_id(), inputChunk->ChunkId_);
    const auto& replicas = inputChunk->GetReplicaList();
    ToProto(chunkSpec->mutable_replicas(), replicas);
    if (inputChunk->TableIndex_ >= 0) {
        chunkSpec->set_table_index(inputChunk->TableIndex_);
    }

    if (inputChunk->ErasureCodec_ != NErasure::ECodec::None) {
        chunkSpec->set_erasure_codec(static_cast<int>(inputChunk->ErasureCodec_));
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

    if (inputChunk->LowerLimit_) {
        ToProto(chunkSpec->mutable_lower_limit(), *inputChunk->LowerLimit_);
    }
    if (inputChunk->UpperLimit_) {
        ToProto(chunkSpec->mutable_upper_limit(), *inputChunk->UpperLimit_);
    }

    // This is the default intermediate table chunk format,
    // so we omit chunk_meta altogether to minimize job spec size.
    if (inputChunk->TableChunkFormat_ != ETableChunkFormat::SchemalessHorizontal || dataSourceType != EDataSourceType::UnversionedTable) {
        chunkSpec->mutable_chunk_meta()->set_type(static_cast<int>(EChunkType::Table));
        chunkSpec->mutable_chunk_meta()->set_version(static_cast<int>(inputChunk->TableChunkFormat_));
        chunkSpec->mutable_chunk_meta()->mutable_extensions();
    }
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
        "RangeIndex: %v, ChunkIndex: %v, TableChunkFormat: %v, UncompressedDataSize: %v, RowCount: %v, "
        "CompressedDataSize: %v, DataWeight: %v, MaxBlockSize: %v, LowerLimit: %v, UpperLimit: %v, "
        "BoundaryKeys: {%v}, PartitionsExt: {%v}}",
        inputChunk->ChunkId(),
        JoinToString(inputChunk->Replicas()),
        inputChunk->GetTableIndex(),
        inputChunk->GetErasureCodec(),
        inputChunk->GetTableRowIndex(),
        inputChunk->GetRangeIndex(),
        inputChunk->GetChunkIndex(),
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
    return IsUnavailable(inputChunk->GetReplicaList(), inputChunk->GetErasureCodec(), checkParityParts);
}

TChunkId EncodeChunkId(const TInputChunkPtr& inputChunk, TNodeId nodeId)
{
    auto replicaIt = std::find_if(
        inputChunk->Replicas().begin(),
        inputChunk->Replicas().end(),
        [=] (TChunkReplica replica) {
            return replica.GetNodeId() == nodeId;
        });
    YCHECK(replicaIt != inputChunk->Replicas().end());

    TChunkIdWithIndexes chunkIdWithIndexes(
        inputChunk->ChunkId(),
        replicaIt->GetReplicaIndex(),
        replicaIt->GetMediumIndex());
    return EncodeChunkId(chunkIdWithIndexes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
