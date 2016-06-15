#include "input_chunk.h"

#include <yt/core/erasure/codec.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NNodeTrackerClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const i64 DefaultMaxBlockSize = (i64)16 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TInputChunkBase::TInputChunkBase(
    const NProto::TChunkSpec& chunkSpec)
    : TInputChunkBase(
        FromProto<TChunkId>(chunkSpec.chunk_id()),
        FromProto<TChunkReplicaList>(chunkSpec.replicas()),
        chunkSpec.chunk_meta(),
        NErasure::ECodec(chunkSpec.erasure_codec()))
{
    TableRowIndex_ = chunkSpec.table_row_index();
    RangeIndex_ = chunkSpec.range_index();
}

TInputChunkBase::TInputChunkBase(
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    NErasure::ECodec erasureCodec)
    : ChunkId_(chunkId)
    , ErasureCodec_(erasureCodec)
{
    SetReplicaList(replicas);

    auto miscExt = GetProtoExtension<NProto::TMiscExt>(chunkMeta.extensions());
    auto sizeOverrideExt = FindProtoExtension<NProto::TSizeOverrideExt>(chunkMeta.extensions());

    if (sizeOverrideExt) {
        UncompressedDataSize_ = sizeOverrideExt->uncompressed_data_size();
        RowCount_ = sizeOverrideExt->row_count();
    } else {
        UncompressedDataSize_ = miscExt.uncompressed_data_size();
        RowCount_ = miscExt.row_count();
    }
    CompressedDataSize_ = miscExt.compressed_data_size();
    if (miscExt.has_max_block_size()) {
        MaxBlockSize_ = miscExt.max_block_size();
    } else {
        MaxBlockSize_ = DefaultMaxBlockSize;
    }

    UniqueKeys_ = miscExt.unique_keys();

    YCHECK(EChunkType(chunkMeta.type()) == EChunkType::Table);
    TableChunkFormat_ = ETableChunkFormat(chunkMeta.version());
}

TChunkReplicaList TInputChunkBase::GetReplicaList() const
{
    TChunkReplicaList replicas;

    replicas.reserve(InputChunkReplicaCount);
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
            if (index < InputChunkReplicaCount) {
                Replicas_[index] = replica;
            }
        } else {
            int erasureIndex = replica.GetIndex();
            YCHECK(erasureIndex < InputChunkReplicaCount);
            Replicas_[erasureIndex] = replica;
        }
    }
}

void TInputChunkBase::Save(NPhoenix::TSaveContext& context) const
{
    // TInputChunkBase is persisted as one block, but we should be insured in offsets of all fields.
    static_assert(offsetof(TInputChunkBase, ChunkId_) == 0, "invalid offset");
    static_assert(offsetof(TInputChunkBase, Replicas_) == 16, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableIndex_) == 80, "invalid offset");
    static_assert(offsetof(TInputChunkBase, ErasureCodec_) == 84, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableRowIndex_) == 88, "invalid offset");
    static_assert(offsetof(TInputChunkBase, RangeIndex_) == 96, "invalid offset");
    static_assert(offsetof(TInputChunkBase, TableChunkFormat_) == 100, "invalid offset");
    static_assert(offsetof(TInputChunkBase, UncompressedDataSize_) == 104, "invalid offset");
    static_assert(offsetof(TInputChunkBase, RowCount_) == 112, "invalid offset");
    static_assert(offsetof(TInputChunkBase, CompressedDataSize_) == 120, "invalid offset");
    static_assert(offsetof(TInputChunkBase, MaxBlockSize_) == 128, "invalid offset");
    static_assert(offsetof(TInputChunkBase, UniqueKeys_) == 136, "invalid offset");
    static_assert(sizeof(TInputChunkBase) == 144, "invalid sizeof");
    NYT::TRangeSerializer::Save(context, TRef(reinterpret_cast<const void*>(this), sizeof(*this)));
}

void TInputChunkBase::Load(NPhoenix::TLoadContext& context)
{
    auto ref = TMutableRef(reinterpret_cast<void*>(this), sizeof(*this));
    NYT::TRangeSerializer::Load(context, ref);
}

////////////////////////////////////////////////////////////////////////////////

TInputChunk::TInputChunk(const NProto::TChunkSpec& chunkSpec)
    : TInputChunkBase(chunkSpec)
    , BoundaryKeys_(GetBoundaryKeys(chunkSpec.chunk_meta()))
{
    if (chunkSpec.has_lower_limit()) {
        LowerLimit_ = std::make_unique<NProto::TReadLimit>(chunkSpec.lower_limit());
    }
    if (chunkSpec.has_upper_limit()) {
        UpperLimit_ = std::make_unique<NProto::TReadLimit>(chunkSpec.upper_limit());
    }

    if (chunkSpec.has_channel()) {
        Channel_ = std::make_unique<NProto::TChannel>(chunkSpec.channel());
    }
    if (HasProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions())) {
        PartitionsExt_ = std::make_unique<NTableClient::NProto::TPartitionsExt>(
            GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions()));
    }
}

TInputChunk::TInputChunk(NProto::TChunkSpec&& chunkSpec)
    : TInputChunkBase(chunkSpec)
    , LowerLimit_(chunkSpec.release_lower_limit())
    , UpperLimit_(chunkSpec.release_upper_limit())
    , BoundaryKeys_(GetBoundaryKeys(chunkSpec.chunk_meta()))
    , Channel_(chunkSpec.release_channel())
{
    if (HasProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions())) {
        PartitionsExt_ = std::make_unique<NTableClient::NProto::TPartitionsExt>(
            GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunkSpec.chunk_meta().extensions()));
    }
}

TInputChunk::TInputChunk(
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TOwningKey& lowerLimit,
    const TOwningKey& upperLimit,
    NErasure::ECodec erasureCodec)
    : TInputChunkBase(chunkId, replicas, chunkMeta, erasureCodec)
    , BoundaryKeys_(GetBoundaryKeys(chunkMeta))
{
    LowerLimit_.reset(new NProto::TReadLimit);
    ToProto(LowerLimit_->mutable_key(), lowerLimit);
    UpperLimit_.reset(new NProto::TReadLimit);
    ToProto(UpperLimit_->mutable_key(), upperLimit);
}

void TInputChunk::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, static_cast<TInputChunkBase&>(*this));
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, BoundaryKeys_);
    Persist(context, Channel_);
    Persist(context, PartitionsExt_);
}

size_t TInputChunk::SpaceUsed() const
{
    return
       sizeof(*this) +
       (LowerLimit_ ? LowerLimit_->SpaceUsed() : 0) +
       (UpperLimit_ ? UpperLimit_->SpaceUsed() : 0) +
       (BoundaryKeys_ ? BoundaryKeys_->SpaceUsed() : 0) +
       (Channel_ ? Channel_->SpaceUsed() : 0) +
       (PartitionsExt_ ? PartitionsExt_->SpaceUsed() : 0);
}

//! Returns |false| iff the chunk has nontrivial limits.
bool TInputChunk::IsCompleteChunk() const
{
    return (!LowerLimit_ || IsTrivial(*LowerLimit_)) &&
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
    BoundaryKeys_.release();
}

//! Release memory occupied by PartitionsExt
void TInputChunk::ReleasePartitionsExt()
{
    PartitionsExt_.release();
}


////////////////////////////////////////////////////////////////////////////////

//! ToProto is used to pass chunk specs to job proxy as part of TUserJobSpecExt
void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkPtr& inputChunk)
{
    ToProto(chunkSpec->mutable_chunk_id(), inputChunk->ChunkId_);
    const auto& replicas = inputChunk->GetReplicaList();
    ToProto(chunkSpec->mutable_replicas(), replicas);
    chunkSpec->set_table_index(inputChunk->TableIndex_);
    chunkSpec->set_erasure_codec(static_cast<int>(inputChunk->ErasureCodec_));
    chunkSpec->set_table_row_index(inputChunk->TableRowIndex_);
    chunkSpec->set_range_index(inputChunk->RangeIndex_);
    if (inputChunk->LowerLimit_) {
        ToProto(chunkSpec->mutable_lower_limit(), *inputChunk->LowerLimit_);
    }
    if (inputChunk->UpperLimit_) {
        ToProto(chunkSpec->mutable_upper_limit(), *inputChunk->UpperLimit_);
    }
    if (inputChunk->Channel_) {
        chunkSpec->mutable_channel()->CopyFrom(*inputChunk->Channel_);
    }
    chunkSpec->mutable_chunk_meta()->set_type(static_cast<int>(EChunkType::Table));
    chunkSpec->mutable_chunk_meta()->set_version(static_cast<int>(inputChunk->TableChunkFormat_));
    chunkSpec->mutable_chunk_meta()->mutable_extensions();
}

Stroka ToString(const TInputChunkPtr& inputChunk)
{
    Stroka boundaryKeys;
    if (inputChunk->BoundaryKeys()) {
        boundaryKeys = Format(
            "MinKey: %v, MaxKey: %v",
            inputChunk->BoundaryKeys()->MinKey,
            inputChunk->BoundaryKeys()->MaxKey);
    }
    return Format(
        "{ChunkId: %v, Replicas: %v, TableIndex: %v, ErasureCodec: %v, TableRowIndex: %v, "
        "RangeIndex: %v, TableChunkFormat: %v, UncompressedDataSize: %v, RowCount: %v, "
        "CompressedDataSize: %v, MaxBlockSize: %v, LowerLimit: {%v}, UpperLimit: {%v}, "
        "BoundaryKeys: {%v}, Channel: {%v}, PartitionsExt: {%v}}",
        inputChunk->ChunkId(),
        JoinToString(inputChunk->Replicas()),
        inputChunk->GetTableIndex(),
        inputChunk->GetErasureCodec(),
        inputChunk->GetTableRowIndex(),
        inputChunk->GetRangeIndex(),
        inputChunk->GetTableChunkFormat(),
        inputChunk->GetUncompressedDataSize(),
        inputChunk->GetRowCount(),
        inputChunk->GetCompressedDataSize(),
        inputChunk->GetMaxBlockSize(),
        inputChunk->LowerLimit() ? inputChunk->LowerLimit()->ShortDebugString() : "",
        inputChunk->UpperLimit() ? inputChunk->UpperLimit()->ShortDebugString() : "",
        inputChunk->BoundaryKeys() ? boundaryKeys : "",
        inputChunk->Channel() ? inputChunk->Channel()->ShortDebugString() : "",
        inputChunk->PartitionsExt() ? inputChunk->PartitionsExt()->ShortDebugString() : "");
}

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(TInputChunkPtr inputChunk, bool checkParityParts)
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

    TChunkIdWithIndex chunkIdWithIndex(
        inputChunk->ChunkId(),
        replicaIt->GetIndex());
    return EncodeChunkId(chunkIdWithIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
