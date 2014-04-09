#include "stdafx.h"
#include "chunk_spec.h"
#include "key.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"

#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <core/ytree/attribute_helpers.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TRefCountedChunkSpec::TRefCountedChunkSpec()
{ }

TRefCountedChunkSpec::TRefCountedChunkSpec(const TChunkSpec& other)
{
    CopyFrom(other);
}

TRefCountedChunkSpec::TRefCountedChunkSpec(TChunkSpec&& other)
{
    Swap(&other);
}

TRefCountedChunkSpec::TRefCountedChunkSpec(const TRefCountedChunkSpec& other)
{
    CopyFrom(other);
}

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice(const TChunkSlice& other)
{
    ChunkSpec = other.ChunkSpec;
    PartIndex = other.PartIndex;
    StartLimit.CopyFrom(other.StartLimit);
    EndLimit.CopyFrom(other.EndLimit);
    SizeOverrideExt.CopyFrom(other.SizeOverrideExt);
}

TChunkSlice::TChunkSlice()
    : PartIndex(DefaultPartIndex)
{ }

std::vector<TChunkSlicePtr> TChunkSlice::SliceEvenly(i64 sliceDataSize) const
{
    YCHECK(sliceDataSize > 0);

    std::vector<TChunkSlicePtr> result;

    i64 dataSize = GetDataSize();
    i64 rowCount = GetRowCount();

    // Inclusive.
    i64 startRowIndex = StartLimit.has_row_index() ? StartLimit.row_index() : 0;
    i64 endRowIndex = EndLimit.has_row_index() ? EndLimit.row_index() : rowCount;

    rowCount = endRowIndex - startRowIndex;
    int count = std::ceil((double)dataSize / (double)sliceDataSize);

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto chunkSlice = New<TChunkSlice>(*this);
            chunkSlice->StartLimit.set_row_index(sliceStartRowIndex);
            chunkSlice->EndLimit.set_row_index(sliceEndRowIndex);
            chunkSlice->SizeOverrideExt.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            chunkSlice->SizeOverrideExt.set_uncompressed_data_size((dataSize + count - 1) / count);

            result.push_back(chunkSlice);
        }
    }

    return result;
}

i64 TChunkSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataSize();

    if (PartIndex == DefaultPartIndex) {
        // For erasure chunks without specified part index,
        // data size is assumed to be split evenly between data parts.
        auto codecId = NErasure::ECodec(ChunkSpec->erasure_codec());
        if (codecId != NErasure::ECodec::None) {
            auto* codec = NErasure::GetCodec(codecId);
            int dataPartCount = codec->GetDataPartCount();
            result = (result + dataPartCount - 1) / dataPartCount;
        }
    } else if (PartIndex != replicaPartIndex) {
        result = 0;
    }

    return result;
}

TRefCountedChunkSpecPtr TChunkSlice::GetChunkSpec() const
{
    return ChunkSpec;
}

i64 TChunkSlice::GetMaxBlockSize() const
{
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkSpec->extensions());
    return miscExt.max_block_size();
}

i64 TChunkSlice::GetDataSize() const
{
    return SizeOverrideExt.uncompressed_data_size();
}

i64 TChunkSlice::GetRowCount() const
{
    return SizeOverrideExt.row_count();
}

void TChunkSlice::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkSpec);
    Persist(context, PartIndex);
    Persist(context, StartLimit);
    Persist(context, EndLimit);
    Persist(context, SizeOverrideExt);
}

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    auto result = New<TChunkSlice>();
    result->ChunkSpec = chunkSpec;
    result->SizeOverrideExt.set_uncompressed_data_size(dataSize);
    result->SizeOverrideExt.set_row_count(rowCount);
    result->PartIndex = DefaultPartIndex;

    if (chunkSpec->has_start_limit()) {
        result->StartLimit.CopyFrom(chunkSpec->start_limit());
    }

    if (chunkSpec->has_end_limit()) {
        result->EndLimit.CopyFrom(chunkSpec->end_limit());
    }

    if (startKey && (!result->StartLimit.has_key() || result->StartLimit.key() < startKey.Get())) {
        *result->StartLimit.mutable_key() = startKey.Get();
    }

    if (endKey && (!result->EndLimit.has_key() || result->EndLimit.key() > endKey.Get())) {
        *result->EndLimit.mutable_key() = endKey.Get();
    }

    return result;
}

std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    std::vector<TChunkSlicePtr> slices;
    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceStartRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceEndRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TChunkSlice>();
            slicedChunk->ChunkSpec = chunkSpec;
            slicedChunk->PartIndex = partIndex;
            slicedChunk->StartLimit.set_row_index(sliceStartRowIndex);
            slicedChunk->EndLimit.set_row_index(sliceEndRowIndex);
            slicedChunk->SizeOverrideExt.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            slicedChunk->SizeOverrideExt.set_uncompressed_data_size((dataSize + dataPartCount - 1) / dataPartCount);

            slices.push_back(slicedChunk);
        }
    }
    return slices;
}

void ToProto(TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice)
{
    chunkSpec->CopyFrom(*chunkSlice.ChunkSpec);
    if (IsNontrivial(chunkSlice.StartLimit)) {
        chunkSpec->mutable_start_limit()->CopyFrom(chunkSlice.StartLimit);
    }

    if (IsNontrivial(chunkSlice.EndLimit)) {
        chunkSpec->mutable_end_limit()->CopyFrom(chunkSlice.EndLimit);
    }

    SetProtoExtension(chunkSpec->mutable_extensions(), chunkSlice.SizeOverrideExt);
}

bool IsNontrivial(const TReadLimit& limit)
{
    return
        limit.has_row_index() ||
        limit.has_key() ||
        limit.has_chunk_index() ||
        limit.has_offset();
}

bool IsTrivial(const TReadLimit& limit)
{
    return !IsNontrivial(limit);
}

bool IsUnavailable(const TChunkReplicaList& replicas, NErasure::ECodec codecId, bool checkParityParts)
{
    if (codecId == NErasure::ECodec::None) {
        return replicas.empty();
    } else {
        auto* codec = NErasure::GetCodec(codecId);
        int partCount = checkParityParts ? codec->GetTotalPartCount() : codec->GetDataPartCount();
        NErasure::TPartIndexSet missingIndexSet((1 << partCount) - 1);
        FOREACH (auto replica, replicas) {
            missingIndexSet.reset(replica.GetIndex());
        }
        return missingIndexSet.any();
    }
}

bool IsUnavailable(const NProto::TChunkSpec& chunkSpec, bool checkParityParts)
{
    auto codecId = NErasure::ECodec(chunkSpec.erasure_codec());
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());
    return IsUnavailable(replicas, codecId, checkParityParts);
}

void GetStatistics(
    const TChunkSpec& chunkSpec,
    i64* dataSize,
    i64* rowCount,
    i64* valueCount)
{
    auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.extensions());
    auto sizeOverrideExt = FindProtoExtension<TSizeOverrideExt>(chunkSpec.extensions());

    if (sizeOverrideExt) {
        if (dataSize) {
            *dataSize = sizeOverrideExt->uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = sizeOverrideExt->row_count();
        }
    } else {
        if (dataSize) {
            *dataSize = miscExt.uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = miscExt.row_count();
        }
    }

    if (valueCount) {
        *valueCount = miscExt.value_count();
    }
}

TRefCountedChunkSpecPtr CreateCompleteChunk(TRefCountedChunkSpecPtr chunkSpec)
{
    auto result = New<TRefCountedChunkSpec>(*chunkSpec);
    result->clear_start_limit();
    result->clear_end_limit();

    RemoveProtoExtension<TSizeOverrideExt>(result->mutable_extensions());

    return result;
}

TChunkId EncodeChunkId(
    const TChunkSpec& chunkSpec,
    NNodeTrackerClient::TNodeId nodeId)
{
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());
    auto replicaIt = std::find_if(
        replicas.begin(),
        replicas.end(),
        [=] (TChunkReplica replica) {
            return replica.GetNodeId() == nodeId;
        });
    YCHECK(replicaIt != replicas.end());

    TChunkIdWithIndex chunkIdWithIndex(
        NYT::FromProto<TChunkId>(chunkSpec.chunk_id()),
        replicaIt->GetIndex());
    return EncodeChunkId(chunkIdWithIndex);
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes)
{
    return !attributes.Get<bool>("append", false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
