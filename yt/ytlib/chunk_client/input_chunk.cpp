#include "stdafx.h"
#include "input_chunk.h"
#include "key.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/erasure/codec.h>

#include <ytlib/ytree/attribute_helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TRefCountedInputChunk::TRefCountedInputChunk(const TInputChunk& other)
{
    CopyFrom(other);
}

TRefCountedInputChunk::TRefCountedInputChunk(TInputChunk&& other)
{
    Swap(&other);
}

TRefCountedInputChunk::TRefCountedInputChunk(const TRefCountedInputChunk& other)
{
    CopyFrom(other);
}

////////////////////////////////////////////////////////////////////////////////

TInputChunkSlice::TInputChunkSlice(const TInputChunkSlice& other)
{
    InputChunk = other.InputChunk;
    PartIndex = other.PartIndex;
    StartLimit.CopyFrom(other.StartLimit);
    EndLimit.CopyFrom(other.EndLimit);
    SizeOverrideExt.CopyFrom(other.SizeOverrideExt);
}

TInputChunkSlice::TInputChunkSlice()
    : PartIndex(DefaultPartIndex)
{ }

std::vector<TInputChunkSlicePtr> TInputChunkSlice::SliceEvenly(i64 sliceDataSize) const
{
    YCHECK(sliceDataSize > 0);

    std::vector<TInputChunkSlicePtr> result;

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
            auto chunkSlice = New<TInputChunkSlice>(*this);
            chunkSlice->StartLimit.set_row_index(sliceStartRowIndex);
            chunkSlice->EndLimit.set_row_index(sliceEndRowIndex);
            chunkSlice->SizeOverrideExt.set_row_count(rowCount);
            chunkSlice->SizeOverrideExt.set_uncompressed_data_size((dataSize + count - 1) / count);

            result.push_back(chunkSlice);
        }
    }

    return result;
}

i64 TInputChunkSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataSize();

    if (PartIndex == DefaultPartIndex) {
        // For erasure chunks without specified part index,
        // data size is assumed to be split evenly between data parts.
        auto codecId = NErasure::ECodec(InputChunk->erasure_codec());
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

TRefCountedInputChunkPtr TInputChunkSlice::GetInputChunk() const
{
    return InputChunk;
}

i64 TInputChunkSlice::GetDataSize() const
{
    return SizeOverrideExt.uncompressed_data_size();
}

i64 TInputChunkSlice::GetRowCount() const
{
    return SizeOverrideExt.row_count();
}

TInputChunkSlicePtr CreateChunkSlice(
    TRefCountedInputChunkPtr inputChunk,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*inputChunk, &dataSize, &rowCount);

    auto result = New<TInputChunkSlice>();
    result->InputChunk = inputChunk;
    result->SizeOverrideExt.set_uncompressed_data_size(dataSize);
    result->SizeOverrideExt.set_row_count(rowCount);
    result->PartIndex = DefaultPartIndex;

    if (inputChunk->has_start_limit()) {
        result->StartLimit.CopyFrom(inputChunk->start_limit());
    }

    if (inputChunk->has_end_limit()) {
        result->EndLimit.CopyFrom(inputChunk->end_limit());
    }

    if (startKey && (!result->StartLimit.has_key() || result->StartLimit.key() < startKey.Get())) {
        *result->StartLimit.mutable_key() = startKey.Get();
    }

    if (endKey && (!result->EndLimit.has_key() || result->EndLimit.key() > endKey.Get())) {
        *result->EndLimit.mutable_key() = endKey.Get();
    }

    return result;
}

void AppendErasureChunkSlices(
    TRefCountedInputChunkPtr inputChunk,
    NErasure::ECodec codecId,
    std::vector<TInputChunkSlicePtr>* slices)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*inputChunk, &dataSize, &rowCount);

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceStartRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceEndRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TInputChunkSlice>();
            slicedChunk->InputChunk = inputChunk;
            slicedChunk->PartIndex = partIndex;
            slicedChunk->StartLimit.set_row_index(sliceStartRowIndex);
            slicedChunk->EndLimit.set_row_index(sliceEndRowIndex);
            slicedChunk->SizeOverrideExt.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            slicedChunk->SizeOverrideExt.set_uncompressed_data_size((dataSize + dataPartCount - 1) / dataPartCount);

            slices->push_back(slicedChunk);
        }
    }
}

void ToProto(NProto::TInputChunk* inputChunk, const TInputChunkSlice& chunkSlice)
{
    inputChunk->CopyFrom(*chunkSlice.InputChunk);
    if (IsNontrivial(chunkSlice.StartLimit)) {
        inputChunk->mutable_start_limit()->CopyFrom(chunkSlice.StartLimit);
    }

    if (IsNontrivial(chunkSlice.EndLimit)) {
        inputChunk->mutable_end_limit()->CopyFrom(chunkSlice.EndLimit);
    }

    SetProtoExtension(inputChunk->mutable_extensions(), chunkSlice.SizeOverrideExt);
}


////////////////////////////////////////////////////////////////////////////////

bool IsNontrivial(const NProto::TReadLimit& limit)
{
    return limit.has_row_index() ||
        limit.has_key() ||
        limit.has_chunk_index() ||
        limit.has_offset();
}

void GetStatistics(
    const TInputChunk& inputChunk,
    i64* dataSize,
    i64* rowCount,
    i64* valueCount)
{
    auto miscExt = GetProtoExtension<TMiscExt>(inputChunk.extensions());
    auto sizeOverrideExt = FindProtoExtension<TSizeOverrideExt>(inputChunk.extensions());

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

TRefCountedInputChunkPtr CreateCompleteChunk(TRefCountedInputChunkPtr inputChunk)
{
    auto result = New<TRefCountedInputChunk>(*inputChunk);
    result->clear_start_limit();
    result->clear_end_limit();

    RemoveProtoExtension<TMiscExt>(result->mutable_extensions());

    return result;
}

TChunkId EncodeChunkId(
    const NProto::TInputChunk& inputChunk,
    NNodeTrackerClient::TNodeId nodeId)
{
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(inputChunk.replicas());
    auto replicaIt = std::find_if(
        replicas.begin(),
        replicas.end(),
        [=] (TChunkReplica replica) {
            return replica.GetNodeId() == nodeId;
        });
    YCHECK(replicaIt != replicas.end());

    TChunkIdWithIndex chunkIdWithIndex(
        FromProto<TChunkId>(inputChunk.chunk_id()),
        replicaIt->GetIndex());
    return EncodeChunkId(chunkIdWithIndex);
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes)
{
    return !attributes.Get<bool>("append", false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
