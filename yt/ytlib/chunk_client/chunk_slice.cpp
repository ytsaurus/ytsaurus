#include "stdafx.h"
#include "chunk_slice.h"
#include "chunk_meta_extensions.h"
#include "schema.h"

#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice()
    : PartIndex(DefaultPartIndex)
{ }

TChunkSlice::TChunkSlice(const TChunkSlice& other)
    : StartLimit(other.StartLimit)
    , EndLimit(other.EndLimit)
{
    ChunkSpec = other.ChunkSpec;
    PartIndex = other.PartIndex;
    SizeOverrideExt.CopyFrom(other.SizeOverrideExt);
}

TChunkSlice::TChunkSlice(TChunkSlice&& other)
    : StartLimit(other.StartLimit)
    , EndLimit(other.EndLimit)
{
    ChunkSpec = std::move(other.ChunkSpec);

    PartIndex = other.PartIndex;
    other.PartIndex = DefaultPartIndex;
    SizeOverrideExt.Swap(&other.SizeOverrideExt);
}

TChunkSlice::~TChunkSlice()
{ }

std::vector<TChunkSlicePtr> TChunkSlice::SliceEvenly(i64 sliceDataSize) const
{
    std::vector<TChunkSlicePtr> result;

    YCHECK(sliceDataSize > 0);

    i64 dataSize = GetDataSize();
    i64 rowCount = GetRowCount();

    // Inclusive.
    i64 startRowIndex = StartLimit.HasRowIndex() ? StartLimit.GetRowIndex() : 0;
    i64 endRowIndex = EndLimit.HasRowIndex() ? EndLimit.GetRowIndex() : rowCount;

    rowCount = endRowIndex - startRowIndex;
    int count = std::ceil((double)dataSize / (double)sliceDataSize);

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto chunkSlice = New<TChunkSlice>(*this);
            chunkSlice->StartLimit.SetRowIndex(sliceStartRowIndex);
            chunkSlice->EndLimit.SetRowIndex(sliceEndRowIndex);
            chunkSlice->SizeOverrideExt.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            chunkSlice->SizeOverrideExt.set_uncompressed_data_size((dataSize + count - 1) / count);
            result.emplace_back(std::move(chunkSlice));
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
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkSpec->chunk_meta().extensions());
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
    const TNullable<NVersionedTableClient::TOwningKey>& startKey,
    const TNullable<NVersionedTableClient::TOwningKey>& endKey)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    auto result = New<TChunkSlice>();
    result->ChunkSpec = chunkSpec;
    result->SizeOverrideExt.set_uncompressed_data_size(dataSize);
    result->SizeOverrideExt.set_row_count(rowCount);
    result->PartIndex = DefaultPartIndex;

    if (chunkSpec->has_lower_limit()) {
        result->StartLimit = chunkSpec->lower_limit();
    }

    if (chunkSpec->has_upper_limit()) {
        result->EndLimit = chunkSpec->upper_limit();
    }

    if (startKey && (!result->StartLimit.HasKey() || result->StartLimit.GetKey() < *startKey)) {
        result->StartLimit.SetKey(*startKey);
    }

    if (endKey && (!result->EndLimit.HasKey() || result->EndLimit.GetKey() > *endKey)) {
        result->EndLimit.SetKey(*endKey);
    }

    return result;
}

std::vector<TChunkSlicePtr> CreateErasureChunkSlices(
    TRefCountedChunkSpecPtr chunkSpec,
    NErasure::ECodec codecId)
{
    std::vector<TChunkSlicePtr> slices;

    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        i64 sliceStartRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceEndRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TChunkSlice>();
            slicedChunk->ChunkSpec = chunkSpec;
            slicedChunk->PartIndex = partIndex;
            slicedChunk->StartLimit.SetRowIndex(sliceStartRowIndex);
            slicedChunk->EndLimit.SetRowIndex(sliceEndRowIndex);
            slicedChunk->SizeOverrideExt.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            slicedChunk->SizeOverrideExt.set_uncompressed_data_size((dataSize + dataPartCount - 1) / dataPartCount);
            slices.emplace_back(std::move(slicedChunk));
        }
    }

    return slices;
}

void ToProto(TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice)
{
    chunkSpec->CopyFrom(*chunkSlice.ChunkSpec);

    if (!IsTrivial(chunkSlice.StartLimit)) {
        ToProto(chunkSpec->mutable_lower_limit(), chunkSlice.StartLimit);
    }

    if (!IsTrivial(chunkSlice.EndLimit)) {
        ToProto(chunkSpec->mutable_upper_limit(), chunkSlice.EndLimit);
    }

    SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), chunkSlice.SizeOverrideExt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
