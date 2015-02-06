#include "stdafx.h"
#include "chunk_slice.h"
#include "chunk_meta_extensions.h"
#include "schema.h"

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;

using NTableClient::NProto::TIndexExt;
using NTableClient::NProto::TIndexRow;
using NVersionedTableClient::NProto::TBlockMetaExt;
using NVersionedTableClient::NProto::TBlockMeta;

////////////////////////////////////////////////////////////////////////////////

static int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice()
    : PartIndex(DefaultPartIndex)
{ }

TChunkSlice::TChunkSlice(const TChunkSlice& other)
    : LowerLimit(other.LowerLimit)
    , UpperLimit(other.UpperLimit)
{
    ChunkSpec = other.ChunkSpec;
    PartIndex = other.PartIndex;
    SizeOverrideExt.CopyFrom(other.SizeOverrideExt);
}

TChunkSlice::TChunkSlice(TChunkSlice&& other)
    : LowerLimit(other.LowerLimit)
    , UpperLimit(other.UpperLimit)
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
    i64 LowerRowIndex = LowerLimit.HasRowIndex() ? LowerLimit.GetRowIndex() : 0;
    i64 UpperRowIndex = UpperLimit.HasRowIndex() ? UpperLimit.GetRowIndex() : rowCount;

    rowCount = UpperRowIndex - LowerRowIndex;
    int count = std::ceil((double)dataSize / (double)sliceDataSize);

    for (int i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = LowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = LowerRowIndex + rowCount * (i + 1) / count;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TChunkSlice>(*this);
            chunkSlice->LowerLimit.SetRowIndex(sliceLowerRowIndex);
            chunkSlice->UpperLimit.SetRowIndex(sliceUpperRowIndex);
            chunkSlice->SizeOverrideExt.set_row_count(sliceUpperRowIndex - sliceLowerRowIndex);
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
    Persist(context, LowerLimit);
    Persist(context, UpperLimit);
    Persist(context, SizeOverrideExt);
}

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NVersionedTableClient::TOwningKey>& lowerKey,
    const TNullable<NVersionedTableClient::TOwningKey>& upperKey)
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
        result->LowerLimit = chunkSpec->lower_limit();
    }

    if (chunkSpec->has_upper_limit()) {
        result->UpperLimit = chunkSpec->upper_limit();
    }

    if (lowerKey && (!result->LowerLimit.HasKey() || result->LowerLimit.GetKey() < *lowerKey)) {
        result->LowerLimit.SetKey(*lowerKey);
    }

    if (upperKey && (!result->UpperLimit.HasKey() || result->UpperLimit.GetKey() > *upperKey)) {
        result->UpperLimit.SetKey(*upperKey);
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
        i64 sliceLowerRowIndex = rowCount * partIndex / dataPartCount;
        i64 sliceUpperRowIndex = rowCount * (partIndex + 1) / dataPartCount;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto slicedChunk = New<TChunkSlice>();
            slicedChunk->ChunkSpec = chunkSpec;
            slicedChunk->PartIndex = partIndex;
            slicedChunk->LowerLimit.SetRowIndex(sliceLowerRowIndex);
            slicedChunk->UpperLimit.SetRowIndex(sliceUpperRowIndex);
            slicedChunk->SizeOverrideExt.set_row_count(sliceUpperRowIndex - sliceLowerRowIndex);
            slicedChunk->SizeOverrideExt.set_uncompressed_data_size((dataSize + dataPartCount - 1) / dataPartCount);
            slices.emplace_back(std::move(slicedChunk));
        }
    }

    return slices;
}

std::vector<TChunkSlicePtr> SliceChunkByRowIndexes(TRefCountedChunkSpecPtr chunkSpec, i64 maxSliceSize)
{
    return CreateChunkSlice(chunkSpec)->SliceEvenly(maxSliceSize);
}

std::vector<TChunkSlicePtr> SliceNewChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec, 
    int keyColumnCount, 
    i64 maxSliceSize)
{
    const auto& meta = chunkSpec->chunk_meta();

    TOwningKey minKey, maxKey;
    YCHECK(TryGetBoundaryKeys(meta, &minKey, &maxKey));
    // Leave only key prefix.
    auto lowerKey = GetKeyPrefix(minKey.Get(), keyColumnCount);
    auto upperKey = GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount);

    auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(meta.extensions());
    if (blockMetaExt.blocks_size() == 1) {
        // Only one block available - no need to split.
        return { CreateChunkSlice(chunkSpec, lowerKey, upperKey) };
    }

    using NChunkClient::TReadLimit;
    auto comparer = [&] (
        const TReadLimit& limit,
        const TBlockMeta& blockMeta,
        bool isStartLimit) -> int
    {
        if (!limit.HasRowIndex() && !limit.HasKey()) {
            return isStartLimit ? -1 : 1;
        }

        auto result = 0;
        if (limit.HasRowIndex()) {
            auto diff = limit.GetRowIndex() - blockMeta.chunk_row_count();
            // Sign function.
            result += (diff > 0) - (diff < 0);
        }

        if (limit.HasKey()) {
            TOwningKey indexKey;
            FromProto(&indexKey, blockMeta.last_key());
            result += CompareRows(limit.GetKey(), indexKey, keyColumnCount);
        }

        if (result == 0) {
            return isStartLimit ? -1 : 1;
        }

        return (result > 0) - (result < 0);
    };

    auto beginIt = std::lower_bound(
        blockMetaExt.blocks().begin(),
        blockMetaExt.blocks().end(),
        TReadLimit(chunkSpec->lower_limit()),
        [&] (const TBlockMeta& blockMeta, const TReadLimit& limit) {
            return comparer(limit, blockMeta, true) > 0;
        });

    auto endIt = std::upper_bound(
        beginIt,
        blockMetaExt.blocks().end(),
        TReadLimit(chunkSpec->upper_limit()),
        [&] (const TReadLimit& limit, const TBlockMeta& blockMeta) {
            return comparer(limit, blockMeta, false) < 0;
        });

    if (std::distance(beginIt, endIt) < 2) {
        // Too small distance between given read limits.
        return { CreateChunkSlice(chunkSpec, lowerKey, upperKey) };
    }

    std::vector<TChunkSlicePtr> slices;
    i64 startRowIndex = beginIt->chunk_row_count();
    i64 dataSize = 0;

    while (true) {
        dataSize += beginIt->uncompressed_size();

        auto nextIt = beginIt + 1;
        if (nextIt == endIt) {
            break;
        }

        TOwningKey key, nextKey;
        FromProto(&key, beginIt->last_key());
        FromProto(&nextKey, nextIt->last_key());
        if (CompareRows(nextKey, key, keyColumnCount) == 0) {
            continue;
        }

        if (dataSize > maxSliceSize) {
            // Sanity check.
            YCHECK(CompareRows(lowerKey, key) <= 0);
    
            i64 endRowIndex = beginIt->chunk_row_count();
            key = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);

            auto slice = CreateChunkSlice(chunkSpec, lowerKey, key);
            slice->SizeOverrideExt.set_row_count(endRowIndex - startRowIndex);
            slice->SizeOverrideExt.set_uncompressed_data_size(dataSize);
            slices.emplace_back(std::move(slice));

            lowerKey = key;
            startRowIndex = endRowIndex;
            dataSize = 0;
        }

        ++beginIt;
    }

    YCHECK(CompareRows(lowerKey, upperKey) <= 0);
    i64 endRowIndex = (--endIt)->chunk_row_count();
    auto slice = CreateChunkSlice(chunkSpec, lowerKey, upperKey);
    slice->SizeOverrideExt.set_row_count(endRowIndex - startRowIndex);
    slice->SizeOverrideExt.set_uncompressed_data_size(dataSize);
    slices.emplace_back(std::move(slice));

    return slices;
}

std::vector<TChunkSlicePtr> SliceOldChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec, 
    int keyColumnCount, 
    i64 maxSliceSize)
{
    const auto& meta = chunkSpec->chunk_meta();

    TOwningKey minKey, maxKey;
    YCHECK(TryGetBoundaryKeys(meta, &minKey, &maxKey));
    auto lowerKey = GetKeyPrefix(minKey.Get(), keyColumnCount);
    auto upperKey = GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount);

    auto indexExt = GetProtoExtension<TIndexExt>(meta.extensions());
    if (indexExt.items_size() == 1) {
        // Only one index entry available - no need to split.
        return { CreateChunkSlice(chunkSpec, lowerKey, upperKey) };
    }

    auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
    i64 dataSizeBetweenSamples = miscExt.uncompressed_data_size() / indexExt.items_size();
    YCHECK(dataSizeBetweenSamples > 0);

    using NChunkClient::TReadLimit;
    auto comparer = [&] (
        const TReadLimit& limit,
        const TIndexRow& indexRow,
        bool isStartLimit) -> int
    {
        if (!limit.HasRowIndex() && !limit.HasKey()) {
            return isStartLimit ? -1 : 1;
        }

        auto result = 0;
        if (limit.HasRowIndex()) {
            auto diff = limit.GetRowIndex() - indexRow.row_index();
            // Sign function.
            result += (diff > 0) - (diff < 0);
        }

        if (limit.HasKey()) {
            TOwningKey indexKey;
            FromProto(&indexKey, indexRow.key());
            result += CompareRows(limit.GetKey(), indexKey, keyColumnCount);
        }

        if (result == 0) {
            return isStartLimit ? -1 : 1;
        }

        return (result > 0) - (result < 0);
    };

    auto beginIt = std::lower_bound(
        indexExt.items().begin(),
        indexExt.items().end(),
        TReadLimit(chunkSpec->lower_limit()),
        [&] (const TIndexRow& indexRow, const TReadLimit& limit) {
            return comparer(limit, indexRow, true) > 0;
        });

    auto endIt = std::upper_bound(
        beginIt,
        indexExt.items().end(),
        TReadLimit(chunkSpec->upper_limit()),
        [&] (const TReadLimit& limit, const TIndexRow& indexRow) {
            return comparer(limit, indexRow, false) < 0;
        });

    if (std::distance(beginIt, endIt) < 2) {
        // Too small distance between given read limits.
        return { CreateChunkSlice(chunkSpec, lowerKey, upperKey) };
    }

    std::vector<TChunkSlicePtr> slices;
    i64 startRowIndex = beginIt->row_index();
    i64 dataSize = 0;

    while (true) {
        ++beginIt;
        dataSize += dataSizeBetweenSamples;

        auto nextIt = beginIt + 1;
        if (nextIt == endIt) {
            break;
        }

        TOwningKey key, nextKey;
        FromProto(&key, beginIt->key());
        FromProto(&nextKey, nextIt->key());
        if (CompareRows(nextKey, key, keyColumnCount) == 0) {
            continue;
        }

        if (dataSize > maxSliceSize) {
            // Sanity check.
            YCHECK(CompareRows(lowerKey, key, keyColumnCount) <= 0);
    
            i64 endRowIndex = beginIt->row_index();
            key = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);

            auto slice = CreateChunkSlice(chunkSpec, lowerKey, key);
            slice->SizeOverrideExt.set_row_count(endRowIndex - startRowIndex);
            slice->SizeOverrideExt.set_uncompressed_data_size(dataSize);
            slices.emplace_back(std::move(slice));

            lowerKey = key;
            startRowIndex = endRowIndex;
            dataSize = 0;
        }
    }

    // Sanity check.
    YCHECK (CompareRows(lowerKey, upperKey, keyColumnCount) <= 0);
    i64 endRowIndex = (--endIt)->row_index();
    auto slice = CreateChunkSlice(chunkSpec, lowerKey, upperKey);
    slice->SizeOverrideExt.set_row_count(endRowIndex - startRowIndex);
    slice->SizeOverrideExt.set_uncompressed_data_size(dataSize);
    slices.emplace_back(std::move(slice));
    
    return slices;
}

std::vector<TChunkSlicePtr> SliceChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec, 
    int keyColumnCount, 
    i64 maxSliceSize)
{
    const auto& meta = chunkSpec->chunk_meta();
    auto chunkFormat = ETableChunkFormat(meta.version());
    switch (chunkFormat) {
        case ETableChunkFormat::Old:
            return SliceOldChunkByKeys(
                chunkSpec,
                maxSliceSize,
                keyColumnCount);

        case ETableChunkFormat::SchemalessHorizontal:
        case ETableChunkFormat::VersionedSimple:
            return SliceNewChunkByKeys(
                chunkSpec,
                maxSliceSize,
                keyColumnCount);

        default: {
            using NYT::FromProto;
            auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
            THROW_ERROR_EXCEPTION("Unsupported chunk version (ChunkId: %v; ChunkFormat: %v)",
                chunkId,
                ETableChunkFormat(meta.version()));
        }
    }
}

void ToProto(TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice)
{
    chunkSpec->CopyFrom(*chunkSlice.ChunkSpec);

    if (IsNontrivial(chunkSlice.LowerLimit)) {
        ToProto(chunkSpec->mutable_lower_limit(), chunkSlice.LowerLimit);
    }

    if (IsNontrivial(chunkSlice.UpperLimit)) {
        ToProto(chunkSpec->mutable_upper_limit(), chunkSlice.UpperLimit);
    }

    SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), chunkSlice.SizeOverrideExt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
