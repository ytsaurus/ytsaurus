#include "stdafx.h"
#include "chunk_slice.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "private.h"

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>
#include <core/logging/log.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static int DefaultPartIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TChunkSlice::TChunkSlice()
    : PartIndex_(DefaultPartIndex)
{ }

TChunkSlice::TChunkSlice(const TChunkSlice& other)
    : ChunkSpec_(other.ChunkSpec_)
    , PartIndex_(other.PartIndex_)
    , LowerLimit_(other.LowerLimit_)
    , UpperLimit_(other.UpperLimit_)
{
    SizeOverrideExt_.CopyFrom(other.SizeOverrideExt_);
}

TChunkSlice::TChunkSlice(TChunkSlice&& other)
    : ChunkSpec_(std::move(other.ChunkSpec_))
    , PartIndex_(other.PartIndex_)
    , LowerLimit_(std::move(other.LowerLimit_))
    , UpperLimit_(std::move(other.UpperLimit_))
{
    SizeOverrideExt_.Swap(&other.SizeOverrideExt_);
    other.PartIndex_ = DefaultPartIndex;
}

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
    : ChunkSpec_(chunkSpec)
    , PartIndex_(DefaultPartIndex)
{
    i64 dataSize;
    i64 rowCount;
    GetStatistics(*chunkSpec, &dataSize, &rowCount);

    SizeOverrideExt_.set_uncompressed_data_size(dataSize);
    SizeOverrideExt_.set_row_count(rowCount);

    if (chunkSpec->has_lower_limit()) {
        LowerLimit_ = chunkSpec->lower_limit();
    }

    if (chunkSpec->has_upper_limit()) {
        UpperLimit_ = chunkSpec->upper_limit();
    }

    if (lowerKey && (!LowerLimit_.HasKey() || LowerLimit_.GetKey() < *lowerKey)) {
        LowerLimit_.SetKey(*lowerKey);
    }

    if (upperKey && (!UpperLimit_.HasKey() || UpperLimit_.GetKey() > *upperKey)) {
        UpperLimit_.SetKey(*upperKey);
    }
}

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataSize)
    : ChunkSpec_(chunkSpec)
    , PartIndex_(partIndex)
{
    if (chunkSpec->has_lower_limit()) {
        LowerLimit_ = chunkSpec->lower_limit();
    }

    if (chunkSpec->has_upper_limit()) {
        UpperLimit_ = chunkSpec->upper_limit();
    }

    if (!LowerLimit_.HasRowIndex() || LowerLimit_.GetRowIndex() < lowerRowIndex) {
        LowerLimit_.SetRowIndex(lowerRowIndex);
    }

    if (!UpperLimit_.HasRowIndex() || UpperLimit_.GetRowIndex() > upperRowIndex) {
        UpperLimit_.SetRowIndex(upperRowIndex);
    }

    SizeOverrideExt_.set_row_count(UpperLimit_.GetRowIndex() - LowerLimit_.GetRowIndex());
    SizeOverrideExt_.set_uncompressed_data_size(dataSize);
}

TChunkSlice::TChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice)
    : ChunkSpec_(chunkSpec)
    , PartIndex_(protoChunkSlice.part_index())
    , LowerLimit_(protoChunkSlice.lower_limit())
    , UpperLimit_(protoChunkSlice.upper_limit())
    , SizeOverrideExt_(protoChunkSlice.size_override_ext())
{
    // merge limits
    if (chunkSpec->has_lower_limit()) {
        LowerLimit_.MergeLowerLimit(chunkSpec->lower_limit());
    }
    if (chunkSpec->has_upper_limit()) {
        LowerLimit_.MergeUpperLimit(chunkSpec->upper_limit());
    }
}

std::vector<TChunkSlicePtr> TChunkSlice::SliceEvenly(i64 sliceDataSize) const
{
    std::vector<TChunkSlicePtr> result;

    YCHECK(sliceDataSize > 0);

    i64 dataSize = GetDataSize();
    i64 rowCount = GetRowCount();

    // Inclusive.
    i64 LowerRowIndex = LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;
    i64 UpperRowIndex = UpperLimit_.HasRowIndex() ? UpperLimit_.GetRowIndex() : rowCount;

    rowCount = UpperRowIndex - LowerRowIndex;
    int count = (dataSize + sliceDataSize - 1) / sliceDataSize;

    for (int i = 0; i < count; ++i) {
        i64 sliceLowerRowIndex = LowerRowIndex + rowCount * i / count;
        i64 sliceUpperRowIndex = LowerRowIndex + rowCount * (i + 1) / count;
        if (sliceLowerRowIndex < sliceUpperRowIndex) {
            auto chunkSlice = New<TChunkSlice>(*this);
            chunkSlice->LowerLimit_.SetRowIndex(sliceLowerRowIndex);
            chunkSlice->UpperLimit_.SetRowIndex(sliceUpperRowIndex);
            chunkSlice->SetRowCount(sliceUpperRowIndex - sliceLowerRowIndex);
            chunkSlice->SetDataSize((dataSize + count - 1) / count);
            result.emplace_back(std::move(chunkSlice));
        }
    }

    return result;
}

i64 TChunkSlice::GetLocality(int replicaPartIndex) const
{
    i64 result = GetDataSize();

    if (PartIndex_ == DefaultPartIndex) {
        // For erasure chunks without specified part index,
        // data size is assumed to be split evenly between data parts.
        auto codecId = NErasure::ECodec(ChunkSpec_->erasure_codec());
        if (codecId != NErasure::ECodec::None) {
            auto* codec = NErasure::GetCodec(codecId);
            int dataPartCount = codec->GetDataPartCount();
            result = (result + dataPartCount - 1) / dataPartCount;
        }
    } else if (PartIndex_ != replicaPartIndex) {
        result = 0;
    }

    return result;
}

TRefCountedChunkSpecPtr TChunkSlice::ChunkSpec() const
{
    return ChunkSpec_;
}

int TChunkSlice::GetPartIndex() const
{
    return PartIndex_;
}

const TReadLimit& TChunkSlice::LowerLimit() const
{
    return LowerLimit_;
}

const TReadLimit& TChunkSlice::UpperLimit() const
{
    return UpperLimit_;
}

const NProto::TSizeOverrideExt& TChunkSlice::SizeOverrideExt() const
{
    return SizeOverrideExt_;
}

i64 TChunkSlice::GetMaxBlockSize() const
{
    auto miscExt = GetProtoExtension<NProto::TMiscExt>(ChunkSpec_->chunk_meta().extensions());
    return miscExt.max_block_size();
}

i64 TChunkSlice::GetDataSize() const
{
    return SizeOverrideExt_.uncompressed_data_size();
}

i64 TChunkSlice::GetRowCount() const
{
    return SizeOverrideExt_.row_count();
}

void TChunkSlice::SetDataSize(i64 dataSize)
{
    SizeOverrideExt_.set_uncompressed_data_size(dataSize);
}

void TChunkSlice::SetRowCount(i64 rowCount)
{
    SizeOverrideExt_.set_row_count(rowCount);
}

void TChunkSlice::SetLowerRowIndex(i64 lowerRowIndex)
{
    if (!LowerLimit_.HasRowIndex() || LowerLimit_.GetRowIndex() < lowerRowIndex) {
        LowerLimit_.SetRowIndex(lowerRowIndex);
    }
}

void TChunkSlice::SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey)
{
    if (!LowerLimit_.HasKey() || LowerLimit_.GetKey() < lowerKey) {
        LowerLimit_.SetKey(lowerKey);
    }

    if (!UpperLimit_.HasKey() || UpperLimit_.GetKey() > upperKey) {
        UpperLimit_.SetKey(upperKey);
    }
}

void TChunkSlice::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkSpec_);
    Persist(context, PartIndex_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, SizeOverrideExt_);
}

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const TNullable<NTableClient::TOwningKey>& lowerKey,
    const TNullable<NTableClient::TOwningKey>& upperKey)
{
    return New<TChunkSlice>(chunkSpec, lowerKey, upperKey);
}

TChunkSlicePtr CreateChunkSlice(
    TRefCountedChunkSpecPtr chunkSpec,
    const NProto::TChunkSlice& protoChunkSlice)
{
    return New<TChunkSlice>(chunkSpec, protoChunkSlice);
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
            auto slicedChunk = New<TChunkSlice>(
                chunkSpec,
                partIndex,
                sliceLowerRowIndex,
                sliceUpperRowIndex,
                (dataSize + dataPartCount - 1) / dataPartCount);
            slices.emplace_back(std::move(slicedChunk));
        }
    }

    return slices;
}

std::vector<TChunkSlicePtr> SliceChunkByRowIndexes(TRefCountedChunkSpecPtr chunkSpec, i64 sliceDataSize)
{
    return CreateChunkSlice(chunkSpec)->SliceEvenly(sliceDataSize);
}

std::vector<TChunkSlicePtr> SliceNewChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount)
{
    const auto& meta = chunkSpec->chunk_meta();

    TOwningKey minKey, maxKey;
    YCHECK(TryGetBoundaryKeys(meta, &minKey, &maxKey));
    // Leave only key prefix.
    auto lowerKey = GetKeyPrefix(minKey.Get(), keyColumnCount);
    auto upperKey = GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount);

    auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(meta.extensions());
    if (blockMetaExt.blocks_size() == 1) {
        // Only one block available - no need to slice.
        return { New<TChunkSlice>(chunkSpec, lowerKey, upperKey) };
    }

    std::vector<TOwningKey> indexKeys(blockMetaExt.blocks_size(), TOwningKey());
    for (int i = 0; i < blockMetaExt.blocks_size(); ++i) {
        YCHECK(i == blockMetaExt.blocks(i).block_index());
        FromProto(&indexKeys[i], blockMetaExt.blocks(i).last_key());
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
            result += CompareRows(
                limit.GetKey(),
                indexKeys[blockMeta.block_index()],
                keyColumnCount);
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
        return { New<TChunkSlice>(chunkSpec, lowerKey, upperKey) };
    }

    std::vector<TChunkSlicePtr> slices;
    i64 startRowIndex = beginIt->chunk_row_count();
    i64 dataSize = 0;

    while (true) {
        auto currentIt = beginIt++;
        dataSize += currentIt->uncompressed_size();

        if (beginIt == endIt) {
            break;
        }

        TOwningKey& key = indexKeys[currentIt->block_index()];
        const TOwningKey& nextKey = indexKeys[beginIt->block_index()];
        if (CompareRows(nextKey, key, keyColumnCount) == 0) {
            continue;
        }

        if (dataSize > sliceDataSize) {
            // Sanity check.
            YCHECK(CompareRows(lowerKey, key) <= 0);

            i64 endRowIndex = currentIt->chunk_row_count();
            key = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);

            auto slice = New<TChunkSlice>(chunkSpec, lowerKey, key);
            slice->SetRowCount(endRowIndex - startRowIndex);
            slice->SetDataSize(dataSize);
            slices.emplace_back(std::move(slice));

            lowerKey = key;
            startRowIndex = endRowIndex;
            dataSize = 0;
        }
    }

    YCHECK(CompareRows(lowerKey, upperKey) <= 0);
    i64 endRowIndex = (--endIt)->chunk_row_count();
    auto slice = New<TChunkSlice>(chunkSpec, lowerKey, upperKey);
    slice->SetRowCount(endRowIndex - startRowIndex);
    slice->SetDataSize(dataSize);
    slices.emplace_back(std::move(slice));

    return slices;
}

std::vector<TChunkSlicePtr> SliceOldChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount)
{
    const auto& meta = chunkSpec->chunk_meta();

    TOwningKey minKey, maxKey;
    YCHECK(TryGetBoundaryKeys(meta, &minKey, &maxKey));
    auto lowerKey = GetKeyPrefix(minKey.Get(), keyColumnCount);
    auto upperKey = GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount);

    auto indexExt = GetProtoExtension<TIndexExt>(meta.extensions());
    if (indexExt.items_size() == 1) {
        // Only one index entry available - no need to slice.
        return { New<TChunkSlice>(chunkSpec, lowerKey, upperKey) };
    }

    auto miscExt = GetProtoExtension<NProto::TMiscExt>(meta.extensions());
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
        return { New<TChunkSlice>(chunkSpec, lowerKey, upperKey) };
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

        if (dataSize > sliceDataSize) {
            // Sanity check.
            YCHECK(CompareRows(lowerKey, key, keyColumnCount) <= 0);

            i64 endRowIndex = beginIt->row_index();
            key = GetKeyPrefixSuccessor(key.Get(), keyColumnCount);

            auto slice = New<TChunkSlice>(chunkSpec, lowerKey, key);
            slice->SetRowCount(endRowIndex - startRowIndex);
            slice->SetDataSize(dataSize);
            slices.emplace_back(std::move(slice));

            lowerKey = key;
            startRowIndex = endRowIndex;
            dataSize = 0;
        }
    }

    // Sanity check.
    YCHECK (CompareRows(lowerKey, upperKey, keyColumnCount) <= 0);
    i64 endRowIndex = (--endIt)->row_index();
    auto slice = New<TChunkSlice>(chunkSpec, lowerKey, upperKey);
    slice->SetRowCount(endRowIndex - startRowIndex);
    slice->SetDataSize(dataSize);
    slices.emplace_back(std::move(slice));

    return slices;
}

std::vector<TChunkSlicePtr> SliceChunkByKeys(
    TRefCountedChunkSpecPtr chunkSpec,
    i64 sliceDataSize,
    int keyColumnCount)
{
    const auto& meta = chunkSpec->chunk_meta();
    auto chunkFormat = ETableChunkFormat(meta.version());
    switch (chunkFormat) {
        case ETableChunkFormat::Old:
            return SliceOldChunkByKeys(
                chunkSpec,
                sliceDataSize,
                keyColumnCount);

        case ETableChunkFormat::SchemalessHorizontal:
        case ETableChunkFormat::VersionedSimple:
            return SliceNewChunkByKeys(
                chunkSpec,
                sliceDataSize,
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

void ToProto(NProto::TChunkSpec* chunkSpec, const TChunkSlice& chunkSlice)
{
    chunkSpec->CopyFrom(*chunkSlice.ChunkSpec());

    if (IsNontrivial(chunkSlice.LowerLimit())) {
        ToProto(chunkSpec->mutable_lower_limit(), chunkSlice.LowerLimit());
    }

    if (IsNontrivial(chunkSlice.UpperLimit())) {
        ToProto(chunkSpec->mutable_upper_limit(), chunkSlice.UpperLimit());
    }

    SetProtoExtension(chunkSpec->mutable_chunk_meta()->mutable_extensions(), chunkSlice.SizeOverrideExt());

    // NB(psushin): probably, #ToProto should never ever filter anything...
    // ToDo(psushin): get rid of this after refactoring get of GetChunkSlices.
    std::vector<int> extensionTags = {
        TProtoExtensionTag<NProto::TMiscExt>::Value,
        TProtoExtensionTag<TBoundaryKeysExt>::Value,
        TProtoExtensionTag<TOldBoundaryKeysExt>::Value,
        TProtoExtensionTag<NProto::TSizeOverrideExt>::Value };
    auto filteredMeta = FilterChunkMetaByExtensionTags(chunkSpec->chunk_meta(), extensionTags);
    *chunkSpec->mutable_chunk_meta() = filteredMeta;
}

void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlice& chunkSlice)
{
    protoChunkSlice->set_part_index(chunkSlice.GetPartIndex());
    if (IsNontrivial(chunkSlice.LowerLimit())) {
        ToProto(protoChunkSlice->mutable_lower_limit(), chunkSlice.LowerLimit());
    }
    if (IsNontrivial(chunkSlice.UpperLimit())) {
        ToProto(protoChunkSlice->mutable_upper_limit(), chunkSlice.UpperLimit());
    }
    protoChunkSlice->mutable_size_override_ext()->CopyFrom(chunkSlice.SizeOverrideExt());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
