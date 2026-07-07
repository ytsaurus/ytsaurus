#include "compressed_block_last_keys.h"
#include "read_span_refiner.h"
#include "block_ref.h"
#include "dispatch_by_type.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>
#include <yt/yt/ytlib/table_chunk_format/column_writer.h>

namespace NYT::NColumnarChunkFormat {

using NTableClient::TTableSchema;
using NTableClient::TTableSchemaPtr;

using NTableClient::TKeyRef;

////////////////////////////////////////////////////////////////////////////////

struct IKeyColumnAccessor
{
    virtual TUnversionedValue GetValue(ui32 rowIndex) const = 0;
    virtual ~IKeyColumnAccessor() = default;
};

template <EValueType Type>
class TKeyColumnAccessor
    : public IKeyColumnAccessor
    , public TLookupKeyIndexExtractor
    , public TLookupDataExtractor<Type>
    , public TColumnBase
{
public:
    explicit TKeyColumnAccessor(const TColumnBase* base)
        : TColumnBase(base)
    {
        bool newMeta = true;
        auto meta = SkipToSegment<TKeyMeta<Type>>(0);
        if (meta) {
            auto data = GetBlock().begin() + meta->DataOffset;
            if (newMeta) {
                DoInitLookupKeySegment</*NewMeta*/ true>(this, meta, reinterpret_cast<const ui64*>(data));
            } else {
                DoInitLookupKeySegment</*NewMeta*/ false>(this, meta, reinterpret_cast<const ui64*>(data));
            }
        } else {
            // Key column not present in chunk.
            TLookupKeyIndexExtractor::InitNullIndex();
            TLookupDataExtractor<Type>::InitNullData();
        }
    }

    TUnversionedValue GetValue(ui32 rowIndex) const
    {
        auto position = TLookupKeyIndexExtractor::SkipTo(rowIndex, 0);
        YT_ASSERT(position < GetCount());
        TUnversionedValue result;
        TLookupDataExtractor<Type>::Extract(&result, position);
        return result;
    }
};

template <EValueType Type>
struct TCreateKeyColumn
{
    static std::unique_ptr<IKeyColumnAccessor> Do(const TColumnBase* columnBase)
    {
        return std::make_unique<TKeyColumnAccessor<Type>>(columnBase);
    }
};

struct TSimpleBoundsIterator
{
    TRange<TLegacyKey> Items;

    ui32 ColumnId = 0;
    ui32 BoundIndex = 0;
    ui32 Limit = 0;

    bool IsExhausted() const
    {
        return BoundIndex == Limit;
    }

    Y_FORCE_INLINE TUnversionedValue GetItem(ui32 boundIndex) const
    {
        auto bound = Items[boundIndex];
        // Can also determine lower and upper bound by index.
        if (ColumnId < bound.GetCount()) {
            return bound[ColumnId];
        } else {
            return MakeUnversionedSentinelValue(EValueType::Min);
        }
    }

    Y_FORCE_INLINE void SetReadSpan(TReadSpan span)
    {
        BoundIndex = span.Lower;
        Limit = span.Upper;
    }

    Y_FORCE_INLINE TUnversionedValue GetValue() const
    {
        return GetItem(BoundIndex);
    }

    template <class TPredicate>
    Y_FORCE_INLINE void SkipWhile(TPredicate pred)
    {
        YT_ASSERT(BoundIndex != Limit);
        BoundIndex = ExponentialSearch(BoundIndex + 1, Limit, [&] (auto index) {
            return pred(GetItem(index));
        });
    }
};

template <EValueType Type>
struct TRefineColumn
{
    static void Do(
        const TColumnBase* columnBase,
        TSimpleBoundsIterator* items,
        TRange<std::pair<ui32, ui32>> matchings,
        std::vector<std::pair<ui32, ui32>>* nextMatchings)
    {
        TColumnIterator<Type> columnIterator;

        if (columnBase->GetBlock()) {
            // Blocks are not set for null columns.
            columnIterator.SetBlock(columnBase->GetBlock(), columnBase->GetSegmentMetas<TKeyMeta<Type>>());
        }

        ui32 itemsOffset = 0;
        ui32 chunkOffset = 0;
        for (auto [itemsLimit, chunkLimit] : matchings) {
            if (itemsOffset == itemsLimit || chunkOffset == chunkLimit) {
                nextMatchings->emplace_back(itemsLimit, chunkLimit);
            } else {
                columnIterator.SetReadSpan({chunkOffset, chunkLimit});
                items->SetReadSpan({itemsOffset, itemsLimit});

                auto* chunk = &columnIterator;

                // Symmetric code.
                while (!items->IsExhausted() && !chunk->IsExhausted()) {
                    auto keyValue = items->GetValue();
                    auto chunkValue = chunk->GetValue();

                    auto cmpRes = CompareValues<Type>(keyValue, chunkValue);

                    if (cmpRes < 0) {
                        items->SkipWhile([&] (TUnversionedValue value) {
                            return CompareValues<Type>(value, chunkValue) < 0;
                        });
                    } else if (cmpRes > 0) {
                        chunk->SkipWhile([&] (TUnversionedValue value) {
                            return CompareValues<Type>(value, keyValue) < 0;
                        });
                    } else {
                        items->SkipWhile([&] (TUnversionedValue value) {
                            return CompareValues<Type>(value, chunkValue) == 0;
                        });
                        chunk->SkipWhile([&] (TUnversionedValue value) {
                            return CompareValues<Type>(value, keyValue) == 0;
                        });
                    }
                    nextMatchings->emplace_back(items->BoundIndex, chunk->GetCurrentIndex());
                }

                if (items->IsExhausted() != chunk->IsExhausted()) {
                    nextMatchings->emplace_back(items->Limit, chunk->SkipTillEnd().Upper);
                }
            }

            itemsOffset = itemsLimit;
            chunkOffset = chunkLimit;
        }
    }
};

class TBlockLastKeys
    : public IBlockLastKeys
{
public:
    struct TTag{};

    TBlockLastKeys(
        TRange<TUnversionedRow> blockLastKeys,
        std::vector<ui32> chunkRowCountsUnique,
        TTableSchemaPtr chunkSchema)
        : ChunkRowCounts_(std::move(chunkRowCountsUnique))
        , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    {
        YT_VERIFY(blockLastKeys.size() == ChunkRowCounts_.size());

        NTableChunkFormat::TDataBlockWriter blockWriter(true);
        std::vector<std::unique_ptr<NTableChunkFormat::IValueColumnWriter>> columnWriters;

        if (!blockLastKeys.Empty() && chunkSchema->GetKeyColumnCount() > 0) {
            int maxSegmentValueCount = std::ssize(blockLastKeys);

            for (int keyColumnIndex = 0; keyColumnIndex < chunkSchema->GetKeyColumnCount(); ++keyColumnIndex) {
                const auto& columnSchema = chunkSchema->Columns()[keyColumnIndex];

                columnWriters.emplace_back(NTableChunkFormat::CreateUnversionedColumnWriter(
                    keyColumnIndex,
                    columnSchema,
                    &blockWriter,
                    nullptr,
                    /*serializeFloatsAsDoubles*/ true,
                    maxSegmentValueCount));
            }

            for (const auto& columnWriter : columnWriters) {
                columnWriter->WriteUnversionedValues(blockLastKeys);
            }

            auto block = blockWriter.DumpBlock(0, std::ssize(blockLastKeys));

            // Reader.
            BlockData_ = MergeRefsToRef<TTag>(block.Data);
            BlockRef_ = TBlockRef{BlockData_, TRef(BlockData_.Begin() + *block.SegmentMetaOffset, BlockData_.End())};
        } else {
            BlockRef_ = TBlockRef{BlockData_, TRef(BlockData_.Begin(), BlockData_.End())};
        }

        for (int keyColumnIndex = 0; keyColumnIndex < chunkSchema->GetKeyColumnCount(); ++keyColumnIndex) {
            const auto& columnSchema = chunkSchema->Columns()[keyColumnIndex];

            TColumnBase columnBase(&BlockRef_, keyColumnIndex, keyColumnIndex);
            ColumnAccessors_.push_back(DispatchByDataType<TCreateKeyColumn>(columnSchema.GetWireType(), &columnBase));
        }
    }

    TKeyRef GetBlockLastKey(int rowIndex, std::vector<TUnversionedValue>* buffer) const override
    {
        buffer->resize(std::ssize(ColumnAccessors_));

        for (int columnIndex = 0; columnIndex < std::ssize(ColumnAccessors_); ++columnIndex) {
            (*buffer)[columnIndex] = ColumnAccessors_[columnIndex]->GetValue(rowIndex);
        }

        return *buffer;
    }

    std::vector<TSpanMatching> BuildReadListForWindow(TRange<TRowRange> readItems, const TTableSchema& tableSchema) const override
    {
        TSimpleBoundsIterator keys;
        keys.Items = TRange(&readItems.Begin()->first, readItems.Size() * 2);

        auto matchings = DoBuildReadListForWindow(&keys, tableSchema);

        std::vector<TSpanMatching> result;

        ui32 lastChunkOffset = -1;
        auto pushMatching = [&] (ui32 chunkOffset, TReadSpan control) {
            control = {control.Lower / 2, (control.Upper + 1) / 2};
            if (lastChunkOffset == chunkOffset) {
                result.back().Control.Upper = control.Upper;
            } else {
                result.emplace_back(GetChunkSpan(chunkOffset), control);
            }
            lastChunkOffset = chunkOffset;
        };

        // Can use table key column count but chunk key column count is sufficient.
        auto keyColumnCount = ChunkKeyColumnCount_;

        ui32 itemsOffset = 0;
        ui32 chunkOffset = 0;

        for (auto [itemsLimit, chunkLimit] : matchings) {
            TReadSpan itemSpan{itemsOffset, itemsLimit};
            TReadSpan chunkSpan{chunkOffset, chunkLimit};

            if (IsEmpty(chunkSpan)) {
                YT_VERIFY(!IsEmpty(itemSpan));
                pushMatching(chunkOffset, itemSpan);
            } else if (!IsEmpty(itemSpan)) {
                YT_VERIFY(chunkOffset + 1 == chunkLimit);

                for (auto i = itemsOffset; i < itemsLimit; ++i) {
                    // Need to refine matching by checking min/max sentinels.
                    auto bound = keys.Items[i];
                    TUnversionedValue trailingValue = keyColumnCount < int(bound.GetCount())
                        ? bound[keyColumnCount]
                        : MakeUnversionedSentinelValue(EValueType::Min);

                    bool isUpperBound = i & 1;

                    // Skip excluded lower bound.
                    if (isUpperBound || trailingValue.Type != EValueType::Max) {
                        pushMatching(chunkOffset, {i, i + 1});
                    }
                }
            } else {
                YT_VERIFY(IsEmpty(itemSpan));

                // Check cover.
                bool isInsideRange = itemsOffset & 1;

                if (isInsideRange) {
                    for (auto i = chunkOffset; i < chunkLimit; ++i) {
                        pushMatching(i, TReadSpan{itemsOffset, itemsOffset + 1});
                    }
                }
            }

            itemsOffset = itemsLimit;
            chunkOffset = chunkLimit;
        }

        return result;
    }

    std::vector<TSpanMatching> BuildReadListForWindow(TRange<TLegacyKey> readItems, const TTableSchema& tableSchema) const override
    {
        TSimpleBoundsIterator keys;
        keys.Items = readItems;

        auto matchings = DoBuildReadListForWindow(&keys, tableSchema);

        std::vector<TSpanMatching> result;

        ui32 lastChunkOffset = -1;
        auto pushMatching = [&] (ui32 chunkOffset, TReadSpan control) {
            if (lastChunkOffset == chunkOffset) {
                result.back().Control.Upper = control.Upper;
            } else {
                result.emplace_back(GetChunkSpan(chunkOffset), control);
            }
            lastChunkOffset = chunkOffset;
        };

        ui32 itemsOffset = 0;
        ui32 chunkOffset = 0;
        for (auto [itemsLimit, chunkLimit] : matchings) {
            TReadSpan itemSpan{itemsOffset, itemsLimit};
            TReadSpan chunkSpan{chunkOffset, chunkLimit};

            if (!IsEmpty(itemSpan)) {
                YT_VERIFY(chunkLimit - chunkOffset <= 1);
                pushMatching(chunkOffset, itemSpan);
            }

            itemsOffset = itemsLimit;
            chunkOffset = chunkLimit;
        }

        return result;
    }

    i64 GetByteSize() const override
    {
        return BlockData_.Size() + sizeof(*this) + ChunkRowCounts_.size() * sizeof(ui32);
    }

private:
    TSharedRef BlockData_;
    TBlockRef BlockRef_;
    std::vector<std::unique_ptr<IKeyColumnAccessor>> ColumnAccessors_;

    const std::vector<ui32> ChunkRowCounts_;
    const int ChunkKeyColumnCount_;

    static std::pair<ui32, ui32>* MergeAdjacentMatchings(TMutableRange<std::pair<ui32, ui32>> matchings)
    {
        bool keySpanWasEmpty = false;
        bool chunkSpanWasEmpty = false;

        ui32 lastKeyIndex = 0;
        ui32 lastChunkIndex = 0;

        auto inputIt = matchings.Begin();
        auto endIt = matchings.End();
        auto outputIt = inputIt;

        while (inputIt != endIt) {
            auto [keysIndex, chunkIndex] = *inputIt++;

            bool keySpanEmpty = keysIndex == lastKeyIndex;
            bool chunkSpanEmpty = chunkIndex == lastChunkIndex;

            if (keySpanWasEmpty && keySpanEmpty || chunkSpanWasEmpty && chunkSpanEmpty) {
                *(outputIt - 1) = {keysIndex, chunkIndex};
            } else {
                *outputIt++ = {keysIndex, chunkIndex};
            }

            keySpanWasEmpty = keySpanEmpty;
            chunkSpanWasEmpty = chunkSpanEmpty;

            lastKeyIndex = keysIndex;
            lastChunkIndex = chunkIndex;
        }

        return outputIt;
    }

    std::vector<std::pair<ui32, ui32>> DoBuildReadListForWindow(TSimpleBoundsIterator* keys, const TTableSchema& tableSchema) const
    {
        std::vector<std::pair<ui32, ui32>> matchings;
        std::vector<std::pair<ui32, ui32>> nextMatchings;

        matchings.push_back({ui32(std::ssize(keys->Items)), ui32(ChunkRowCounts_.size())});

        // Can use table key column count but chunk key column count is sufficient.
        auto keyColumnCount = ChunkKeyColumnCount_;

        for (int keyColumnIndex = 0; keyColumnIndex < keyColumnCount; ++keyColumnIndex) {
            keys->ColumnId = keyColumnIndex;

            const auto& columnSchema = tableSchema.Columns()[keyColumnIndex];
            TColumnBase columnBase(keyColumnIndex < ChunkKeyColumnCount_ ? &BlockRef_ : nullptr, keyColumnIndex, keyColumnIndex);
            DispatchByDataType<TRefineColumn>(columnSchema.GetWireType(), &columnBase, keys, matchings, &nextMatchings);

            // Merging adjacent matchings is optimization.
            nextMatchings.erase(MergeAdjacentMatchings(nextMatchings), nextMatchings.end());

            matchings.clear();
            nextMatchings.swap(matchings);
        }

        return matchings;
    }

    TReadSpan GetChunkSpan(ui32 index) const
    {
        ui32 startBound = index > 0 ? ChunkRowCounts_[index - 1] : 0;
        ui32 endBound = index < ChunkRowCounts_.size() ? ChunkRowCounts_[index] : startBound;

        return {startBound, endBound};
    }
};

std::unique_ptr<IBlockLastKeys> CompressBlockLastKeys(
    TRange<TUnversionedRow> blockLastKeys,
    std::vector<ui32> chunkRowCountsUnique,
    TTableSchemaPtr chunkSchema)
{
    return std::make_unique<TBlockLastKeys>(blockLastKeys, std::move(chunkRowCountsUnique), chunkSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
