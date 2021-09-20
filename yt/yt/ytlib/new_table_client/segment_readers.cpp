#include "segment_readers.h"

#define UNROLL_LOOPS

namespace NYT::NNewTableClient {

struct TWriteIdsTag { };
struct TRowToValueTag { };
struct TRowIndexTag { };

////////////////////////////////////////////////////////////////////////////////

void TScanTimestampExtractor::ReadSegment(const TMetaBase* meta, const char* data, TTmpBuffers* tmpBuffers)
{
    SegmentRowOffset_ = meta->ChunkRowCount - meta->RowCount;
    RowCount_ = meta->RowCount;
    DoInitSegment(static_cast<const TTimestampMeta*>(meta), data, tmpBuffers);
}

void TScanTimestampExtractor::DoInitSegment(const TTimestampMeta* meta, const char* data, TTmpBuffers* tmpBuffers)
{
    auto [timestamps, ids, offsets] = *tmpBuffers;

    TCompressedVectorView view(reinterpret_cast<const ui64*>(data));
    UnpackBitVector(view, &timestamps);

    auto writeTimestampsData = ++view;
    auto deleteTimestampsData = ++view;
    auto writeTimestampsIndexData = ++view;
    auto deleteTimestampsIndexData = ++view;

    YT_VERIFY(writeTimestampsIndexData.GetSize() == RowCount_);
    YT_VERIFY(writeTimestampsIndexData.GetSize() == deleteTimestampsIndexData.GetSize());

    // Calculate size of memory for holder.
    std::tie(
        WriteTimestamps_,
        DeleteTimestamps_,
        WriteTimestampOffsets_,
        DeleteTimestampOffsets_) = AllocateCombined<TTimestamp, TTimestamp, ui32, ui32>(
        &Holder_,
        writeTimestampsData.GetSize(),
        deleteTimestampsData.GetSize(),
        writeTimestampsIndexData.GetSize() + 1,
        deleteTimestampsIndexData.GetSize() + 1);

    {
        UnpackBitVector(writeTimestampsData, &ids);
        for (size_t index = 0; index < ids.size(); ++index) {
            WriteTimestamps_[index] = meta->BaseTimestamp + timestamps[ids[index]];
        }
    }

    {
        UnpackBitVector(deleteTimestampsData, &ids);
        for (size_t index = 0; index < ids.size(); ++index) {
            DeleteTimestamps_[index] = meta->BaseTimestamp + timestamps[ids[index]];
        }
    }

    {
        UnpackBitVector(writeTimestampsIndexData, &offsets);
        WriteTimestampOffsets_[0] = 0;

        auto expectedCount = meta->ExpectedWritesPerRow;
        for (size_t index = 0; index < offsets.size(); ++index) {
            WriteTimestampOffsets_[index + 1] = expectedCount * (index + 1) + ZigZagDecode32(offsets[index]);
        }
    }

    {
        UnpackBitVector(deleteTimestampsIndexData, &offsets);
        DeleteTimestampOffsets_[0] = 0;

        auto expectedCount = meta->ExpectedDeletesPerRow;
        for (size_t index = 0; index < offsets.size(); ++index) {
            DeleteTimestampOffsets_[index + 1] = expectedCount * (index + 1) + ZigZagDecode32(offsets[index]);
        }
    }
}

template <class T>
const ui64* TScanIntegerExtractor<T>::Init(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* tmpBuffers)
{
    auto& values = tmpBuffers->Values;
    auto& ids = tmpBuffers->Ids;

    bool direct = IsDirect(meta->Type);

    const auto* integerMeta = static_cast<const TIntegerMeta*>(meta);
    auto baseValue = integerMeta->BaseValue;

    if (direct) {
        TCompressedVectorView valuesView(ptr);
        ptr += valuesView.GetSizeInWords();
        size_t valueCount = valuesView.GetSize();

        auto [items] = AllocateCombined<T>(&Holder_, valueCount);
        valuesView.UnpackTo(items);

        NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(valueCount);

#ifdef UNROLL_LOOPS
        auto tailCount = valueCount % 8;
        auto itemsPtr = items;
        auto itemsPtrEnd = itemsPtr + valueCount - tailCount;

        while (itemsPtr < itemsPtrEnd) {
            for (int x = 0; x < 8; ++x) {
                *itemsPtr = ConvertInt<T>(baseValue + *itemsPtr);
                ++itemsPtr;
            }
        }

        {
            for (int x = 0; x < static_cast<int>(tailCount); ++x) {
                *itemsPtr = ConvertInt<T>(baseValue + *itemsPtr);
                ++itemsPtr;
            }
        }
#else
        for (size_t index = 0; index < valueCount; ++index) {
            items[index] = ConvertInt<T>(baseValue + items[index]);
        }
#endif

        Items_ = items;
    } else {
        TCompressedVectorView valuesView(ptr);
        ptr += valuesView.GetSizeInWords();
        values.resize(1 + valuesView.GetSize());
        // Zero id denotes null value.
        values[0] = 0;
        valuesView.UnpackTo(values.data() + 1);

        ptr += UnpackBitVector(ptr, &ids);
        auto valueCount = ids.size();

        auto [items, nullBits] = AllocateCombined<T, TBit>(&Holder_, valueCount, valueCount);

#ifdef UNROLL_LOOPS
        auto tailCount = valueCount % 8;
        auto itemsPtr = items;
        auto itemsPtrEnd = itemsPtr + valueCount - tailCount;

        ui8* isNullData = nullBits.GetData();
        auto idsPtr = ids.data();

        while (itemsPtr < itemsPtrEnd) {
            ui32 id;
            ui8 word = 0;
            for (int x = 0; x < 8; ++x) {
                id = *idsPtr++;
                word |= ui8(!id) << x;
                *itemsPtr++ = ConvertInt<T>(baseValue + values[id]);
            }
            *isNullData++ = word;
        }

        {
            ui32 id;
            ui8 word = 0;
            for (int x = 0; x < static_cast<int>(tailCount); ++x) {
                id = *idsPtr++;
                word |= ui8(!id) << x;
                *itemsPtr++ = ConvertInt<T>(baseValue + values[id]);
            }
            *isNullData++ = word;
        }
#else
        for (size_t index = 0; index < valueCount; ++index) {
            auto id = ids[index];
            nullBits.Set(index, id == 0);
            items[index] = ConvertInt<T>(baseValue + values[id]);
        }
#endif

        Items_ = items;
        NullBits_ = nullBits;
    }

    return ptr;
}

template <class T>
void TScanIntegerExtractor<T>::InitNull()
{
    auto [items, nullBits] = AllocateCombined<T, TBit>(&Holder_, 1, 1);

    items[0] = 0;
    nullBits.Set(0, true);

    Items_ = items;
    NullBits_ = nullBits;
}

template
class TScanIntegerExtractor<i64>;

template
class TScanIntegerExtractor<ui64>;

const ui64* TScanDataExtractor<EValueType::Double>::Init(
    const TMetaBase* /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/)
{
    // No dictionary mode for double.
    ui64 count = *ptr++;
    Items_ = reinterpret_cast<const double*>(ptr);
    ptr += count;

    NullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}

void TScanDataExtractor<EValueType::Double>::InitNull()
{
    auto [items, nullBits] = AllocateCombined<double, TBit>(&Holder_, 1, 1);

    items[0] = 0;
    nullBits.Set(0, true);

    Items_ = items;
    NullBits_ = nullBits;
}

ui64 TScanDataExtractor<EValueType::Boolean>::NullBooleanSegmentData;

const ui64* TScanDataExtractor<EValueType::Boolean>::Init(
    const TMetaBase* /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/)
{
    ui64 count = *ptr++;

    Items_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    NullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}

void TScanDataExtractor<EValueType::Boolean>::InitNull()
{
    TMutableBitmap bitmap(&NullBooleanSegmentData);
    bitmap.Set(0, true);

    Items_ = bitmap;
    NullBits_ = bitmap;
}

void TScanBlobExtractor::Init(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* tmpBuffers)
{
    auto& ids = tmpBuffers->Ids;
    auto& offsets = tmpBuffers->Offsets;

    auto expectedLength = static_cast<const TBlobMeta*>(meta)->ExpectedLength;

    bool direct = IsDirect(meta->Type);

    if (direct) {
        ptr += UnpackBitVector(ptr, &offsets);
        auto valueCount = offsets.size();

        auto [items] = AllocateCombined<TItem>(&Holder_, valueCount);

        ui32 begin = 0;
        for (size_t index = 0; index < valueCount; ++index) {
            ui32 end = expectedLength * (index + 1) + ZigZagDecode32(offsets[index]);
            items[index] = {begin, end};
            begin = end;
        }

        Items_ = items;
        NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(valueCount);
    } else {
        ptr += UnpackBitVector(ptr, &ids);
        auto valueCount = ids.size();
        ptr += UnpackBitVector(ptr, &offsets);

        auto [items, nullBits] = AllocateCombined<TItem, TBit>(&Holder_, valueCount, valueCount);

        auto getOffset = [=] (ui32 index) -> ui32 {
            if (index > 0) {
                return expectedLength * index + ZigZagDecode32(offsets[index - 1]);
            } else {
                return 0;
            }
        };

        for (size_t index = 0; index < valueCount; ++index) {
            auto id = ids[index];
            nullBits.Set(index, id == 0);

            if (id > 0) {
                items[index] = {getOffset(id - 1), getOffset(id)};
            }
        }

        Items_ = items;
        NullBits_ = nullBits;
    }

    Data_ = reinterpret_cast<const char*>(ptr);
}

void TScanBlobExtractor::InitNull()
{
    auto [items, nullBits] = AllocateCombined<TItem, TBit>(&Holder_, 1, 1);

    items[0] = TItem{0, 0};
    nullBits.Set(0, true);

    Items_ = items;
    NullBits_ = nullBits;
    Data_ = nullptr;
}

const ui64* TScanKeyIndexExtractor::Init(const TMetaBase* meta, const ui64* ptr, bool dense)
{
    SegmentRowLimit_ = meta->ChunkRowCount;
    ui32 rowOffset = meta->ChunkRowCount - meta->RowCount;

    if (dense) {
        Count_ = meta->RowCount;
        RowIndex_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());

        auto rowIndexData = RowIndex_.GetData();
        auto rowIndexDataEnd = rowIndexData + Count_;
        while (rowIndexData + 4 < rowIndexDataEnd) {
            *rowIndexData++ = rowOffset++;
            *rowIndexData++ = rowOffset++;
            *rowIndexData++ = rowOffset++;
            *rowIndexData++ = rowOffset++;
        }

        while (rowIndexData < rowIndexDataEnd) {
            *rowIndexData++ = rowOffset++;
        }
    } else {
        TCompressedVectorView rowIndexView(ptr);
        ptr += rowIndexView.GetSizeInWords();

        Count_ = rowIndexView.GetSize();
        RowIndex_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        rowIndexView.UnpackTo(RowIndex_.GetData());

        auto rowIndexData = RowIndex_.GetData();
        auto rowIndexDataEnd = rowIndexData + Count_;
        while (rowIndexData + 4 < rowIndexDataEnd) {
            *rowIndexData++ += rowOffset;
            *rowIndexData++ += rowOffset;
            *rowIndexData++ += rowOffset;
            *rowIndexData++ += rowOffset;
        }

        while (rowIndexData < rowIndexDataEnd) {
            *rowIndexData++ += rowOffset;
        }
    }

    RowIndex_[Count_] = meta->ChunkRowCount;

    return ptr;
}

void TScanKeyIndexExtractor::InitNull()
{
    Count_ = 1;
    RowIndex_.Resize(2);
    RowIndex_[0] = 0;
    RowIndex_[1] = std::numeric_limits<ui32>::max();
    SegmentRowLimit_ = std::numeric_limits<ui32>::max();
}

template <class T>
void DoInitSegment(
    TScanIntegerExtractor<T>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers)
{
    data = value->Init(meta, data, tmpBuffers);
    base->Init(meta, data, IsDense(meta->Type));
}

// Instantiate template function.
template
void DoInitSegment<i64>(
    TScanIntegerExtractor<i64>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

template
void DoInitSegment<ui64>(
    TScanIntegerExtractor<ui64>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

void DoInitSegment(
    TScanDataExtractor<EValueType::Double>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers)
{
    data = value->Init(meta, data, tmpBuffers);
    base->Init(meta, data, true);
}

void DoInitSegment(
    TScanDataExtractor<EValueType::Boolean>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers)
{
    data = value->Init(meta, data, tmpBuffers);
    base->Init(meta, data, true);
}

void DoInitSegment(
    TScanBlobExtractor* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers)
{
    data = base->Init(meta, data, IsDense(meta->Type));
    value->Init(meta, data, tmpBuffers);
}

const ui64* TScanVersionExtractor<true>::Init(const ui64* ptr)
{
    TCompressedVectorView writeTimestampIdsView(ptr);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto tsCount = writeTimestampIdsView.GetSize();
    WriteTimestampIds_.Resize(tsCount, GetRefCountedTypeCookie<TWriteIdsTag>());
    writeTimestampIdsView.UnpackTo(WriteTimestampIds_.GetData());

    AggregateBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(tsCount);

    return ptr;
}

const ui64* TScanVersionExtractor<false>::Init(const ui64* ptr)
{
    TCompressedVectorView writeTimestampIdsView(ptr);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto tsCount = writeTimestampIdsView.GetSize();
    WriteTimestampIds_.Resize(tsCount, GetRefCountedTypeCookie<TWriteIdsTag>());
    writeTimestampIdsView.UnpackTo(WriteTimestampIds_.GetData());

    return ptr;
}

const ui64* TScanMultiValueIndexExtractor::Init(
    const TMetaBase* meta,
    const TDenseMeta* denseMeta,
    const ui64* ptr,
    bool dense, // TODO: Merge with denseMeta ?
    TTmpBuffers* tmpBuffers)
{
    SegmentRowLimit_ = meta->ChunkRowCount;

    ui32 rowOffset = meta->ChunkRowCount - meta->RowCount;

    auto& offsets = tmpBuffers->Offsets;
    ptr += UnpackBitVector(ptr, &offsets);

    if (dense) {
        ui32 expectedPerRow = denseMeta->ExpectedPerRow;

        auto perRowDiff = offsets.data();
        ui32 rowCount = offsets.size();
        ui32 valueCount = expectedPerRow * rowCount + ZigZagDecode32(perRowDiff[rowCount - 1]);

        auto rowToValue = RowToValue_.Resize(valueCount + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        ui32 rowIndex = 0;
        ui32 valueOffset = 0;
#define ITERATION { \
            ui32 nextOffset = expectedPerRow * (rowIndex + 1) + ZigZagDecode32(perRowDiff[rowIndex]); \
            if (nextOffset - valueOffset) { \
                *rowToValue++ = {rowOffset + rowIndex, valueOffset}; \
            }   \
            valueOffset = nextOffset; \
            ++rowIndex; \
        }

#ifdef UNROLL_LOOPS
        while (rowIndex + 4 < rowCount) {
            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }
#endif
        while (rowIndex < rowCount) {
            ITERATION
        }

#undef ITERATION

        SegmentRowLimit_ = rowOffset + rowCount;
        YT_VERIFY(meta->ChunkRowCount == SegmentRowLimit_);
        YT_VERIFY(valueOffset == valueCount);
        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {SegmentRowLimit_, valueCount};

        IndexCount_ = rowToValue - RowToValue_.GetData();
    } else {
        auto rowIndexes = offsets.data();
        ui32 count = offsets.size();

        auto rowToValue = RowToValue_.Resize(count + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        // Init with sentinel row index.
        auto rowIndex = SegmentRowLimit_;
        for (ui32 valueOffset = 0; valueOffset < count; ++valueOffset) {
            if (rowIndexes[valueOffset] != rowIndex) {
                rowIndex = rowIndexes[valueOffset];
                *rowToValue++ = {rowOffset + rowIndex, valueOffset};
            }
        }

        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {SegmentRowLimit_, count};

        IndexCount_ = rowToValue - RowToValue_.GetData();
    }

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
