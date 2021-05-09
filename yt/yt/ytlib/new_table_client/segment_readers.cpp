#include "segment_readers.h"

#define UNROLL_LOOPS

namespace NYT::NNewTableClient {

struct TWriteIdsTag { };
struct TRowToValueTag { };
struct TRowIndexTag { };

////////////////////////////////////////////////////////////////////////////////

void TTimestampExtractor::ReadSegment(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers)
{
    SegmentRowOffset_ = meta.chunk_row_count() - meta.row_count();
    RowCount_ = meta.row_count();
    DoReadSegment(meta.GetExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta), data, tmpBuffers);
}

void TTimestampExtractor::DoReadSegment(const NProto::TTimestampSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers)
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
            WriteTimestamps_[index] = meta.min_timestamp() + timestamps[ids[index]];
        }
    }

    {
        UnpackBitVector(deleteTimestampsData, &ids);
        for (size_t index = 0; index < ids.size(); ++index) {
            DeleteTimestamps_[index] = meta.min_timestamp() + timestamps[ids[index]];
        }
    }

    {
        UnpackBitVector(writeTimestampsIndexData, &offsets);
        WriteTimestampOffsets_[0] = 0;

        auto expectedCount = meta.expected_writes_per_row();
        for (size_t index = 0; index < offsets.size(); ++index) {
            ui32 valueIndex = expectedCount * (index + 1) + ZigZagDecode32(offsets[index]);
            WriteTimestampOffsets_[index + 1] = valueIndex;
        }
    }

    {
        UnpackBitVector(deleteTimestampsIndexData, &offsets);
        DeleteTimestampOffsets_[0] = 0;

        auto expectedCount = meta.expected_deletes_per_row();
        for (size_t index = 0; index < offsets.size(); ++index) {
            ui32 valueIndex = expectedCount * (index + 1) + ZigZagDecode32(offsets[index]);
            DeleteTimestampOffsets_[index + 1] = valueIndex;
        }
    }
}

template <class T>
const ui64* TIntegerExtractor<T>::Init(const NProto::TSegmentMeta& meta, const ui64* ptr, TTmpBuffers* tmpBuffers)
{
    auto& values = tmpBuffers->Values;
    auto& ids = tmpBuffers->Ids;

    bool isDirect = GetIsDirect(meta.type());

    auto integerMeta = meta.GetExtension(NProto::TIntegerSegmentMeta::integer_segment_meta);
    auto baseValue = integerMeta.min_value();

    if (isDirect) {
        TCompressedVectorView valuesView(ptr);
        ptr += valuesView.GetSizeInWords();
        size_t valueCount = valuesView.GetSize();

        auto [items] = AllocateCombined<T>(&Holder_, valueCount);
        valuesView.UnpackTo(items);

        IsNullBits_ = TBitmap(ptr);
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

        auto [items, isNullBits] = AllocateCombined<T, TBit>(&Holder_, valueCount, valueCount);

#ifdef UNROLL_LOOPS
        auto tailCount = valueCount % 8;
        auto itemsPtr = items;
        auto itemsPtrEnd = itemsPtr + valueCount - tailCount;

        ui8* isNullData = isNullBits.GetData();
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
            isNullBits.Set(index, id == 0);
            items[index] = ConvertInt<T>(baseValue + values[id]);
        }
#endif

        Items_ = items;
        IsNullBits_ = isNullBits;
    }

    return ptr;
}

template <class T>
void TIntegerExtractor<T>::InitNull()
{
    auto [items, isNullBits] = AllocateCombined<T, TBit>(&Holder_, 1, 1);

    items[0] = 0;
    isNullBits.Set(0, true);

    Items_ = items;
    IsNullBits_ = isNullBits;
}

template
class TIntegerExtractor<i64>;

template
class TIntegerExtractor<ui64>;

const ui64* TValueExtractor<EValueType::Double>::Init(
    const NProto::TSegmentMeta& /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/)
{
    // No dictionary mode for double.
    ui64 count = *ptr++;
    Items_ = reinterpret_cast<const double*>(ptr);
    ptr += count;

    IsNullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}

void TValueExtractor<EValueType::Double>::InitNull()
{
    auto [items, isNullBits] = AllocateCombined<double, TBit>(&Holder_, 1, 1);

    items[0] = 0;
    isNullBits.Set(0, true);

    Items_ = items;
    IsNullBits_ = isNullBits;
}

ui64 TValueExtractor<EValueType::Boolean>::NullBooleanSegmentData;

const ui64* TValueExtractor<EValueType::Boolean>::Init(
    const NProto::TSegmentMeta& /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/)
{
    ui64 count = *ptr++;

    Items_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    IsNullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}

void TValueExtractor<EValueType::Boolean>::InitNull()
{
    TMutableBitmap bitmap(&NullBooleanSegmentData);
    bitmap.Set(0, true);

    Items_ = bitmap;
    IsNullBits_ = bitmap;
}

void TBlobExtractor::Init(const NProto::TSegmentMeta& meta, const ui64* ptr, TTmpBuffers* tmpBuffers)
{
    auto& ids = tmpBuffers->Ids;
    auto& offsets = tmpBuffers->Offsets;

    auto stringMeta = meta.GetExtension(NProto::TStringSegmentMeta::string_segment_meta);

    bool isDirect = GetIsDirect(meta.type());

    if (isDirect) {
        ptr += UnpackBitVector(ptr, &offsets);
        auto valueCount = offsets.size();

        auto [items] = AllocateCombined<TItem>(&Holder_, valueCount);

        ui32 begin = 0;
        for (size_t index = 0; index < valueCount; ++index) {
            ui32 end = stringMeta.expected_length() * (index + 1) + ZigZagDecode32(offsets[index]);
            items[index] = {begin, end};
            begin = end;
        }

        Items_ = items;
        IsNullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(valueCount);
    } else {
        ptr += UnpackBitVector(ptr, &ids);
        auto valueCount = ids.size();
        ptr += UnpackBitVector(ptr, &offsets);

        auto [items, isNullBits] = AllocateCombined<TItem, TBit>(&Holder_, valueCount, valueCount);

        auto getOffset = [&] (ui32 index) -> ui32 {
            if (index > 0) {
                return stringMeta.expected_length() * index + ZigZagDecode32(offsets[index - 1]);
            } else {
                return 0;
            }
        };

        for (size_t index = 0; index < valueCount; ++index) {
            auto id = ids[index];
            isNullBits.Set(index, id == 0);

            if (id > 0) {
                items[index] = {getOffset(id - 1), getOffset(id)};
            }
        }

        Items_ = items;
        IsNullBits_ = isNullBits;
    }

    Data_ = reinterpret_cast<const char*>(ptr);
}

void TBlobExtractor::InitNull()
{
    auto [items, isNullBits] = AllocateCombined<TItem, TBit>(&Holder_, 1, 1);

    items[0] = TItem{0, 0};
    isNullBits.Set(0, true);

    Items_ = items;
    IsNullBits_ = isNullBits;
    Data_ = nullptr;
}

const ui64* TRleBase::Init(const NProto::TSegmentMeta& meta, const ui64* ptr, bool isDense)
{
    SegmentRowLimit_ = meta.chunk_row_count();

    if (isDense) {
        Count_ = meta.row_count();
        RowIndex_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        for (size_t index = 0; index < Count_; ++index) {
            RowIndex_[index] = index;
        }
    } else {
        TCompressedVectorView rowIndexView(ptr);
        ptr += rowIndexView.GetSizeInWords();

        Count_ = rowIndexView.GetSize();
        RowIndex_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        rowIndexView.UnpackTo(RowIndex_.GetData());
    }

    auto rowOffset = meta.chunk_row_count() - meta.row_count();
    for (size_t index = 0; index < Count_; ++index) {
        RowIndex_[index] += rowOffset;
    }

    RowIndex_[Count_] = meta.chunk_row_count();

    return ptr;
}

void TRleBase::InitNull()
{
    Count_ = 1;
    RowIndex_.Resize(2);
    RowIndex_[0] = 0;
    RowIndex_[1] = std::numeric_limits<ui32>::max();
    SegmentRowLimit_ = std::numeric_limits<ui32>::max();
}

// FORMAT:
// DirectDense
// [Values] [IsNullBits]

// DictionaryDense
// [Values] [Ids]

// DirectRle
// Non string: [Values] [IsNullBits] [RowIndex]
// String: [RowIndex] [Offsets] [IsNullBits] [Data]
// Bool: [Values] [IsNullBits]

// DictionaryRle
// Non string: [Values] [Ids] [RowIndex]
// String: [RowIndex] [Ids] [Offsets] [Data]

template <class T>
void DoReadSegment(
    TIntegerExtractor<T>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers)
{
    auto ptr = reinterpret_cast<const ui64*>(data);
    bool isDense = GetIsDense(meta.type());

    ptr = value->Init(meta, ptr, tmpBuffers);
    base->Init(meta, ptr, isDense);
}

// Instantiate template function.
template
void DoReadSegment<i64>(
    TIntegerExtractor<i64>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

template
void DoReadSegment<ui64>(
    TIntegerExtractor<ui64>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

void DoReadSegment(
    TValueExtractor<EValueType::Double>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers)
{
    auto ptr = reinterpret_cast<const ui64*>(data);
    ptr = value->Init(meta, ptr, tmpBuffers);
    base->Init(meta, ptr, true);
}

void DoReadSegment(
    TValueExtractor<EValueType::Boolean>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers)
{
    auto ptr = reinterpret_cast<const ui64*>(data);
    ptr = value->Init(meta, ptr, tmpBuffers);
    base->Init(meta, ptr, true);
}

void DoReadSegment(
    TBlobExtractor* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers)
{
    auto ptr = reinterpret_cast<const ui64*>(data);
    bool isDense = GetIsDense(meta.type());

    ptr = base->Init(meta, ptr, isDense);
    value->Init(meta, ptr, tmpBuffers);
}

const ui64* TVersionInfo<true>::Init(const ui64* ptr)
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

const ui64* TVersionInfo<false>::Init(const ui64* ptr)
{
    TCompressedVectorView writeTimestampIdsView(ptr);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto tsCount = writeTimestampIdsView.GetSize();
    WriteTimestampIds_.Resize(tsCount, GetRefCountedTypeCookie<TWriteIdsTag>());
    writeTimestampIdsView.UnpackTo(WriteTimestampIds_.GetData());

    return ptr;
}

const ui64* TMultiValueBase::Init(
    const NProto::TSegmentMeta& meta,
    const ui64* ptr,
    bool isDense,
    TTmpBuffers* tmpBuffers)
{
    SegmentRowLimit_ = meta.chunk_row_count();

    ui32 rowOffset = meta.chunk_row_count() - meta.row_count();

    if (isDense) {
        auto& offsets = tmpBuffers->Offsets;

        ptr += UnpackBitVector(ptr, &offsets);

        auto denseVersionedMeta = meta.GetExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);

        ui32 expectedPerRow = denseVersionedMeta.expected_values_per_row();

        auto diffs = offsets.data();
        ui32 rowCount = offsets.size();
        ui32 valueCount = expectedPerRow * rowCount + ZigZagDecode32(diffs[rowCount - 1]);

        auto rowToValue = RowToValue_.Resize(valueCount + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        ui32 valueOffset = 0;
#ifdef UNROLL_LOOPS
        ui32 rowIndex = 0;
        while (rowIndex + 4 < rowCount) {
#define ITERATION { \
                ui32 nextOffset = expectedPerRow * (rowIndex + 1) + ZigZagDecode32(diffs[rowIndex]); \
                if (nextOffset - valueOffset) { \
                    *rowToValue++ = {rowOffset + rowIndex, valueOffset}; \
                }   \
                valueOffset = nextOffset; \
                ++rowIndex; \
            }

            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }

        while (rowIndex < rowCount) {
            ITERATION
        }
#undef ITERATION

#else
        for (ui32 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            ui32 nextOffset = expectedPerRow * (rowIndex + 1) + ZigZagDecode32(diffs[rowIndex]);
            if (nextOffset - valueOffset) {
                *rowToValue++ = {rowOffset + rowIndex, valueOffset};
            }
            valueOffset = nextOffset;
        }
#endif

        SegmentRowLimit_ = rowOffset + rowCount;
        YT_VERIFY(meta.chunk_row_count() == SegmentRowLimit_);
        YT_VERIFY(valueOffset == valueCount);
        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {SegmentRowLimit_, valueCount};

        IndexCount_ = rowToValue - RowToValue_.GetData();
    } else {
        auto& rowIndexes = tmpBuffers->Offsets;

        ptr += UnpackBitVector(ptr, &rowIndexes);
        ui32 count = rowIndexes.size();

        auto rowToValue = RowToValue_.Resize(count + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        // Init with sentinel row index.
        auto rowIndex = SegmentRowLimit_;
        for (ui32 valueIndex = 0; valueIndex < count; ++valueIndex) {
            if (rowIndexes[valueIndex] != rowIndex) {
                rowIndex = rowIndexes[valueIndex];
                *rowToValue++ = {rowOffset + rowIndex, valueIndex};
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
