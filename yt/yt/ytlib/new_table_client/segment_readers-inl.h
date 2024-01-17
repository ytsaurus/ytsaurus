#ifndef SEGMENT_READERS_INL_H_
#error "Direct inclusion of this file is not allowed, include segment_readers.h"
// For the sake of sane code completion.
#include "segment_readers.h"
#endif
#undef SEGMENT_READERS_INL_H_

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

template <>
Y_FORCE_INLINE ui64 ConvertInt<ui64>(ui64 value)
{
    return value;
}

template <>
Y_FORCE_INLINE i64 ConvertInt<i64>(ui64 value)
{
    return ZigZagDecode64(value);
}

template <class TDiffs>
ui32 GetOffsetNonZero(const TDiffs& diffs, ui32 expected, ui32 position)
{
    return expected * position + ZigZagDecode32(diffs[position - 1]);
}

template <class TDiffs>
ui32 GetOffset(const TDiffs& diffs, ui32 expected, ui32 position)
{
    return position > 0
        ? GetOffsetNonZero(diffs, expected, position)
        : 0;
}

////////////////////////////////////////////////////////////////////////////////

template <class TExtractor, EValueType Type>
void DoInitRangesKeySegment(
    TExtractor* extractor,
    const TKeyMeta<Type>* meta,
    const ui64* ptr,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = extractor->InitIndex(meta, ptr, newMeta);
        extractor->InitData(meta, ptr, tmpBuffers, newMeta);
    } else {
        ptr = extractor->InitData(meta, ptr, tmpBuffers, newMeta);
        extractor->InitIndex(meta, ptr, newMeta);
    }
}

template <class TExtractor, EValueType Type>
void DoInitRangesKeySegment(
    TExtractor* extractor,
    const TKeyMeta<Type>* meta,
    const ui64* ptr,
    ui32 rowOffset,
    TMutableRange<TReadSpan> spans,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = extractor->InitIndex(meta, ptr, rowOffset, spans, tmpBuffers, newMeta);
        extractor->InitData(
            meta,
            ptr,
            tmpBuffers->DataSpans,
            extractor->GetCount(),
            tmpBuffers,
            meta->ChunkRowCount,
            newMeta);
    } else {
        // For partial unpacking index must be always initialized before data.
        auto indexDataPtr = extractor->GetEndPtr(meta, ptr, newMeta);
        extractor->InitIndex(meta, indexDataPtr, rowOffset, spans, tmpBuffers, newMeta);
        ptr = extractor->InitData(
            meta,
            ptr,
            tmpBuffers->DataSpans,
            extractor->GetCount(),
            tmpBuffers,
            meta->ChunkRowCount,
            newMeta);
    }
}

template <bool NewMeta, class TExtractor, EValueType Type>
void DoInitLookupKeySegment(TExtractor* extractor, const TKeyMeta<Type>* meta, const ui64* ptr)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = extractor->template InitIndex<NewMeta>(meta, ptr);
        extractor->template InitData<NewMeta>(meta, ptr);
    } else {
        ptr = extractor->template InitData<NewMeta>(meta, ptr);
        extractor->template InitIndex<NewMeta>(meta, ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ReadUnversionedValueData(TUnversionedValue* value, ui64 data)
{
    value->Data.Uint64 = data;
}

void ReadUnversionedValueData(TUnversionedValue* value, i64 data)
{
    value->Data.Int64 = data;
}

void ReadUnversionedValueData(TUnversionedValue* value, double data)
{
    value->Data.Double = data;
}

void ReadUnversionedValueData(TUnversionedValue* value, bool data)
{
    value->Data.Uint64 = 0;
    value->Data.Boolean = data;
}

void ReadUnversionedValueData(TUnversionedValue* value, TStringBuf data)
{
    value->Length = data.Size();
    value->Data.String = data.Data();
}

////////////////////////////////////////////////////////////////////////////////

ui32 TScanTimestampExtractor::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

std::pair<ui32, ui32> TScanTimestampExtractor::GetWriteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - RowOffset_;
    YT_ASSERT(rowIndex < SegmentRowLimit_);
    return {WriteTimestampOffsets_[position], WriteTimestampOffsets_[position + 1]};
}

std::pair<ui32, ui32> TScanTimestampExtractor::GetDeleteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - RowOffset_;
    YT_ASSERT(rowIndex < SegmentRowLimit_);
    return {DeleteTimestampOffsets_[position], DeleteTimestampOffsets_[position + 1]};
}

TRange<TTimestamp> TScanTimestampExtractor::GetWriteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* /*memoryPool*/) const
{
    auto [begin, end] = GetWriteTimestampsSpan(rowIndex);
    return MakeRange(WriteTimestamps_.GetData() + begin, WriteTimestamps_.GetData() + end);
}

TRange<TTimestamp> TScanTimestampExtractor::GetDeleteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* /*memoryPool*/) const
{
    auto [begin, end] = GetDeleteTimestampsSpan(rowIndex);
    return MakeRange(DeleteTimestamps_.GetData() + begin, DeleteTimestamps_.GetData() + end);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TScanIntegerExtractor<T>::Extract(TUnversionedValue* value, ui32 position) const
{
    bool nullBit = NullBits_[position];
    value->Type = nullBit ? EValueType::Null : GetValueType<T>();
    ReadUnversionedValueData(value, Items_[position]);
}

void TScanDataExtractor<EValueType::Double>::Extract(TUnversionedValue* value, ui32 position) const
{
    bool nullBit = NullBits_[position];
    value->Type = nullBit ? EValueType::Null : EValueType::Double;
    ReadUnversionedValueData(value, Items_[position]);
}

void TScanDataExtractor<EValueType::Boolean>::Extract(TUnversionedValue* value, ui32 position) const
{
    bool nullBit = NullBits_[position];
    value->Type = nullBit ? EValueType::Null : EValueType::Boolean;
    ReadUnversionedValueData(value, Items_[position]);
}

TScanBlobExtractor::TScanBlobExtractor(EValueType type)
    : Type_(type)
{ }

void TScanBlobExtractor::Extract(TUnversionedValue* value, ui32 position) const
{
    bool nullBit = NullBits_[position];
    value->Type = nullBit ? EValueType::Null : Type_;

    auto [begin, end] = Items_[position];
    ReadUnversionedValueData(value, TStringBuf(Data_ + begin, Data_ + end));
}

TScanDataExtractor<EValueType::String>::TScanDataExtractor()
    : TScanBlobExtractor(EValueType::String)
{ }

TScanDataExtractor<EValueType::Composite>::TScanDataExtractor()
    : TScanBlobExtractor(EValueType::Composite)
{ }

TScanDataExtractor<EValueType::Any>::TScanDataExtractor()
    : TScanBlobExtractor(EValueType::Any)
{ }

////////////////////////////////////////////////////////////////////////////////

void TScanKeyIndexExtractor::Reset()
{
    Count_ = 0;
    SegmentRowLimit_ = 0;
}

ui32 TScanKeyIndexExtractor::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

ui32 TScanKeyIndexExtractor::GetCount() const
{
    return Count_;
}

// Skip is allowed till SegmentRowLimit.
ui32 TScanKeyIndexExtractor::SkipTo(ui32 rowIndex, ui32 position) const
{
    YT_ASSERT(position < GetCount() && rowIndex >= LowerRowBound(position));

    if (Y_LIKELY(rowIndex < UpperRowBound(position))) {
        return position;
    }

    // Skip to rowIndex.
    position = ExponentialSearch(position, GetCount(), [&] (auto position) {
        return UpperRowBound(position) <= rowIndex;
    });

    return position;
}

ui32 TScanKeyIndexExtractor::LowerRowBound(ui32 position) const
{
    return RowIndexes_[position];
}

ui32 TScanKeyIndexExtractor::UpperRowBound(ui32 position) const
{
    return RowIndexes_[position + 1];
}

template <bool Aggregate>
ui32 TScanVersionExtractor<Aggregate>::AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const
{
    return LinearSearch(valueIdx, valueIdxEnd, [&] (auto position) {
        return WriteTimestampIds_[position] < timestampId;
    });
}

template <bool Aggregate>
ui32 TScanVersionExtractor<Aggregate>::AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const
{
    YT_ASSERT(valueIdx != valueIdxEnd);
    while (WriteTimestampIds_[valueIdx] < timestampId && ++valueIdx != valueIdxEnd)
    { }

    return valueIdx;
}

template <bool Aggregate>
void TScanVersionExtractor<Aggregate>::ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const
{
    if constexpr (Aggregate) {
        if (this->AggregateBits_[position]) {
            value->Flags |= NTableClient::EValueFlags::Aggregate;
        }
    }

    value->Timestamp = timestamps[WriteTimestampIds_[position]];
}

ui32 TScanMultiValueIndexExtractor::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

ui32 TScanMultiValueIndexExtractor::SkipTo(ui32 rowIndex, ui32 position) const
{
    // Position can point to end of segment.
    YT_ASSERT(position <= IndexCount_ && (position == 0 || RowToValue_[position - 1].RowIndex <= rowIndex));

    if (Y_LIKELY(rowIndex <= RowToValue_[position].RowIndex)) {
        return position;
    }

    position = ExponentialSearch(position, IndexCount_, [&] (auto position) {
        return RowToValue_[position].RowIndex < rowIndex;
    });

    return position;
}

ui32 TScanMultiValueIndexExtractor::GetValueCount() const
{
    return RowToValue_[IndexCount_].ValueOffset;
}

////////////////////////////////////////////////////////////////////////////////

template <bool NewMeta>
void InitCompressedView(TCompressedVectorView* view, const ui64* ptr, ui32 size, ui8 width)
{
    if constexpr (NewMeta) {
        *view = TCompressedVectorView(ptr, size, width);
    } else {
        *view = TCompressedVectorView(ptr);
    }
}

template <bool NewMeta>
void InitCompressedView(TCompressedVectorView32* view, const ui64* ptr, ui32 size, ui8 width)
{
    if constexpr (NewMeta) {
        *view = TCompressedVectorView32(ptr, size, width);
    } else {
        *view = TCompressedVectorView32(ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <bool NewMeta>
void TLookupTimestampExtractor::InitSegment(const TTimestampMeta* meta, const char* data)
{
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    SegmentRowLimit_ = meta->ChunkRowCount;

    BaseTimestamp_ = meta->BaseTimestamp;
    ExpectedDeletesPerRow_ = meta->ExpectedDeletesPerRow;
    ExpectedWritesPerRow_ = meta->ExpectedWritesPerRow;

    const ui64* ptr = reinterpret_cast<const ui64*>(data);

    InitCompressedView<NewMeta>(&TimestampsDict_, ptr, meta->TimestampsDictSize, meta->TimestampsDictWidth);
    ptr += TimestampsDict_.GetSizeInWords();

    InitCompressedView<NewMeta>(&WriteTimestampIds_, ptr, meta->WriteTimestampSize, meta->WriteTimestampWidth);
    ptr += WriteTimestampIds_.GetSizeInWords();

    InitCompressedView<NewMeta>(&DeleteTimestampIds_, ptr, meta->DeleteTimestampSize, meta->DeleteTimestampWidth);
    ptr += DeleteTimestampIds_.GetSizeInWords();

    InitCompressedView<NewMeta>(&WriteOffsetDiffs_, ptr, meta->WriteOffsetDiffsSize, meta->WriteOffsetDiffsWidth);
    ptr += WriteOffsetDiffs_.GetSizeInWords();

    InitCompressedView<NewMeta>(&DeleteOffsetDiffs_, ptr, meta->DeleteOffsetDiffsSize, meta->DeleteOffsetDiffsWidth);
    ptr += DeleteOffsetDiffs_.GetSizeInWords();
}

ui32 TLookupTimestampExtractor::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

std::pair<ui32, ui32> TLookupTimestampExtractor::GetWriteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - RowOffset_;
    auto begin = GetOffset(WriteOffsetDiffs_, ExpectedWritesPerRow_, position);
    auto end = GetOffsetNonZero(WriteOffsetDiffs_, ExpectedWritesPerRow_, position + 1);

    return {begin, end};
}

std::pair<ui32, ui32> TLookupTimestampExtractor::GetDeleteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - RowOffset_;
    auto begin = GetOffset(DeleteOffsetDiffs_, ExpectedDeletesPerRow_, position);
    auto end = GetOffsetNonZero(DeleteOffsetDiffs_, ExpectedDeletesPerRow_, position + 1);

    return {begin, end};
}

TRange<TTimestamp> TLookupTimestampExtractor::GetWriteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* memoryPool) const
{
    auto [begin, end] = GetWriteTimestampsSpan(rowIndex);
    auto count = end - begin;
    auto* timestamps = reinterpret_cast<TTimestamp*>(memoryPool->AllocateAligned(sizeof(TTimestamp) * count));
    auto startTimestamps = timestamps;
    for (auto it = begin; it != end; ++it) {
        *timestamps++ = BaseTimestamp_ + TimestampsDict_[WriteTimestampIds_[it]];
    }

    return MakeRange(startTimestamps, timestamps);
}

TRange<TTimestamp> TLookupTimestampExtractor::GetDeleteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* memoryPool) const
{
    auto [begin, end] = GetDeleteTimestampsSpan(rowIndex);
    auto count = end - begin;
    auto* timestamps = reinterpret_cast<TTimestamp*>(memoryPool->AllocateAligned(sizeof(TTimestamp) * count));
    auto startTimestamps = timestamps;
    for (auto it = begin; it != end; ++it) {
        *timestamps++ = BaseTimestamp_ + TimestampsDict_[DeleteTimestampIds_[it]];
    }

    return MakeRange(startTimestamps, timestamps);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
template <bool NewMeta>
const ui64* TLookupIntegerExtractor<T>::InitData(const TIntegerMeta* meta, const ui64* ptr)
{
    BaseValue_ = meta->BaseValue;
    Direct_ = meta->Direct;

    InitCompressedView<NewMeta>(&Values_, ptr, meta->ValuesSize, meta->ValuesWidth);
    ptr += Values_.GetSizeInWords();

    if (Direct_) {
        U_.NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(Values_.GetSize());
    } else {
        InitCompressedView<NewMeta>(&U_.Ids_, ptr, meta->IdsSize, meta->IdsWidth);
        ptr += U_.Ids_.GetSizeInWords();
    }

    return ptr;
}

template <class T>
void TLookupIntegerExtractor<T>::InitNullData()
{
    BaseValue_ = 0;
    Direct_ = true;

    static ui64 Data = 1;
    Values_ = TCompressedVectorView(&Data);
    U_.NullBits_ = TBitmap(&Data);
}

template <class T>
void TLookupIntegerExtractor<T>::ExtractDict(TUnversionedValue* value, ui32 position) const
{
    ui32 id = U_.Ids_[position];

    if (id > 0) {
        value->Type = GetValueType<T>();
        ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + Values_[id - 1]));
    } else {
        value->Type = EValueType::Null;
    }
}

template <class T>
void TLookupIntegerExtractor<T>::ExtractDirect(TUnversionedValue* value, ui32 position) const
{
    bool nullBit = U_.NullBits_[position];
    value->Type = nullBit ? EValueType::Null : GetValueType<T>();
    ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + Values_[position]));
}

template <class T>
void TLookupIntegerExtractor<T>::Extract(TUnversionedValue* value, ui32 position) const
{
    if (Direct_) {
        ExtractDirect(value, position);
    } else {
        ExtractDict(value, position);
    }
}

template <class T>
void TLookupIntegerExtractor<T>::Prefetch(ui32 position) const
{
    if (Direct_) {
        U_.NullBits_.Prefetch(position);
        Values_.Prefetch(position);
    } else {
        U_.Ids_.Prefetch(position);
    }
}

template <bool NewMeta>
const ui64* TLookupDataExtractor<EValueType::Double>::InitData(const TEmptyMeta* /*meta*/, const ui64* ptr)
{
    Ptr_ = ptr;

    ui64 count = *ptr++;
    ptr += count;
    ptr += GetBitmapSize(count);

    return ptr;
}

void TLookupDataExtractor<EValueType::Double>::InitNullData()
{
    static ui64 Data[3] {1, 0, 1};
    Ptr_ = &Data[0];
}

void TLookupDataExtractor<EValueType::Double>::Extract(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;
    ui64 count = *ptr++;
    auto items = reinterpret_cast<const double*>(ptr);
    ptr += count;

    TBitmap nullBits(ptr);
    //ptr += GetBitmapSize(count);

    bool nullBit = nullBits[position];
    value->Type = nullBit ? EValueType::Null : EValueType::Double;
    ReadUnversionedValueData(value, items[position]);
}

void TLookupDataExtractor<EValueType::Double>::Prefetch(ui32 /*position*/) const
{ }

template <bool NewMeta>
const ui64* TLookupDataExtractor<EValueType::Boolean>::InitData(const TEmptyMeta* /*meta*/, const ui64* ptr)
{
    Ptr_ = ptr;

    ui64 count = *ptr++;
    ptr += GetBitmapSize(count);
    ptr += GetBitmapSize(count);

    return ptr;
}

void TLookupDataExtractor<EValueType::Boolean>::InitNullData()
{
    static ui64 Data[3] {1, 0, 1};
    Ptr_ = &Data[0];
}

void TLookupDataExtractor<EValueType::Boolean>::Extract(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;
    ui64 count = *ptr++;
    TBitmap items(ptr);
    ptr += GetBitmapSize(count);

    TBitmap nullBits(ptr);
    ptr += GetBitmapSize(count);

    bool nullBit = nullBits[position];
    value->Type = nullBit ? EValueType::Null : EValueType::Boolean;
    ReadUnversionedValueData(value, items[position]);
}

void TLookupDataExtractor<EValueType::Boolean>::Prefetch(ui32 /*position*/) const
{ }

TLookupBlobExtractor::TLookupBlobExtractor(EValueType type)
    : Type_(type)
{ }

template <bool NewMeta>
void TLookupBlobExtractor::InitData(const TBlobMeta* meta, const ui64* ptr)
{
    ExpectedLength_ = meta->ExpectedLength;
    Direct_ = meta->Direct;

    if (meta->Direct) {
        InitCompressedView<NewMeta>(&Offsets_, ptr, meta->OffsetsSize, meta->OffsetsWidth);
        ptr += Offsets_.GetSizeInWords();

        U_.NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(Offsets_.GetSize());
    } else {
        InitCompressedView<NewMeta>(&U_.Ids_, ptr, meta->IdsSize, meta->IdsWidth);
        ptr += U_.Ids_.GetSizeInWords();

        InitCompressedView<NewMeta>(&Offsets_, ptr, meta->OffsetsSize, meta->OffsetsWidth);
        ptr += Offsets_.GetSizeInWords();
    }

    Data_ = reinterpret_cast<const char*>(ptr);
}

void TLookupBlobExtractor::InitNullData()
{
    ExpectedLength_ = 0;
    Direct_ = true;
    static ui64 Data[2] {1, 1};
    Offsets_ = TCompressedVectorView32(&Data[0]);
    U_.NullBits_ = TBitmap(&Data[1]);
}

void TLookupBlobExtractor::ExtractDict(TUnversionedValue* value, ui32 position) const
{
    ui32 id = U_.Ids_[position];

    if (id > 0) {
        value->Type = Type_;
        ReadUnversionedValueData(value, GetBlob(Offsets_, Data_, id - 1));
    } else {
        value->Type = EValueType::Null;
    }
}

void TLookupBlobExtractor::ExtractDirect(TUnversionedValue* value, ui32 position) const
{
    if (U_.NullBits_[position]) {
        value->Type = EValueType::Null;
    } else {
        value->Type = Type_;
        ReadUnversionedValueData(value, GetBlob(Offsets_, Data_, position));
    }
}

void TLookupBlobExtractor::Extract(TUnversionedValue* value, ui32 position) const
{
    if (Direct_) {
        ExtractDirect(value, position);
    } else {
        ExtractDict(value, position);
    }
}

void TLookupBlobExtractor::Prefetch(ui32 position) const
{
    if (Direct_) {
        U_.NullBits_.Prefetch(position);
        if (position > 0) {
            Offsets_.Prefetch(position);
        }
    } else {
        U_.Ids_.Prefetch(position);
    }
}

TStringBuf TLookupBlobExtractor::GetBlob(
    TCompressedVectorView32 offsets,
    const char* data,
    ui32 position) const
{
    return TStringBuf(
        data + GetOffset(offsets, ExpectedLength_, position),
        data + GetOffsetNonZero(offsets, ExpectedLength_, position + 1));
}

////////////////////////////////////////////////////////////////////////////////

TLookupDataExtractor<EValueType::String>::TLookupDataExtractor()
    : TLookupBlobExtractor(EValueType::String)
{ }

TLookupDataExtractor<EValueType::Composite>::TLookupDataExtractor()
    : TLookupBlobExtractor(EValueType::Composite)
{ }

TLookupDataExtractor<EValueType::Any>::TLookupDataExtractor()
    : TLookupBlobExtractor(EValueType::Any)
{ }

////////////////////////////////////////////////////////////////////////////////

template <bool NewMeta>
const ui64* TLookupKeyIndexExtractor::InitIndex(const TKeyIndexMeta* meta, const ui64* ptr)
{
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    RowLimit_ = meta->ChunkRowCount;
    Dense_ = meta->Dense;

    if (!Dense_) {
        InitCompressedView<NewMeta>(&RowIndexes_, ptr, meta->RowIndexesSize, meta->RowIndexesWidth);
        ptr += RowIndexes_.GetSizeInWords();
    }

    return ptr;
}

void TLookupKeyIndexExtractor::InitNullIndex()
{
    static ui64 Data[2] = {1};
    RowIndexes_ = TCompressedVectorView(&Data[0]);
    RowOffset_ = 0;
    RowLimit_ = std::numeric_limits<ui32>::max();
    Dense_ = false;
}

void TLookupKeyIndexExtractor::Reset()
{
    RowIndexes_ = TCompressedVectorView();
    RowOffset_ = 0;
    RowLimit_ = 0;
    Dense_ = true;
}

ui32 TLookupKeyIndexExtractor::GetSegmentRowLimit() const
{
    return RowLimit_;
}

ui32 TLookupKeyIndexExtractor::GetCount() const
{
    if (Dense_) {
        return RowLimit_ - RowOffset_;
    } else {
        return RowIndexes_.GetSize();
    }
}

ui32 TLookupKeyIndexExtractor::SkipTo(ui32 rowIndex, ui32 position) const
{
    YT_VERIFY(rowIndex >= RowOffset_);

    if (Dense_) {
        return rowIndex - RowOffset_;
    } else {
        position = ExponentialSearch(position, GetCount(), [&] (auto position) {
            return UpperRowBound(position) <= rowIndex;
        });

        return position;
    }
}

ui32 TLookupKeyIndexExtractor::LowerRowBound(ui32 position) const
{
    if (Dense_) {
        return RowOffset_ + position;
    } else {
        return RowIndexes_[position] + RowOffset_;
    }
}

ui32 TLookupKeyIndexExtractor::UpperRowBound(ui32 position) const
{
    if (Dense_) {
        return RowOffset_ + position + 1;
    } else {
        auto count = RowIndexes_.GetSize();
        return position + 1 == count
            ? RowLimit_
            : RowIndexes_[position + 1] + RowOffset_;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <bool NewMeta>
const ui64* TLookupMultiValueIndexExtractor::InitIndex(
    const TMultiValueIndexMeta* meta,
    const ui64* ptr)
{
    YT_VERIFY(ptr);
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    RowLimit_ = meta->ChunkRowCount;
    Dense_ = meta->IsDense();

    if (Dense_) {
        ExpectedPerRow_ = meta->ExpectedPerRow;
    } else {
        ExpectedPerRow_ = 0;
    }

    InitCompressedView<NewMeta>(&RowIndexesOrPerRowDiffs_, ptr, meta->OffsetsSize, meta->OffsetsWidth);
    ptr += RowIndexesOrPerRowDiffs_.GetSizeInWords();

    return ptr;
}

ui32 TLookupMultiValueIndexExtractor::GetSegmentRowLimit() const
{
    return RowLimit_;
}

ui32 TLookupMultiValueIndexExtractor::SkipToDense(ui32 rowIndex, ui32) const
{
    YT_ASSERT(rowIndex >= RowOffset_);

    auto indexPosition = rowIndex - RowOffset_;
    // As PerRowDiffs.
    return GetOffset(RowIndexesOrPerRowDiffs_, ExpectedPerRow_, indexPosition);
}

ui32 TLookupMultiValueIndexExtractor::SkipToSparse(ui32 rowIndex, ui32 position) const
{
    YT_ASSERT(rowIndex >= RowOffset_);

    // As RowIndexes.
    ui32 count = RowIndexesOrPerRowDiffs_.GetSize();
    position = ExponentialSearch(position, count, [&] (auto position) {
        return static_cast<ui32>(RowIndexesOrPerRowDiffs_[position]) < (rowIndex - RowOffset_);
    });

    return position;
}

ui32 TLookupMultiValueIndexExtractor::SkipTo(ui32 rowIndex, ui32 position) const
{
    if (Dense_) {
        return SkipToDense(rowIndex, position);
    } else {
        return SkipToSparse(rowIndex, position);
    }
}

void TLookupMultiValueIndexExtractor::Prefetch(ui32 rowIndex) const
{
    if (Dense_) {
        auto indexPosition = rowIndex - RowOffset_;
        RowIndexesOrPerRowDiffs_.Prefetch(indexPosition > 0 ? indexPosition - 1 : 0);
    }
}

template <bool Aggregate>
template <bool NewMeta>
const ui64* TLookupVersionExtractor<Aggregate>::InitVersion(const TMultiValueIndexMeta* meta, const ui64* ptr)
{
    InitCompressedView<NewMeta>(&WriteTimestampIds_, ptr, meta->WriteTimestampIdsSize, meta->WriteTimestampIdsWidth);
    ptr += WriteTimestampIds_.GetSizeInWords();

    if constexpr (Aggregate) {
        this->AggregateBits_ = TBitmap(ptr);
        auto timestampCount = WriteTimestampIds_.GetSize();
        ptr += GetBitmapSize(timestampCount);
    }

    return ptr;
}

template <bool Aggregate>
void TLookupVersionExtractor<Aggregate>::ExtractVersion(
    TVersionedValue* value,
    const TTimestamp* timestamps,
    ui32 position) const
{
    if constexpr (Aggregate) {
        if (this->AggregateBits_[position]) {
            value->Flags |= NTableClient::EValueFlags::Aggregate;
        }
    }

    // Write position and restore timestamps later.
    value->Timestamp = timestamps[WriteTimestampIds_[position]];
}

template <bool Aggregate>
ui32 TLookupVersionExtractor<Aggregate>::AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const
{
    return LinearSearch(valueIdx, valueIdxEnd, [&] (auto position) {
        return WriteTimestampIds_[position] < timestampId;
    });
}

template <bool Aggregate>
ui32 TLookupVersionExtractor<Aggregate>::AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const
{
    return AdjustIndex(valueIdx, valueIdxEnd, timestampId);
}

template <bool Aggregate>
void TLookupVersionExtractor<Aggregate>::Prefetch(ui32 valueIdx) const
{
    WriteTimestampIds_.Prefetch(valueIdx);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
