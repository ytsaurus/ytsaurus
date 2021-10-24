#ifndef SEGMENT_READERS_INL_H_
#error "Direct inclusion of this file is not allowed, include segment_readers.h"
// For the sake of sane code completion.
#include "segment_readers.h"
#endif
#undef SEGMENT_READERS_INL_H_

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

template <EValueType Type, class TExtractor>
void DoInitKeySegment(
    TExtractor* extractor,
    const TMetaBase* meta,
    const ui64* ptr,
    TTmpBuffers* tmpBuffers)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = extractor->InitIndex(meta, ptr, IsDense(meta->Type));
        extractor->InitData(meta, ptr, tmpBuffers);
    } else {
        ptr = extractor->InitData(meta, ptr, tmpBuffers);
        bool dense = Type == EValueType::Double || Type == EValueType::Boolean || IsDense(meta->Type);
        extractor->InitIndex(meta, ptr, dense);
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
    return std::make_pair(WriteTimestampOffsets_[position], WriteTimestampOffsets_[position + 1]);
}

std::pair<ui32, ui32> TScanTimestampExtractor::GetDeleteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - RowOffset_;
    YT_ASSERT(rowIndex < SegmentRowLimit_);
    return std::make_pair(DeleteTimestampOffsets_[position], DeleteTimestampOffsets_[position + 1]);
}

TRange<TTimestamp> TScanTimestampExtractor::GetWriteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* /*memoryPool*/) const
{
    auto [begin, end] = GetWriteTimestampsSpan(rowIndex);
    return MakeRange(WriteTimestamps_ + begin, WriteTimestamps_ + end);
}

TRange<TTimestamp> TScanTimestampExtractor::GetDeleteTimestamps(
    ui32 rowIndex,
    TChunkedMemoryPool* /*memoryPool*/) const
{
    auto [begin, end] = GetDeleteTimestampsSpan(rowIndex);
    return MakeRange(DeleteTimestamps_ + begin, DeleteTimestamps_ + end);
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
    return RowIndex_[position];
}

ui32 TScanKeyIndexExtractor::UpperRowBound(ui32 position) const
{
    return RowIndex_[position + 1];
}

ui32 TScanVersionExtractorBase::AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const
{
    return LinearSearch(valueIdx, valueIdxEnd, [&] (auto position) {
        return WriteTimestampIds_[position] < timestampId;
    });
}

ui32 TScanVersionExtractorBase::AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const
{
    YT_ASSERT(valueIdx != valueIdxEnd);
    while (WriteTimestampIds_[valueIdx] < timestampId && ++valueIdx != valueIdxEnd)
    { }

    return valueIdx;
}

void TScanVersionExtractor<true>::ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const
{
    if (AggregateBits_[position]) {
        value->Flags |= NTableClient::EValueFlags::Aggregate;
    }
    value->Timestamp = timestamps[WriteTimestampIds_[position]];
}

void TScanVersionExtractor<false>::ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const
{
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

////////////////////////////////////////////////////////////////////////////////

ui32 TLookupTimestampExtractor::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

void TLookupTimestampExtractor::ReadSegment(
    const TMetaBase* meta,
    const char* data,
    TTmpBuffers* /*tmpBuffers*/)
{
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    SegmentRowLimit_ = meta->ChunkRowCount;

    TCompressedVectorView view(reinterpret_cast<const ui64*>(data));

    const auto* timestampMeta = static_cast<const TTimestampMeta*>(meta);

    BaseTimestamp_ = timestampMeta->BaseTimestamp;
    ExpectedDeletesPerRow_ = timestampMeta->ExpectedDeletesPerRow;
    ExpectedWritesPerRow_ = timestampMeta->ExpectedWritesPerRow;

    TimestampsDict_ = view;
    WriteTimestampIds_ = ++view;
    DeleteTimestampIds_ = ++view;
    WriteOffsetDiffs_ = ++view;
    DeleteOffsetDiffs_ = ++view;
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
const ui64* TLookupIntegerExtractor<T>::InitData(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* /*tmpBuffers*/)
{
    BaseValue_ = static_cast<const TIntegerMeta*>(meta)->BaseValue;
    Direct_ = IsDirect(meta->Type);

    Ptr_ = ptr;

    TCompressedVectorView values(ptr);
    ptr += values.GetSizeInWords();

    if (Direct_) {
        ptr += GetBitmapSize(values.GetSize());
    } else {
        ptr += TCompressedVectorView(ptr).GetSizeInWords();
    }

    return ptr;
}

template <class T>
void TLookupIntegerExtractor<T>::InitNullData()
{
    BaseValue_ = 0;
    Direct_ = true;

    static ui64 Data[2] {1, 1};
    Ptr_ = &Data[0];
}

template <class T>
void TLookupIntegerExtractor<T>::ExtractDict(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;
    TCompressedVectorView values(ptr);
    ptr += values.GetSizeInWords();

    TCompressedVectorView ids(ptr);
    ptr += ids.GetSizeInWords();
    ui32 id = ids[position];

    if (id > 0) {
        value->Type = GetValueType<T>();
        ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + values[id - 1]));
    } else {
        value->Type = EValueType::Null;
    }

}

template <class T>
void TLookupIntegerExtractor<T>::ExtractDirect(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;
    TCompressedVectorView values(ptr);
    ptr += values.GetSizeInWords();

    TBitmap nullBits(ptr);
    ptr += GetBitmapSize(values.GetSize());

    bool nullBit = nullBits[position];
    value->Type = nullBit ? EValueType::Null : GetValueType<T>();
    ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + values[position]));
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

const ui64* TLookupDataExtractor<EValueType::Double>::InitData(const TMetaBase* /*meta*/, const ui64* ptr, TTmpBuffers* /*tmpBuffers*/)
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
    ptr += GetBitmapSize(count);

    bool nullBit = nullBits[position];
    value->Type = nullBit ? EValueType::Null : EValueType::Double;
    ReadUnversionedValueData(value, items[position]);
}

const ui64* TLookupDataExtractor<EValueType::Boolean>::InitData(const TMetaBase* /*meta*/, const ui64* ptr, TTmpBuffers* /*tmpBuffers*/)
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

TLookupBlobExtractor::TLookupBlobExtractor(EValueType type)
    : Type_(type)
{ }

void TLookupBlobExtractor::InitData(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* /*tmpBuffers*/)
{
    ExpectedLength_ = static_cast<const TBlobMeta*>(meta)->ExpectedLength;
    Direct_ = IsDirect(meta->Type);
    Ptr_ = ptr;
}

void TLookupBlobExtractor::InitNullData()
{
    ExpectedLength_ = 0;
    Direct_ = true;
    static ui64 Data[2] {1, 1};
    Ptr_ = &Data[0];
}

void TLookupBlobExtractor::ExtractDict(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;

    // Dict: [Ids] [Offsets] [Data]
    TCompressedVectorView ids(ptr);
    ptr += ids.GetSizeInWords();

    TCompressedVectorView offsets(ptr);
    ptr += offsets.GetSizeInWords();

    auto* data = reinterpret_cast<const char*>(ptr);

    ui32 id = ids[position];

    if (id > 0) {
        value->Type = Type_;
        ReadUnversionedValueData(value, GetBlob(offsets, data, id - 1));
    } else {
        value->Type = EValueType::Null;
    }
}

void TLookupBlobExtractor::ExtractDirect(TUnversionedValue* value, ui32 position) const
{
    auto ptr = Ptr_;

    // Direct: [Offsets] [IsNullBits] [Data]
    TCompressedVectorView offsets(ptr);
    ptr += offsets.GetSizeInWords();

    TBitmap nullBits(ptr);
    ptr += GetBitmapSize(offsets.GetSize());

    auto* data = reinterpret_cast<const char*>(ptr);

    if (nullBits[position]) {
        value->Type = EValueType::Null;
    } else {
        value->Type = Type_;
        ReadUnversionedValueData(value, GetBlob(offsets, data, position));
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

TStringBuf TLookupBlobExtractor::GetBlob(
    TCompressedVectorView offsets,
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

const ui64* TLookupKeyIndexExtractor::InitIndex(const TMetaBase* meta, const ui64* ptr, bool dense)
{
    Ptr_ = ptr;
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    RowLimit_ = meta->ChunkRowCount;
    Dense_ = dense;

    if (!Dense_) {
        TCompressedVectorView rowIndexes(ptr);
        ptr += rowIndexes.GetSizeInWords();
    }

    return ptr;
}

void TLookupKeyIndexExtractor::InitNullIndex()
{
    static ui64 Data[2] = {1};
    Ptr_ = &Data[0];
    RowOffset_ = 0;
    RowLimit_ = std::numeric_limits<ui32>::max();
    Dense_ = false;
}

void TLookupKeyIndexExtractor::Reset()
{
    Ptr_ = nullptr;
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
        TCompressedVectorView rowIndexes(Ptr_);
        return rowIndexes.GetSize();
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
        TCompressedVectorView rowIndexes(Ptr_);
        return rowIndexes[position] + RowOffset_;
    }
}

ui32 TLookupKeyIndexExtractor::UpperRowBound(ui32 position) const
{
    if (Dense_) {
        return RowOffset_ + position + 1;
    } else {
        TCompressedVectorView rowIndexes(Ptr_);
        auto count = rowIndexes.GetSize();
        return position + 1 == count
            ? RowLimit_
            : rowIndexes[position + 1] + RowOffset_;
    }
}

////////////////////////////////////////////////////////////////////////////////

const ui64* TLookupMultiValueIndexExtractor::InitIndex(
    const TMetaBase* meta,
    const TDenseMeta* denseMeta,
    const ui64* ptr,
    bool dense)
{
    YT_VERIFY(ptr);
    Ptr_ = ptr;
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    RowLimit_ = meta->ChunkRowCount;
    Dense_ = dense;

    if (dense) {
        ExpectedPerRow_ = denseMeta->ExpectedPerRow;
    } else {
        ExpectedPerRow_ = 0;
    }

    TCompressedVectorView offsets(ptr);
    ptr += offsets.GetSizeInWords();

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
    TCompressedVectorView perRowDiff(Ptr_);
    return GetOffset(perRowDiff, ExpectedPerRow_, indexPosition);
}

ui32 TLookupMultiValueIndexExtractor::SkipToSparse(ui32 rowIndex, ui32 position) const
{
    YT_ASSERT(rowIndex >= RowOffset_);

    TCompressedVectorView rowIndexes(Ptr_);
    ui32 count = rowIndexes.GetSize();
    position = ExponentialSearch(position, count, [&] (auto position) {
        return static_cast<ui32>(rowIndexes[position]) < (rowIndex - RowOffset_);
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

template <bool Aggregate>
const ui64* TLookupVersionExtractor<Aggregate>::InitVersion(const ui64* ptr)
{
    Ptr_ = ptr;

    TCompressedVectorView writeTimestampIdsView(ptr);
    ptr += writeTimestampIdsView.GetSizeInWords();

    if constexpr (Aggregate) {
        auto timestampCount = writeTimestampIdsView.GetSize();
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
    TCompressedVectorView writeTimestampIdsView(Ptr_);

    if constexpr (Aggregate) {
        TBitmap aggregateBits(Ptr_ + writeTimestampIdsView.GetSizeInWords());
        if (aggregateBits[position]) {
            value->Flags |= NTableClient::EValueFlags::Aggregate;
        }
    }

    // Write position and restore timestamps later.
    value->Timestamp = timestamps[writeTimestampIdsView[position]];
}

template <bool Aggregate>
ui32 TLookupVersionExtractor<Aggregate>::AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const
{
    TCompressedVectorView writeTimestampIdsView(Ptr_);

    return LinearSearch(valueIdx, valueIdxEnd, [&] (auto position) {
        return writeTimestampIdsView[position] < timestampId;
    });
}

template <bool Aggregate>
ui32 TLookupVersionExtractor<Aggregate>::AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const
{
    return AdjustIndex(valueIdx, valueIdxEnd, timestampId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
