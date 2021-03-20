#pragma once
#ifndef SEGMENT_READERS_INL_H_
#error "Direct inclusion of this file is not allowed, include segment_readers.h"
// For the sake of sane code completion.
#include "segment_readers.h"
#endif
#undef SEGMENT_READERS_INL_H_


namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

inline ui32 TTimestampExtractor::GetSegmentRowLimit() const
{
    return SegmentRowOffset_ + RowCount_;
}

inline std::pair<ui32, ui32> TTimestampExtractor::GetWriteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - SegmentRowOffset_;
    YT_ASSERT(position < RowCount_);
    return std::make_pair(WriteTimestampOffsets_[position], WriteTimestampOffsets_[position + 1]);
}

inline std::pair<ui32, ui32> TTimestampExtractor::GetDeleteTimestampsSpan(ui32 rowIndex) const
{
    auto position = rowIndex - SegmentRowOffset_;
    YT_ASSERT(position < RowCount_);
    return std::make_pair(DeleteTimestampOffsets_[position], DeleteTimestampOffsets_[position + 1]);
}

inline ui32 TTimestampExtractor::GetWriteTimestampsCount(ui32 rowIndex) const
{
    auto [start, end] = GetWriteTimestampsSpan(rowIndex);
    return end - start;
}

inline ui32 TTimestampExtractor::GetDeleteTimestampsCount(ui32 rowIndex) const
{
    auto [start, end] = GetDeleteTimestampsSpan(rowIndex);
    return end - start;
}

inline const TTimestamp* TTimestampExtractor::GetWriteTimestamps() const
{
    return WriteTimestamps_;
}

inline const TTimestamp* TTimestampExtractor::GetDeleteTimestamps() const
{
    return DeleteTimestamps_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline void TIntegerExtractor<T>::Extract(TUnversionedValue* value, ui32 index) const
{
    bool isNull = IsNullBits_[index];
    value->Type = isNull ? EValueType::Null : GetValueType<T>();
    ReadUnversionedValueData(value, Items_[index]);
}

template <class T>
inline TUnversionedValue TIntegerExtractor<T>::operator[] (ui32 index) const
{
    TUnversionedValue result;
    Extract(&result, index);
    return result;
}

inline void TValueExtractor<EValueType::Double>::Extract(TUnversionedValue* value, ui32 index) const
{
    bool isNull = IsNullBits_[index];
    value->Type = isNull ? EValueType::Null : EValueType::Double;
    ReadUnversionedValueData(value, Items_[index]);
}

inline TUnversionedValue TValueExtractor<EValueType::Double>::operator[] (ui32 index) const
{
    TUnversionedValue result;
    Extract(&result, index);
    return result;
}

inline void TValueExtractor<EValueType::Boolean>::Extract(TUnversionedValue* value, ui32 index) const
{
    bool isNull = IsNullBits_[index];
    value->Type = isNull ? EValueType::Null : EValueType::Boolean;
    ReadUnversionedValueData(value, Items_[index]);
}

inline TUnversionedValue TValueExtractor<EValueType::Boolean>::operator[] (ui32 index) const
{
    TUnversionedValue result;
    Extract(&result, index);
    return result;
}

inline void TBlobExtractor::Extract(TUnversionedValue* value, ui32 index) const
{
    bool isNull = IsNullBits_[index];
    value->Type = isNull ? EValueType::Null : Type_;

    auto [begin, end] = Items_[index];
    ReadUnversionedValueData(value, TStringBuf(Data_ + begin, Data_ + end));
}

inline TUnversionedValue TBlobExtractor::operator[] (ui32 index) const
{
    TUnversionedValue result;
    Extract(&result, index);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline void TRleBase::Reset()
{
    Count_ = 0;
    SegmentRowLimit_ = 0;
}

inline ui32 TRleBase::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

// Skip is allowed till SegmentRowLimit.
inline ui32 TRleBase::SkipTo(ui32 rowIndex, ui32 position) const
{
    if (Y_UNLIKELY(position >= Count_ || rowIndex < LowerRowBound(position))) {
        position = 0;
    }

    if (Y_LIKELY(rowIndex < UpperRowBound(position))) {
        return position;
    }

    // Skip to rowIndex.
    position = NYT::ExponentialSearch(position + 1, Count_, [&] (auto position) {
        return UpperRowBound(position) <= rowIndex;
    });

    return position;
}

inline ui32 TRleBase::LowerRowBound(ui32 position) const
{
    return RowIndex_[position];
}

inline ui32 TRleBase::UpperRowBound(ui32 position) const
{
    return RowIndex_[position + 1];
}

inline ui32 TVersionInfoBase::AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const
{
    return LinearSearch(valueIdx, valueIdxEnd, [&] (auto index) {
        return WriteTimestampIds_[index] < id;
    });
}

inline ui32 TVersionInfoBase::AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const
{
    YT_ASSERT(valueIdx != valueIdxEnd);
    while (WriteTimestampIds_[valueIdx] < id && ++valueIdx != valueIdxEnd)
    { }

    return valueIdx;
}

inline void TVersionInfo<true>::Extract(TVersionedValue* value, const TTimestamp* timestamps, ui32 index) const
{
    value->Aggregate = AggregateBits_[index];
    // Write index and restore timestamps later.
    value->Timestamp = timestamps[WriteTimestampIds_[index]];
}

inline void TVersionInfo<false>::Extract(TVersionedValue* value, const TTimestamp* timestamps, ui32 index) const
{
    value->Aggregate = false;
    // Write index and restore timestamps later.
    value->Timestamp = timestamps[WriteTimestampIds_[index]];
}

inline ui32 TMultiValueBase::GetSegmentRowLimit() const
{
    return SegmentRowLimit_;
}

inline ui32 TMultiValueBase::SkipTo(ui32 rowIndex, ui32 position) const
{
    // Position is a hint only.
    if (Y_UNLIKELY(position >= IndexCount_ || position > 0 && rowIndex <= RowToValue_[position - 1].RowIndex)) {
        position = 0;
    }

    if (Y_LIKELY(rowIndex <= RowToValue_[position].RowIndex)) {
        return position;
    }

    position = NYT::ExponentialSearch(position, IndexCount_, [&] (auto position) {
        return RowToValue_[position].RowIndex < rowIndex;
    });

    return position;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline const ui64* TLookupIntegerExtractor<T>::Init(const ui64* ptr)
{
    Ptr_ = ptr;

    TCompressedVectorView values(ptr);
    ptr += values.GetSizeInWords();

    if (IsDirect_) {
        ptr += GetBitmapSize(values.GetSize());
    } else {
        ptr += TCompressedVectorView(ptr).GetSizeInWords();
    }

    return ptr;
}

template <class T>
inline void TLookupIntegerExtractor<T>::Extract(TUnversionedValue* value, ui32 index) const
{
    auto ptr = Ptr_;
    TCompressedVectorView values(ptr);
    ptr += values.GetSizeInWords();

    if (IsDirect_) {
        TBitmap nullBits(ptr);
        ptr += GetBitmapSize(values.GetSize());

        bool isNull = nullBits[index];
        value->Type = isNull ? EValueType::Null : GetValueType<T>();
        ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + values[index]));
    } else {
        TCompressedVectorView ids(ptr);
        ptr += ids.GetSizeInWords();
        ui32 id = ids[index];

        if (id > 0) {
            value->Type = GetValueType<T>();
            ReadUnversionedValueData(value, ConvertInt<T>(BaseValue_ + values[id - 1]));
        } else {
            value->Type = EValueType::Null;
        }
    }
}

inline const ui64* TLookupDoubleExtractor::Init(const ui64* ptr)
{
    Ptr_ = ptr;

    ui64 count = *ptr++;
    ptr += count;
    ptr += GetBitmapSize(count);

    return ptr;
}

inline void TLookupDoubleExtractor::Extract(TUnversionedValue* value, ui32 index) const
{
    auto ptr = Ptr_;
    ui64 count = *ptr++;
    auto items = reinterpret_cast<const double*>(ptr);
    ptr += count;

    TBitmap isNullBits(ptr);
    ptr += GetBitmapSize(count);

    bool isNull = isNullBits[index];
    value->Type = isNull ? EValueType::Null : EValueType::Double;
    ReadUnversionedValueData(value, items[index]);
}

inline const ui64* TLookupBooleanExtractor::Init(const ui64* ptr)
{
    Ptr_ = ptr;

    ui64 count = *ptr++;
    ptr += GetBitmapSize(count);
    ptr += GetBitmapSize(count);

    return ptr;
}

inline void TLookupBooleanExtractor::Extract(TUnversionedValue* value, ui32 index) const
{
    auto ptr = Ptr_;
    ui64 count = *ptr++;
    TBitmap items(ptr);
    ptr += GetBitmapSize(count);

    TBitmap isNullBits(ptr);
    ptr += GetBitmapSize(count);

    bool isNull = isNullBits[index];
    value->Type = isNull ? EValueType::Null : EValueType::Boolean;
    ReadUnversionedValueData(value, items[index]);
}

inline void TLookupBlobExtractor::Init(const ui64* ptr)
{
    Ptr_ = ptr;
}

inline void TLookupBlobExtractor::Extract(TUnversionedValue* value, ui32 index) const
{
    auto ptr = Ptr_;

    auto getBlob = [&] (TCompressedVectorView offsets, const char* data, ui32 index) {
        auto getOffset = [&] (ui32 index) -> ui32 {
            if (index > 0) {
                return ExpectedLength_ * index + ZigZagDecode32(offsets[index - 1]);
            } else {
                return 0;
            }
        };

        return TStringBuf(data + getOffset(index), data + getOffset(index + 1));
    };

    // Direct: [Offsets] [IsNullBits] [Data]
    // Dict: [Ids] [Offsets] [Data]
    if (IsDirect_) {
        TCompressedVectorView offsets(ptr);
        ptr += offsets.GetSizeInWords();

        TBitmap nullBits(ptr);
        ptr += GetBitmapSize(offsets.GetSize());

        auto* data = reinterpret_cast<const char*>(ptr);

        if (nullBits[index]) {
            value->Type = EValueType::Null;
        } else {
            value->Type = Type_;
            ReadUnversionedValueData(value, getBlob(offsets, data, index));
        }
    } else {
        TCompressedVectorView ids(ptr);
        ptr += ids.GetSizeInWords();
        TCompressedVectorView offsets(ptr);
        ptr += offsets.GetSizeInWords();

        auto* data = reinterpret_cast<const char*>(ptr);

        ui32 id = ids[index];

        if (id > 0) {
            value->Type = Type_;
            ReadUnversionedValueData(value, getBlob(offsets, data, id - 1));
        } else {
            value->Type = EValueType::Null;
        }
    }
}

inline const ui64* TLookupIndexReader::Init(const ui64* ptr)
{
    Ptr_ = ptr;

    if (!IsDense_) {
        TCompressedVectorView rowIndexes(ptr);
        ptr += rowIndexes.GetSizeInWords();
    }

    return ptr;
}

inline TReadSpan TLookupIndexReader::GetRowIndex(ui32 position) const
{
    if (IsDense_) {
        return {RowOffset_ + position, RowOffset_ + position + 1};
    } else {
        TCompressedVectorView rowIndexes(Ptr_);
        auto count = rowIndexes.GetSize();

        return {
            static_cast<ui32>(rowIndexes[position]),
            (position == count ? RowLimit_ : static_cast<ui32>(rowIndexes[position + 1]))};
    }
}

inline ui32 TLookupIndexReader::GetCount() const
{
    if (IsDense_) {
        return RowLimit_ - RowOffset_;
    } else {
        TCompressedVectorView rowIndexes(Ptr_);
        return rowIndexes.GetSize();
    }
}

inline ui32 TLookupIndexReader::SkipTo(ui32 rowIndex, ui32 position) const
{
    YT_VERIFY(rowIndex >= RowOffset_);

    if (IsDense_) {
        return rowIndex - RowOffset_;
    } else {
        TCompressedVectorView rowIndexes(Ptr_);

        position = BinarySearch(position, rowIndexes.GetSize(), [&] (auto position) {
            return static_cast<ui32>(rowIndexes[position]) < (rowIndex - RowOffset_);
        });

        return position;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
