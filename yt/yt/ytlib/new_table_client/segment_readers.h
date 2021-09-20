#pragma once

#include "public.h"
#include "read_span.h"
#include "memory_helpers.h"
#include "prepared_meta.h"

#include <yt/yt/core/misc/bit_packing.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/core/misc/zigzag.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NNewTableClient {

// KEY SEGMENTS FORMAT:
// DirectDense
// [Values] [NullBits]

// DictionaryDense
// [Values] [Ids]

// DirectRle
// Non string: [Values] [NullBits] [RowIndex]
// String: [RowIndex] [Offsets] [NullBits] [Data]
// Bool: [Values] [NullBits]

// DictionaryRle
// Non string: [Values] [Ids] [RowIndex]
// String: [RowIndex] [Ids] [Offsets] [Data]


// VALUE SEGMENTS FORMAT:
// DirectDense
// [DiffPerRow] [TimestampIds] [AggregateBits] [Values] [NullBits]
// DiffPerRow for each row

// DictionaryDense
// [DiffPerRow] [TimestampIds] [AggregateBits] [Values] [Ids]
// DiffPerRow for each row

// DirectSparse
// [RowIndex] [TimestampIds] [AggregateBits] [Values] [NullBits]
// RowIndex for each value

// DictionarySparse
// [RowIndex] [TimestampIds] [AggregateBits] [Values] [Ids]
// TDictionaryString: [Ids] [Offsets] [Data]

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ConvertInt(ui64 value);

////////////////////////////////////////////////////////////////////////////////

// Describes set of column segments in block.
struct TColumnSlice
{
    TRef Block;
    TRef SegmentsMeta;
};

////////////////////////////////////////////////////////////////////////////////

struct TTmpBuffers
{
    std::vector<ui64> Values;
    std::vector<ui32> Ids;
    std::vector<ui32> Offsets;
};

////////////////////////////////////////////////////////////////////////////////

// Scan extractors.

class TScanTimestampExtractor
{
public:
    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    // Skip is allowed till SegmentRowLimit.
    void ReadSegment(const TMetaBase* meta, const char* data, TTmpBuffers* tmpBuffers);

    Y_FORCE_INLINE TTimestamp GetWriteTimestamp(ui32 position) const;
    Y_FORCE_INLINE TTimestamp GetDeleteTimestamp(ui32 position) const;

    Y_FORCE_INLINE std::pair<ui32, ui32> GetWriteTimestampsSpan(ui32 rowIndex) const;
    Y_FORCE_INLINE std::pair<ui32, ui32> GetDeleteTimestampsSpan(ui32 rowIndex) const;

    Y_FORCE_INLINE TRange<TTimestamp> GetWriteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;
    Y_FORCE_INLINE TRange<TTimestamp> GetDeleteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;

private:
    // For each row index value offsets.
    ui32* WriteTimestampOffsets_ = nullptr;
    TTimestamp* WriteTimestamps_ = nullptr;
    ui32* DeleteTimestampOffsets_ = nullptr;
    TTimestamp* DeleteTimestamps_ = nullptr;

    ui32 SegmentRowOffset_ = 0;
    ui32 RowCount_ = 0;

    TMemoryHolder<char> Holder_;

    void DoInitSegment(const TTimestampMeta* meta, const char* data, TTmpBuffers* tmpBuffers);
};

Y_FORCE_INLINE void ReadUnversionedValueData(TUnversionedValue* value, ui64 data);

Y_FORCE_INLINE void ReadUnversionedValueData(TUnversionedValue* value, i64 data);

Y_FORCE_INLINE void ReadUnversionedValueData(TUnversionedValue* value, double data);

Y_FORCE_INLINE void ReadUnversionedValueData(TUnversionedValue* value, bool data);

Y_FORCE_INLINE void ReadUnversionedValueData(TUnversionedValue* value, TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr EValueType GetValueType();

template <>
constexpr EValueType GetValueType<ui64>()
{
    return EValueType::Uint64;
}

template <>
constexpr EValueType GetValueType<i64>()
{
    return EValueType::Int64;
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
class TScanDataExtractor;

template <class T>
class TScanIntegerExtractor
{
public:
    const ui64* Init(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* tmpBuffers);
    void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    const T* Items_ = nullptr;
    TMemoryHolder<char> Holder_;
    TBitmap NullBits_;
};

template <>
class TScanDataExtractor<EValueType::Int64>
    : public TScanIntegerExtractor<i64>
{ };

template <>
class TScanDataExtractor<EValueType::Uint64>
    : public TScanIntegerExtractor<ui64>
{ };

template <>
class TScanDataExtractor<EValueType::Double>
{
public:
    const ui64* Init(const TMetaBase* /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers);
    void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    TMemoryHolder<char> Holder_;
    const double* Items_ = nullptr;
    TBitmap NullBits_;
};

template <>
class TScanDataExtractor<EValueType::Boolean>
{
public:
    const ui64* Init(const TMetaBase* /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers);
    void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    TBitmap Items_;
    TBitmap NullBits_;

    static ui64 NullBooleanSegmentData;
};

class TScanBlobExtractor
{
public:
    Y_FORCE_INLINE explicit TScanBlobExtractor(EValueType type);

    void Init(const TMetaBase* meta, const ui64* ptr, TTmpBuffers* tmpBuffers);
    void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    struct TItem
    {
        ui32 Begin;
        ui32 End;
    };

    TMemoryHolder<char> Holder_;
    const TItem* Items_ = nullptr;
    TBitmap NullBits_;
    const char* Data_ = nullptr;
    EValueType Type_;
};

template <>
class TScanDataExtractor<EValueType::String>
    : public TScanBlobExtractor
{
public:
    Y_FORCE_INLINE TScanDataExtractor();
};

template <>
class TScanDataExtractor<EValueType::Composite>
    : public TScanBlobExtractor
{
public:
    Y_FORCE_INLINE TScanDataExtractor();
};

template <>
class TScanDataExtractor<EValueType::Any>
    : public TScanBlobExtractor
{
public:
    Y_FORCE_INLINE TScanDataExtractor();
};

////////////////////////////////////////////////////////////////////////////////

class TScanKeyIndexExtractor
{
public:
    const ui64* Init(const TMetaBase* meta, const ui64* ptr, bool dense);
    void InitNull();
    Y_FORCE_INLINE void Reset();

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    Y_FORCE_INLINE ui32 GetCount() const;

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 LowerRowBound(ui32 position) const;
    Y_FORCE_INLINE ui32 UpperRowBound(ui32 position) const;

protected:
    TMemoryHolder<ui32> RowIndex_;
    ui32 Count_ = 0;
    ui32 SegmentRowLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void DoInitSegment(
    TScanIntegerExtractor<T>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

void DoInitSegment(
    TScanDataExtractor<EValueType::Double>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

void DoInitSegment(
    TScanDataExtractor<EValueType::Boolean>* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

void DoInitSegment(
    TScanBlobExtractor* value,
    TScanKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data,
    TTmpBuffers* tmpBuffers);

////////////////////////////////////////////////////////////////////////////////

class TScanVersionExtractorBase
{
public:
    Y_FORCE_INLINE ui32 AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const;

    // Micro-optimized version of AdjustIndex.
    // No check valueIdx != valueIdxEnd for initial value of index.
    Y_FORCE_INLINE ui32 AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const;

protected:
    TMemoryHolder<ui16> WriteTimestampIds_;
};

template <bool Aggregate>
class TScanVersionExtractor;

template <>
class TScanVersionExtractor<true>
    : public TScanVersionExtractorBase
{
public:
    const ui64* Init(const ui64* ptr);
    Y_FORCE_INLINE void ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const;

private:
    TBitmap AggregateBits_;
};

template <>
class TScanVersionExtractor<false>
    : public TScanVersionExtractorBase
{
public:
    const ui64* Init(const ui64* ptr);
    Y_FORCE_INLINE void ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexItem
{
    ui32 RowIndex;
    ui32 ValueOffset;
};

class TScanMultiValueIndexExtractor
{
public:
    const ui64* Init(
        const TMetaBase* meta,
        const TDenseMeta* denseMeta,
        const ui64* ptr,
        bool dense,
        TTmpBuffers* tmpBuffers);

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;

protected:
    // Keep IndexCount_ + 1 items to eliminate extra branches in read routines.
    TMemoryHolder<TIndexItem> RowToValue_;

    ui32 IndexCount_ = 0;
    // No need to keep value count. ValueCount is RowToValue[IndexCount]->ValueIndex

    ui32 SegmentRowLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Lookup readers.

class TLookupTimestampExtractor
{
public:
    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE void ReadSegment(const TMetaBase* meta, const char* data, TTmpBuffers* tmpBuffers);

    Y_FORCE_INLINE TTimestamp GetDeleteTimestamp(ui32 position) const;
    Y_FORCE_INLINE TTimestamp GetWriteTimestamp(ui32 position) const;

    Y_FORCE_INLINE std::pair<ui32, ui32> GetWriteTimestampsSpan(ui32 rowIndex) const;
    Y_FORCE_INLINE std::pair<ui32, ui32> GetDeleteTimestampsSpan(ui32 rowIndex) const;

    Y_FORCE_INLINE TRange<TTimestamp> GetWriteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;
    Y_FORCE_INLINE TRange<TTimestamp> GetDeleteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;

protected:
    TTimestamp BaseTimestamp_;
    ui32 ExpectedDeletesPerRow_;
    ui32 ExpectedWritesPerRow_;
    // For each row index value offsets.
    TCompressedVectorView TimestampsDict_;
    TCompressedVectorView WriteTimestampIds_;
    TCompressedVectorView DeleteTimestampIds_;
    TCompressedVectorView WriteOffsetDiffs_;
    TCompressedVectorView DeleteOffsetDiffs_;

    ui32 SegmentRowOffset_ = 0;
    ui32 RowCount_ = 0;

    Y_FORCE_INLINE ui32 GetDeleteTimestampOffset(ui32 rowIndex) const;

    Y_FORCE_INLINE ui32 GetWriteTimestampOffset(ui32 rowIndex) const;
};

template <EValueType Type>
class TLookupDataExtractor;

template <class T>
class TLookupIntegerExtractor
{
public:
    Y_FORCE_INLINE const ui64* Init(const TMetaBase* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNull();
    Y_FORCE_INLINE void ExtractDict(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void ExtractDirect(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
    ui64 BaseValue_ = 0;
    bool Direct_ = true;
};

template <>
class TLookupDataExtractor<EValueType::Int64>
    : public TLookupIntegerExtractor<i64>
{ };

template <>
class TLookupDataExtractor<EValueType::Uint64>
    : public TLookupIntegerExtractor<ui64>
{ };

template <>
class TLookupDataExtractor<EValueType::Double>
{
public:
    Y_FORCE_INLINE const ui64* Init(const TMetaBase* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
};

template <>
class TLookupDataExtractor<EValueType::Boolean>
{
public:
    Y_FORCE_INLINE const ui64* Init(const TMetaBase* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNull();
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
};

class TLookupBlobExtractor
{
public:
    Y_FORCE_INLINE explicit TLookupBlobExtractor(EValueType type);

    Y_FORCE_INLINE void Init(const TMetaBase* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNull();
    Y_FORCE_INLINE void ExtractDict(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void ExtractDirect(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
    ui32 ExpectedLength_ = 0;
    bool Direct_ = true;
    EValueType Type_;

    Y_FORCE_INLINE TStringBuf GetBlob(TCompressedVectorView offsets, const char* data, ui32 position) const;
};

template <>
class TLookupDataExtractor<EValueType::String>
    : public TLookupBlobExtractor
{
public:
    Y_FORCE_INLINE TLookupDataExtractor();
};

template <>
class TLookupDataExtractor<EValueType::Composite>
    : public TLookupBlobExtractor
{
public:
    Y_FORCE_INLINE TLookupDataExtractor();
};

template <>
class TLookupDataExtractor<EValueType::Any>
    : public TLookupBlobExtractor
{
public:
    Y_FORCE_INLINE TLookupDataExtractor();
};

////////////////////////////////////////////////////////////////////////////////

class TLookupKeyIndexExtractor
{
public:
    Y_FORCE_INLINE const ui64* Init(const TMetaBase* meta, const ui64* ptr, bool dense);
    Y_FORCE_INLINE void InitNull();
    Y_FORCE_INLINE void Reset();

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    Y_FORCE_INLINE ui32 GetCount() const;

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 LowerRowBound(ui32 position) const;
    Y_FORCE_INLINE ui32 UpperRowBound(ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
    ui32 RowOffset_ = 0;
    ui32 RowLimit_ = 0;
    bool Dense_ = true;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
Y_FORCE_INLINE void DoInitSegment(
    TLookupIntegerExtractor<T>* value,
    TLookupKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data);

Y_FORCE_INLINE void DoInitSegment(
    TLookupDataExtractor<EValueType::Double>* value,
    TLookupKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data);

Y_FORCE_INLINE void DoInitSegment(
    TLookupDataExtractor<EValueType::Boolean>* value,
    TLookupKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data);

Y_FORCE_INLINE void DoInitSegment(
    TLookupBlobExtractor* value,
    TLookupKeyIndexExtractor* base,
    const TMetaBase* meta,
    const ui64* data);

////////////////////////////////////////////////////////////////////////////////

class TLookupMultiValueIndexExtractor
{
public:
    Y_FORCE_INLINE const ui64* Init(
        const TMetaBase* meta,
        const TDenseMeta* denseMeta,
        const ui64* ptr,
        bool dense);

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    // Returns value offset.
    // For sparse index and value offset are equal.
    // For dense initial offset hint is not used.

    Y_FORCE_INLINE ui32 SkipToDense(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 SkipToSparse(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;

protected:
    const ui64* Ptr_ = nullptr;
    ui32 RowOffset_ = 0;
    ui32 RowLimit_ = 0;
    ui32 ExpectedPerRow_ = 0;
    bool Dense_ = false;
};

template <bool Aggregate>
class TLookupVersionExtractor
{
public:
    Y_FORCE_INLINE const ui64* Init(const ui64* ptr);

    Y_FORCE_INLINE void ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const;

    Y_FORCE_INLINE ui32 AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const;

    // Micro-optimized version of AdjustIndex.
    // No check valueIdx != valueIdxEnd for initial value of index.
    Y_FORCE_INLINE ui32 AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 timestampId) const;

protected:
    const ui64* Ptr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define SEGMENT_READERS_INL_H_
#include "segment_readers-inl.h"
#undef SEGMENT_READERS_INL_H_
