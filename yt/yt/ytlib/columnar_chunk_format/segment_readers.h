#pragma once

#include "public.h"
#include "read_span.h"
#include "memory_helpers.h"
#include "prepared_meta.h"

#include <yt/yt/core/misc/bit_packing.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/misc/heap.h>

// This definition is used to hide code when checking code coverage in unittests.
// Code under for this case will be removed or reworked in future.
// Now it is used for benchmark and debug purpose.
#define FULL_UNPACK

namespace NYT::NColumnarChunkFormat {

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

template <class T>
T ConvertInt(ui64 value);

template <class TDiffs>
Y_FORCE_INLINE ui32 GetOffsetNonZero(const TDiffs& diffs, ui32 expected, ui32 position);

template <class TDiffs>
Y_FORCE_INLINE ui32 GetOffset(const TDiffs& diffs, ui32 expected, ui32 position);

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
    std::vector<TReadSpan> DataSpans;
};

////////////////////////////////////////////////////////////////////////////////

template <class TExtractor, EValueType Type>
Y_FORCE_INLINE void DoInitRangesKeySegment(
    TExtractor* extractor,
    const TKeyMeta<Type>* meta,
    const ui64* ptr,
    TTmpBuffers* tmpBuffers,
    bool newMeta);

template <class TExtractor, EValueType Type>
Y_FORCE_INLINE void DoInitRangesKeySegment(
    TExtractor* extractor,
    const TKeyMeta<Type>* meta,
    const ui64* ptr,
    ui32 rowOffset,
    TMutableRange<TReadSpan> spans,
    TTmpBuffers* tmpBuffers,
    bool newMeta);

////////////////////////////////////////////////////////////////////////////////

// Scan extractors.

class TScanTimestampExtractor
{
public:
#ifdef FULL_UNPACK
    void InitSegment(const TTimestampMeta* meta, const char* data, TTmpBuffers* tmpBuffers, bool newMeta);
#endif
    void InitSegment(
        const TTimestampMeta* meta,
        const char* data,
        ui32 rowOffset,
        TMutableRange<TReadSpan> spans,
        TTmpBuffers* tmpBuffers,
        bool newMeta);

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    Y_FORCE_INLINE std::pair<ui32, ui32> GetWriteTimestampsSpan(ui32 rowIndex) const;
    Y_FORCE_INLINE std::pair<ui32, ui32> GetDeleteTimestampsSpan(ui32 rowIndex) const;

    Y_FORCE_INLINE TRange<TTimestamp> GetWriteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;
    Y_FORCE_INLINE TRange<TTimestamp> GetDeleteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;

private:
    // For each row index value offsets.
    TMemoryHolder<ui32> WriteTimestampOffsets_;
    TMemoryHolder<TTimestamp> WriteTimestamps_;
    TMemoryHolder<ui32> DeleteTimestampOffsets_;
    TMemoryHolder<TTimestamp> DeleteTimestamps_;

    // Row offset in terms of resultRowIndex.
    ui32 RowOffset_ = 0;
    // Segment row limit in terms of chunkRowIndex.
    ui32 SegmentRowLimit_ = 0;
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
    static const ui64* GetEndPtr(const TIntegerMeta* meta, const ui64* ptr, bool newMeta);

#ifdef FULL_UNPACK
    const ui64* InitData(const TIntegerMeta* meta, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta);
#endif
    const ui64* InitData(
        const TIntegerMeta* meta,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TTmpBuffers* tmpBuffers,
        ui32 segmentChunkRowCount,
        bool newMeta);
    void InitNullData();

    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    TMemoryHolder<T> Items_;
    TBitmap NullBits_;

    // SegmentChunkRowCount uniquely identifies segment in chunk and used to reuse or reset dictionary.
    ui32 SegmentChunkRowCount_ = 0;
    // Even if chunk (or segment) is read completely segment can be initialized
    // multiple times in partial unpack mode. Dict values are cached.
    // It happens because read ranges are calculated only for current block window
    // and segments can overlap block windows.
    std::vector<ui64> ValuesDict_;
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
    static const ui64* GetEndPtr(const TEmptyMeta* /*meta*/, const ui64* ptr, bool /*newMeta*/);

#ifdef FULL_UNPACK
    const ui64* InitData(const TEmptyMeta* /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers, bool /*newMeta*/);
#endif
    const ui64* InitData(
        const TEmptyMeta* /*meta*/,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TTmpBuffers* tmpBuffers,
        ui32 /*segmentChunkRowCount*/,
        bool /*newMeta*/);
    void InitNullData();

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
    static const ui64* GetEndPtr(const TEmptyMeta* /*meta*/, const ui64* ptr, bool /*newMeta*/);

#ifdef FULL_UNPACK
    const ui64* InitData(const TEmptyMeta* /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta);
#endif
    const ui64* InitData(
        const TEmptyMeta* /*meta*/,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TTmpBuffers* tmpBuffers,
        ui32 /*segmentChunkRowCount*/,
        bool newMeta);
    void InitNullData();

    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    TMemoryHolder<char> Holder_;
    TBitmap Items_;
    TBitmap NullBits_;

    static ui64 NullBooleanSegmentData;
};

class TScanBlobExtractor
{
public:
    Y_FORCE_INLINE explicit TScanBlobExtractor(EValueType type);

#ifdef FULL_UNPACK
    void InitData(const TBlobMeta* meta, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta);
#endif
    void InitData(
        const TBlobMeta* meta,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TTmpBuffers* tmpBuffers,
        ui32 segmentChunkRowCount,
        bool newMeta);
    void InitNullData();

    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

private:
    struct TItem
    {
        ui32 Begin;
        ui32 End;
    };

    TMemoryHolder<TItem> Items_;
    TBitmap NullBits_;
    const char* Data_ = nullptr;
    EValueType Type_;

    // SegmentChunkRowCount uniquely identifies segment in chunk and used to reuse or reset dictionary.
    ui32 SegmentChunkRowCount_ = 0;
    // Even if chunk (or segment) is read completely segment can be initialized
    // multiple times in partial unpack mode. Dict values are cached.
    // It happens because read ranges are calculated only for current block window
    // and segments can overlap block windows.
    std::vector<ui32> OffsetsDict_;
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
#ifdef FULL_UNPACK
    const ui64* InitIndex(const TKeyIndexMeta* meta, const ui64* ptr, bool newMeta);
#endif
    const ui64* InitIndex(
        const TKeyIndexMeta* meta,
        const ui64* ptr,
        ui32 rowOffset,
        TMutableRange<TReadSpan> spans,
        TTmpBuffers* tmpBuffers,
        bool newMeta);

    void InitNullIndex();
    Y_FORCE_INLINE void Reset();

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    Y_FORCE_INLINE ui32 GetCount() const;

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 LowerRowBound(ui32 position) const;
    Y_FORCE_INLINE ui32 UpperRowBound(ui32 position) const;

protected:
    TMemoryHolder<ui32> RowIndexes_;
    ui32 Count_ = 0;
    ui32 SegmentRowLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Aggregate>
class TAggregateBitsBase;

template <>
class TAggregateBitsBase<false>
{ };

template <>
class TAggregateBitsBase<true>
{
protected:
    TBitmap AggregateBits_;
};

////////////////////////////////////////////////////////////////////////////////

template <bool Aggregate>
class TScanVersionExtractor
    : public TAggregateBitsBase<Aggregate>
{
public:
#ifdef FULL_UNPACK
    const ui64* InitVersion(const TMultiValueIndexMeta* meta, const ui64* ptr, bool newMeta);
#endif
    const ui64* InitVersion(
        const TMultiValueIndexMeta* meta,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        bool newMeta);

    Y_FORCE_INLINE void ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const;

    Y_FORCE_INLINE ui32 AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 id) const;

    // Micro-optimized version of AdjustIndex.
    // No check valueIdx != valueIdxEnd for initial value of index.
    Y_FORCE_INLINE ui32 AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 id) const;

private:
    TMemoryHolder<ui32> WriteTimestampIds_;
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
#ifdef FULL_UNPACK
    const ui64* InitIndex(
        const TMultiValueIndexMeta* meta,
        const ui64* ptr,
        TTmpBuffers* tmpBuffers, bool newMeta);
#endif

    const ui64* InitIndex(
        const TMultiValueIndexMeta* meta,
        const ui64* ptr,
        ui32 rowOffset,
        TMutableRange<TReadSpan> spans,
        TTmpBuffers* tmpBuffers,
        bool newMeta);

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 GetValueCount() const;

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
    template <bool NewMeta>
    Y_FORCE_INLINE void InitSegment(const TTimestampMeta* meta, const char* data);

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    Y_FORCE_INLINE std::pair<ui32, ui32> GetWriteTimestampsSpan(ui32 rowIndex) const;
    Y_FORCE_INLINE std::pair<ui32, ui32> GetDeleteTimestampsSpan(ui32 rowIndex) const;

    Y_FORCE_INLINE TRange<TTimestamp> GetWriteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;
    Y_FORCE_INLINE TRange<TTimestamp> GetDeleteTimestamps(ui32 rowIndex, TChunkedMemoryPool* memoryPool) const;

protected:
    ui32 RowOffset_ = 0;
    ui32 SegmentRowLimit_ = 0;

    TTimestamp BaseTimestamp_;
    ui32 ExpectedDeletesPerRow_;
    ui32 ExpectedWritesPerRow_;
    // For each row index value offsets.

    TCompressedVectorView TimestampsDict_;
    TCompressedVectorView32 WriteTimestampIds_;
    TCompressedVectorView32 DeleteTimestampIds_;
    TCompressedVectorView32 WriteOffsetDiffs_;
    TCompressedVectorView32 DeleteOffsetDiffs_;
};

template <EValueType Type>
class TLookupDataExtractor;

template <class T>
class TLookupIntegerExtractor
{
public:
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitData(const TIntegerMeta* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNullData();

    Y_FORCE_INLINE void ExtractDict(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void ExtractDirect(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

    Y_FORCE_INLINE void Prefetch(ui32 position) const;

private:
    TCompressedVectorView Values_;

    union TUnion {
        TUnion()
            : Ids_()
        { }
        TCompressedVectorView Ids_;
        TBitmap NullBits_;
    } U_;

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
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitData(const TEmptyMeta* /*meta*/, const ui64* ptr);
    Y_FORCE_INLINE void InitNullData();

    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

    Y_FORCE_INLINE void Prefetch(ui32 /*position*/) const;

private:
    const ui64* Ptr_ = nullptr;
};

template <>
class TLookupDataExtractor<EValueType::Boolean>
{
public:
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitData(const TEmptyMeta* /*meta*/, const ui64* ptr);
    Y_FORCE_INLINE void InitNullData();

    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

    Y_FORCE_INLINE void Prefetch(ui32 /*position*/) const;

private:
    const ui64* Ptr_ = nullptr;
};

class TLookupBlobExtractor
{
public:
    Y_FORCE_INLINE explicit TLookupBlobExtractor(EValueType type);

    template <bool NewMeta>
    Y_FORCE_INLINE void InitData(const TBlobMeta* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNullData();

    Y_FORCE_INLINE void ExtractDict(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void ExtractDirect(TUnversionedValue* value, ui32 position) const;
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const;

    Y_FORCE_INLINE void Prefetch(ui32 position) const;

private:
    TCompressedVectorView32 Offsets_;

    union TUnion {
        TUnion()
            : Ids_()
        { }
        TCompressedVectorView32 Ids_;
        TBitmap NullBits_;
    } U_;

    const char* Data_= nullptr;

    ui32 ExpectedLength_ = 0;
    bool Direct_ = true;
    EValueType Type_;

    Y_FORCE_INLINE TStringBuf GetBlob(TCompressedVectorView32 offsets, const char* data, ui32 position) const;
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
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitIndex(const TKeyIndexMeta* meta, const ui64* ptr);
    Y_FORCE_INLINE void InitNullIndex();
    Y_FORCE_INLINE void Reset();

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;
    Y_FORCE_INLINE ui32 GetCount() const;

    // Skip is allowed till SegmentRowLimit.
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 LowerRowBound(ui32 position) const;
    Y_FORCE_INLINE ui32 UpperRowBound(ui32 position) const;

private:
    TCompressedVectorView RowIndexes_;
    ui32 RowOffset_ = 0;
    ui32 RowLimit_ = 0;

    bool Dense_ = true;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupMultiValueIndexExtractor
{
public:
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitIndex(
        const TMultiValueIndexMeta* meta,
        const ui64* ptr);

    Y_FORCE_INLINE ui32 GetSegmentRowLimit() const;

    // Returns value offset.
    // For sparse index and value offset are equal.
    // For dense initial offset hint is not used.

    Y_FORCE_INLINE ui32 SkipToDense(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 SkipToSparse(ui32 rowIndex, ui32 position) const;
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const;

    Y_FORCE_INLINE void Prefetch(ui32 rowIndex) const;

protected:
    TCompressedVectorView32 RowIndexesOrPerRowDiffs_;

    ui32 RowOffset_ = 0;
    ui32 RowLimit_ = 0;
    ui32 ExpectedPerRow_ = 0;
    bool Dense_ = false;
};

template <bool Aggregate>
class TLookupVersionExtractor
    : public TAggregateBitsBase<Aggregate>
{
public:
    template <bool NewMeta>
    Y_FORCE_INLINE const ui64* InitVersion(const TMultiValueIndexMeta* meta, const ui64* ptr);

    Y_FORCE_INLINE void ExtractVersion(TVersionedValue* value, const TTimestamp* timestamps, ui32 position) const;

    Y_FORCE_INLINE ui32 AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const;

    // Micro-optimized version of AdjustIndex.
    // No check valueIdx != valueIdxEnd for initial value of index.
    Y_FORCE_INLINE ui32 AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui32 timestampId) const;

    Y_FORCE_INLINE void Prefetch(ui32 valueIdx) const;

protected:
    TCompressedVectorView32 WriteTimestampIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat

#define SEGMENT_READERS_INL_H_
#include "segment_readers-inl.h"
#undef SEGMENT_READERS_INL_H_
