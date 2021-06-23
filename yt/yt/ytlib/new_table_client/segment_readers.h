#pragma once

#include "public.h"
#include "read_span.h"
#include "memory_helpers.h"

#include <yt/yt/core/misc/bit_packing.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/core/misc/zigzag.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NNewTableClient {

using TSegmentMetas = TRange<const NProto::TSegmentMeta*>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ConvertInt(ui64 value);

////////////////////////////////////////////////////////////////////////////////

// Describes set of column segments in block.
struct TColumnSlice
{
    TRef Block;
    TSegmentMetas SegmentsMeta;
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

class TTimestampExtractor
{
public:
    ui32 GetSegmentRowLimit() const;
    // Skip is allowed till SegmentRowLimit.
    void ReadSegment(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers);

    std::pair<ui32, ui32> GetWriteTimestampsSpan(ui32 rowIndex) const;

    std::pair<ui32, ui32> GetDeleteTimestampsSpan(ui32 rowIndex) const;

    ui32 GetWriteTimestampsCount(ui32 rowIndex) const;

    ui32 GetDeleteTimestampsCount(ui32 rowIndex) const;

    const TTimestamp* GetWriteTimestamps() const;

    const TTimestamp* GetDeleteTimestamps() const;

private:
    void DoReadSegment(const NProto::TTimestampSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers);

protected:
    // For each row index value offsets.
    ui32* WriteTimestampOffsets_ = nullptr;
    TTimestamp* WriteTimestamps_ = nullptr;
    ui32* DeleteTimestampOffsets_ = nullptr;
    TTimestamp* DeleteTimestamps_ = nullptr;

    ui32 SegmentRowOffset_ = 0;
    ui32 RowCount_ = 0;

    TMemoryHolder<char> Holder_;
};

void ReadUnversionedValueData(TUnversionedValue* value, ui64 data);

void ReadUnversionedValueData(TUnversionedValue* value, i64 data);

void ReadUnversionedValueData(TUnversionedValue* value, double data);

void ReadUnversionedValueData(TUnversionedValue* value, bool data);

void ReadUnversionedValueData(TUnversionedValue* value, TStringBuf data);

////////////////////////////////////////////////////////////////////////////////

bool GetIsDirect(int type);

bool GetIsDense(int type);

bool GetIsDense(const NProto::TSegmentMeta& meta, EValueType dataType);

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

template <class T>
class TIntegerExtractor
{
public:
    const ui64* Init(const NProto::TSegmentMeta& meta, const ui64* ptr, TTmpBuffers* tmpBuffers);

    void InitNull();

    void Extract(TUnversionedValue* value, ui32 index) const;

    TUnversionedValue operator[] (ui32 index) const;

private:
    const T* Items_ = nullptr;
    TMemoryHolder<char> Holder_;
    TBitmap IsNullBits_;

};

template <EValueType Type>
class TValueExtractor;

template <>
class TValueExtractor<EValueType::Int64>
    : public TIntegerExtractor<i64>
{ };

template <>
class TValueExtractor<EValueType::Uint64>
    : public TIntegerExtractor<ui64>
{ };

template <>
class TValueExtractor<EValueType::Double>
{
public:
    const ui64* Init(const NProto::TSegmentMeta& /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers);

    void InitNull();

    void Extract(TUnversionedValue* value, ui32 index) const;

    TUnversionedValue operator[] (ui32 index) const;

private:
    TMemoryHolder<char> Holder_;
    const double* Items_ = nullptr;
    TBitmap IsNullBits_;
};

template <>
class TValueExtractor<EValueType::Boolean>
{
public:
    const ui64* Init(const NProto::TSegmentMeta& /*meta*/, const ui64* ptr, TTmpBuffers* tmpBuffers);

    void InitNull();

    void Extract(TUnversionedValue* value, ui32 index) const;

    TUnversionedValue operator[] (ui32 index) const;

private:
    TBitmap Items_;
    TBitmap IsNullBits_;

    static ui64 NullBooleanSegmentData;
};

class TBlobExtractor
{
public:
    explicit TBlobExtractor(EValueType type)
        : Type_(type)
    { }

    void Init(const NProto::TSegmentMeta& meta, const ui64* ptr, TTmpBuffers* tmpBuffers);

    void InitNull();

    void Extract(TUnversionedValue* value, ui32 index) const;

    TUnversionedValue operator[] (ui32 index) const;

private:
    struct TItem
    {
        ui32 Begin;
        ui32 End;
    };

    TMemoryHolder<char> Holder_;
    const TItem* Items_ = nullptr;
    TBitmap IsNullBits_;
    const char* Data_ = nullptr;
    EValueType Type_;
};

template <>
class TValueExtractor<EValueType::String>
    : public TBlobExtractor
{
public:
    TValueExtractor()
        : TBlobExtractor(EValueType::String)
    { }
};

template <>
class TValueExtractor<EValueType::Composite>
    : public TBlobExtractor
{
public:
    TValueExtractor()
        : TBlobExtractor(EValueType::Composite)
    { }
};

template <>
class TValueExtractor<EValueType::Any>
    : public TBlobExtractor
{
public:
    TValueExtractor()
        : TBlobExtractor(EValueType::Any)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TRleBase
{
public:
    const ui64* Init(const NProto::TSegmentMeta& meta, const ui64* ptr, bool isDense);

    void InitNull();

    void Reset();

    ui32 GetSegmentRowLimit() const;

    // Skip is allowed till SegmentRowLimit.
    ui32 SkipTo(ui32 rowIndex, ui32 position) const;

protected:
    TMemoryHolder<ui32> RowIndex_;
    ui32 Count_ = 0;
    ui32 SegmentRowLimit_ = 0;

    ui32 LowerRowBound(ui32 position) const;

    ui32 UpperRowBound(ui32 position) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void DoReadSegment(
    TIntegerExtractor<T>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

void DoReadSegment(
    TValueExtractor<EValueType::Double>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

void DoReadSegment(
    TValueExtractor<EValueType::Boolean>* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

void DoReadSegment(
    TBlobExtractor* value,
    TRleBase* base,
    const NProto::TSegmentMeta& meta,
    const char* data,
    TTmpBuffers* tmpBuffers);

////////////////////////////////////////////////////////////////////////////////

class TVersionInfoBase
{
public:
    ui32 AdjustIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const;

    // Micro-optimized version of AdjustIndex.
    // No check valueIdx != valueIdxEnd for initial value of index.
    ui32 AdjustLowerIndex(ui32 valueIdx, ui32 valueIdxEnd, ui16 id) const;

protected:
    TMemoryHolder<ui16> WriteTimestampIds_;
};

template <bool Aggregate>
class TVersionInfo;

template <>
class TVersionInfo<true>
    : public TVersionInfoBase
{
public:
    const ui64* Init(const ui64* ptr);

    void Extract(TVersionedValue* value, const TTimestamp* timestamps, ui32 index) const;

private:
    TBitmap AggregateBits_;
};

template <>
class TVersionInfo<false>
    : public TVersionInfoBase
{
public:
    const ui64* Init(const ui64* ptr);

    void Extract(TVersionedValue* value, const TTimestamp* timestamps, ui32 index) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexItem
{
    ui32 RowIndex;
    ui32 ValueOffset;
};

class TMultiValueBase
{
public:
    const ui64* Init(
        const NProto::TSegmentMeta& meta,
        const ui64* ptr,
        bool isDense,
        TTmpBuffers* tmpBuffers);

    ui32 GetSegmentRowLimit() const;

    // TODO(lukyan): Reset position on Init and do not check here.
    ui32 SkipTo(ui32 rowIndex, ui32 position) const;

protected:
    // Keep IndexCount_ + 1 items to eliminate extra branches in read routines.
    TMemoryHolder<TIndexItem> RowToValue_;

    ui32 IndexCount_ = 0;
    // No need to keep value count. ValueCount is RowToValue[IndexCount]->ValueIndex

    ui32 SegmentRowLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Lookup readers.

template <class T>
class TLookupIntegerExtractor
{
public:
    explicit TLookupIntegerExtractor(const NProto::TSegmentMeta& meta);

    const ui64* Init(const ui64* ptr);

    void Extract(TUnversionedValue* value, ui32 index) const;

private:
    const ui64* Ptr_ = nullptr;
    const ui64 BaseValue_;
    const bool IsDirect_;
};

class TLookupDoubleExtractor
{
public:
    explicit TLookupDoubleExtractor(const NProto::TSegmentMeta& /*meta*/);

    const ui64* Init(const ui64* ptr);

    void Extract(TUnversionedValue* value, ui32 index) const;

private:
    const ui64* Ptr_ = nullptr;
};

class TLookupBooleanExtractor
{
public:
    explicit TLookupBooleanExtractor(const NProto::TSegmentMeta& /*meta*/);

    const ui64* Init(const ui64* ptr);

    void Extract(TUnversionedValue* value, ui32 index) const;

private:
    const ui64* Ptr_ = nullptr;
};

class TLookupBlobExtractor
{
public:
    TLookupBlobExtractor(const NProto::TSegmentMeta& meta, EValueType type);

    void Init(const ui64* ptr);

    void Extract(TUnversionedValue* value, ui32 index) const;

private:
    const ui64* Ptr_ = nullptr;
    ui32 ExpectedLength_;
    bool IsDirect_;
    EValueType Type_;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupIndexReader
{
public:
    TLookupIndexReader(const NProto::TSegmentMeta& meta, bool isDense);

    const ui64* Init(const ui64* ptr);

    TReadSpan GetRowIndex(ui32 position) const;

    ui32 GetCount() const;

    ui32 SkipTo(ui32 rowIndex, ui32 position) const;

private:
    const ui64* Ptr_ = nullptr;
    const ui32 RowOffset_;
    const ui32 RowLimit_;
    bool IsDense_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
class TLookupSegmentReader;

template <>
class TLookupSegmentReader<EValueType::Int64>
    : public TLookupIntegerExtractor<i64>
    , public TLookupIndexReader
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);

    TUnversionedValue GetLastValue() const;
};

template <>
class TLookupSegmentReader<EValueType::Uint64>
    : public TLookupIntegerExtractor<ui64>
    , public TLookupIndexReader
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);

    TUnversionedValue GetLastValue() const;
};

template <>
class TLookupSegmentReader<EValueType::Double>
    : public TLookupDoubleExtractor
    , public TLookupIndexReader
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);

    TUnversionedValue GetLastValue() const;
};

template <>
class TLookupSegmentReader<EValueType::Boolean>
    : public TLookupBooleanExtractor
    , public TLookupIndexReader
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);

    TUnversionedValue GetLastValue() const;
};

class TLookupBlobReaderBase
    : public TLookupIndexReader
    , public TLookupBlobExtractor
{
public:
    TLookupBlobReaderBase(const NProto::TSegmentMeta& meta, const ui64* ptr, EValueType type);

    TUnversionedValue GetLastValue() const;
};

template <>
class TLookupSegmentReader<EValueType::String>
    : public TLookupBlobReaderBase
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64*& ptr);
};

template <>
class TLookupSegmentReader<EValueType::Composite>
    : public TLookupBlobReaderBase
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);
};

template <>
class TLookupSegmentReader<EValueType::Any>
    : public TLookupBlobReaderBase
{
public:
    TLookupSegmentReader(const NProto::TSegmentMeta& meta, const ui64* ptr);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define SEGMENT_READERS_INL_H_
#include "segment_readers-inl.h"
#undef SEGMENT_READERS_INL_H_
