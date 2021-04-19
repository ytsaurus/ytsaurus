#pragma once

#include "public.h"
#include "segment_readers.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NNewTableClient {

using TIdRange = std::pair<ui16, ui16>;

struct TDataBufferTag { };

////////////////////////////////////////////////////////////////////////////////

struct TValueProducerInfo
{
    TVersionedValue* Ptr;
    const TTimestamp* Timestamps;
    TIdRange IdRange;
};

////////////////////////////////////////////////////////////////////////////////

template <template <EValueType Type> class TFunction, class... TArgs>
auto DispatchByDataType(EValueType type, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Replace data weight with row buffer allocated memory count.
// Data weight does not match actual memory read/write footprint.
// Types of memory are not counted: value types, ids, pointer to string data, length.
struct TDataWeightStatistics
{
    template <EValueType Type>
    void AddFixedPart(ui64 count)
    {
        if (!IsStringLikeType(Type)) {
            Count += count * GetDataWeight(Type);
        }
    }

    template <EValueType Type>
    void AddVariablePart(TUnversionedValue value, ui64 count = 1)
    {
        if (IsStringLikeType(Type)) {
            Count += value.Length * count;
        }
    }

    ui64 Count = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IColumnBase
{
    virtual ~IColumnBase() = default;

    // Skip is allowed till SegmentRowLimit.
    virtual void SetSegmentData(
        const NProto::TSegmentMeta& meta,
        /*TRef*/ const char* data,
        TTmpBuffers* tmpBuffers) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TKeyColumnBase
    : public IColumnBase
    , public TRleBase
{
public:
    virtual ui32 ReadRows(
        TUnversionedValue** keys,
        TRange<TReadSpan> spans,
        ui32 position,
        ui16 id,
        TDataWeightStatistics* statistics) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedValueColumnBase
    : public IColumnBase
    , public TMultiValueBase
{
public:
    ui32 CollectCounts(
        ui32* counts,
        TRange<TReadSpan> spans,
        ui32 position) const;

    // Compaction read.
    virtual ui32 ReadAllValues(
        TVersionedValue** values,
        const TTimestamp** timestamps,
        TRange<TReadSpan> spans,
        ui32 position,
        TDataWeightStatistics* statistics) const = 0;

    // Transactional read.
    virtual ui32 ReadValues(
        TValueProducerInfo* values,
        TRange<TReadSpan> spans,
        ui32 position,
        bool produceAll,
        TDataWeightStatistics* statistics) const = 0;

private:
    ui32 DoCollectCounts(
        ui32* counts,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TValueSchema
{
    EValueType Type;
    ui16 Id;
    bool Aggregate = false;
};

// Reads rows within segments window.
class TReaderBase
{
public:
    TReaderBase(TRange<EValueType> keyTypes, TRange<TValueSchema> valueSchema);

    ui32 GetKeySegmentsRowLimit(ui32 limit = std::numeric_limits<ui32>::max());
    ui32 GetValueSegmentsRowLimit(ui32 limit = std::numeric_limits<ui32>::max());

    void CollectCounts(ui32* valueCounts, TRange<TReadSpan> spans);

    void ReadKeys(
        TMutableVersionedRow* rows,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TDataWeightStatistics* statistics);

    ui32 GetKeyColumnCount() const;

    template <class T>
    T* Allocate(size_t size);

    TChunkedMemoryPool* GetPool() const;
    void ClearBuffer();

    TRange<std::unique_ptr<TKeyColumnBase>> GetKeyColumns() const;
    TRange<std::unique_ptr<TVersionedValueColumnBase>> GetValueColumns() const;

protected:
    // Positions in segments are kept separately to minimize write memory footprint.
    // Column readers are immutable during read.
    TMemoryHolder<ui32> Positions_;

    const NTableClient::TRowBufferPtr Buffer_ = New<NTableClient::TRowBuffer>(TDataBufferTag());

private:
    std::vector<std::unique_ptr<TKeyColumnBase>> KeyColumns_;
    std::vector<std::unique_ptr<TVersionedValueColumnBase>> ValueColumns_;
};

class TVersionedRowsetBuilder
    : public IColumnBase // Timestamp column.
    , public TTimestampExtractor
    , public TReaderBase
{
public:
    using TReaderBase::TReaderBase;

    virtual void SetSegmentData(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers) override
    {
        ReadSegment(meta, data, tmpBuffers);
    }

    virtual void ReadRows(
        TMutableVersionedRow* rows,
        TRange<TReadSpan> spans,
        TDataWeightStatistics* statistics) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TVersionedRowsetBuilder> CreateVersionedRowsetBuilder(
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueTypes,
    TTimestamp timestamp,
    bool produceAll);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define ROWSET_BUILDER_INL_H_
#include "rowset_builder-inl.h"
#undef ROWSET_BUILDER_INL_H_
