#pragma once

#include "public.h"
#include "read_span.h"
#include "block_ref.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NColumnarChunkFormat {

using TIdRange = std::pair<ui32, ui32>;

struct TDataBufferTag { };

constexpr ui32 SentinelRowIndex = -1;

////////////////////////////////////////////////////////////////////////////////

struct TValueSchema
{
    ui16 Id;
    EValueType Type;
    bool Aggregate = false;

    // It is allowed to alter value columns with primitive types to any.
    // In this case, the expected read schema would be EValueType::Any,
    // but the chunk schema can still hold primitive types.
    bool ConvertToAny;
};

struct IRowsetBuilder
{
    virtual ~IRowsetBuilder() = default;

    virtual bool IsReadListEmpty() const = 0;

    virtual void BuildReadListForWindow(
        TSpanMatching initialWindow,
        const NTableClient::TKeyFilterStatisticsPtr& keyFilterStatistics) = 0;

    virtual ui32 ReadRowsByList(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics) = 0;

    virtual TChunkedMemoryPool* GetPool() = 0;

    virtual void ClearBuffer() = 0;
};

struct TRowsetBuilderParams
{
    // Const qualifier is used to force all fields initialization.
    const TRange<EValueType> KeyTypes;
    const ui16 ReadItemWidth;
    const bool ProduceAll;
    const bool NewMeta;
    const TCompactVector<ui16, 8> KeyColumnIndexes;
    const TRange<TValueSchema> ValueSchema;
    const TRange<TColumnBase> ColumnInfos;
    const TTimestamp Timestamp;
    const IMemoryUsageTrackerPtr MemoryUsageTracker;
};

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TLegacyKey> keys,
    const TRowsetBuilderParams& params);

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TRowRange> keyRanges,
    const TRowsetBuilderParams& params);

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TKeysWithHints keysWithHints,
    const TRowsetBuilderParams& params);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
