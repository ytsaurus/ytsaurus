#pragma once

#include "public.h"
#include "read_span.h"
#include "block_ref.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NNewTableClient {

using TIdRange = std::pair<ui32, ui32>;

struct TDataBufferTag { };

constexpr ui32 SentinelRowIndex = -1;

////////////////////////////////////////////////////////////////////////////////

struct TValueSchema
{
    EValueType Type;
    ui16 Id;
    bool Aggregate = false;
};

struct IRowsetBuilder
{
    virtual ~IRowsetBuilder() = default;

    virtual bool IsReadListEmpty() const = 0;

    virtual void BuildReadListForWindow(TSpanMatching initialWindow) = 0;

    virtual ui32 ReadRowsByList(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics) = 0;

    virtual TChunkedMemoryPool* GetPool() const = 0;

    virtual void ClearBuffer() = 0;
};

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TLegacyKey> keys,
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TRange<TColumnBase> columnInfos,
    TTimestamp timestamp,
    bool produceAll,
    bool newMeta);

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TRowRange> keyRanges,
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TRange<TColumnBase> columnInfos,
    TTimestamp timestamp,
    bool produceAll,
    bool newMeta);

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    std::vector<ui32> chunkRowIndexes,
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TRange<TColumnBase> columnInfos,
    TTimestamp timestamp,
    bool produceAll,
    bool newMeta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
