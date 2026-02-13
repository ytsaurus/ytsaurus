#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/query/base/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedRowMerger
    : public TNonCopyable
{
    using TResultingRow = NTableClient::TVersionedRow;

    virtual ~IVersionedRowMerger() = default;

    virtual void AddPartialRow(
        NTableClient::TVersionedRow row,
        NTableClient::TTimestamp upperTimestampLimit = NTableClient::MaxTimestamp) = 0;

    virtual NTableClient::TMutableVersionedRow BuildMergedRow(bool produceEmptyRow = false) = 0;

    virtual void Reset() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateLegacyVersionedRowMerger(
    NTableClient::TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const NTableClient::TColumnFilter& columnFilter,
    NTableClient::TRetentionConfigPtr config,
    NTableClient::TTimestamp currentTimestamp,
    NTableClient::TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    bool mergeRowsOnFlush,
    std::optional<int> ttlColumnIndex = std::nullopt,
    bool mergeDeletionsOnFlush = false,
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateVersionedRowMerger(
    NTabletClient::ERowMergerType rowMergerType,
    NTableClient::TRowBufferPtr rowBuffer,
    NTableClient::TTableSchemaPtr tableSchema,
    const NTableClient::TColumnFilter& columnFilter,
    NTableClient::TRetentionConfigPtr config,
    NTableClient::TTimestamp currentTimestamp,
    NTableClient::TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    const NYT::NYson::TYsonString& customRuntimeData,
    bool mergeRowsOnFlush,
    bool useTtlColumn = false,
    bool mergeDeletionsOnFlush = false,
    TNestedRowDiscardPolicyPtr nestedRowDiscardPolicy = nullptr,
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
