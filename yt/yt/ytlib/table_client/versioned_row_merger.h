#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/query/base/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedRowMerger
    : public TNonCopyable
{
    using TResultingRow = TVersionedRow;

    virtual ~IVersionedRowMerger() = default;

    virtual void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit = MaxTimestamp) = 0;

    virtual TMutableVersionedRow BuildMergedRow(bool produceEmptyRow = false) = 0;

    virtual void Reset() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateLegacyVersionedRowMerger(
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    bool mergeRowsOnFlush,
    std::optional<int> ttlColumnIndex = std::nullopt,
    bool mergeDeletionsOnFlush = false);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateVersionedRowMerger(
    NTabletClient::ERowMergerType rowMergerType,
    TRowBufferPtr rowBuffer,
    TTableSchemaPtr tableSchema,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    NYT::NYson::TYsonString customRuntimeData,
    bool mergeRowsOnFlush,
    bool useTtlColumn = false,
    bool mergeDeletionsOnFlush = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
