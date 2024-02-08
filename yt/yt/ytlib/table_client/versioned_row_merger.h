#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedRowMerger
    : public TNonCopyable
{
    using TResultingRow = TVersionedRow;

    virtual ~IVersionedRowMerger() = default;

    virtual void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit = MaxTimestamp) = 0;

    virtual TMutableVersionedRow BuildMergedRow() = 0;

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
    bool lookup,
    bool mergeRowsOnFlush,
    bool mergeDeletionsOnFlush = false);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedRowMerger> CreateVersionedRowMerger(
    NTabletClient::ERowMergerType rowMergerType,
    TRowBufferPtr rowBuffer,
    int columnCount,
    int keyColumnCount,
    const TColumnFilter& columnFilter,
    TRetentionConfigPtr config,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    NQueryClient::TColumnEvaluatorPtr columnEvaluator,
    bool lookup,
    bool mergeRowsOnFlush,
    bool mergeDeletionsOnFlush = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
