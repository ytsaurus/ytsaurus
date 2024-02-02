#pragma once

#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowMerger
{
public:
    using TResultingRow = TUnversionedRow;

    TSchemafulRowMerger(
        TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        TTimestamp retentionTimestamp = NullTimestamp);

    void AddPartialRow(TVersionedRow row);
    void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit);
    TMutableUnversionedRow BuildMergedRow();
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int ColumnCount_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;
    const TTimestamp RetentionTimestamp_;

    TMutableUnversionedRow MergedRow_;
    TCompactVector<TTimestamp, TypicalColumnCount> MergedTimestamps_;

    TCompactVector<int, TypicalColumnCount> ColumnIds_;
    TCompactVector<int, TypicalColumnCount> ColumnIdToIndex_;

    TCompactVector<TVersionedValue, TypicalColumnCount> AggregateValues_;

    TTimestamp LatestWrite_;
    TTimestamp LatestDelete_;
    bool Started_ = false;

    void Cleanup();
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMerger
{
public:
    using TResultingRow = TUnversionedRow;

    TUnversionedRowMerger(
        TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator);

    void AddPartialRow(TUnversionedRow row);
    void DeletePartialRow(TUnversionedRow row);
    void InitPartialRow(TUnversionedRow row);
    TMutableUnversionedRow BuildDeleteRow();
    TMutableUnversionedRow BuildMergedRow();

private:
    const TRowBufferPtr RowBuffer_;
    const int ColumnCount_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    TMutableUnversionedRow MergedRow_;
    TCompactVector<bool, TypicalColumnCount> ValidValues_;
};

////////////////////////////////////////////////////////////////////////////////

class TSamplingRowMerger
{
public:
    TSamplingRowMerger(
        TRowBufferPtr rowBuffer,
        TTableSchemaPtr schema);

    TMutableUnversionedRow MergeRow(TVersionedRow row);
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;

    int SampledColumnCount_ = 0;

    TCompactVector<TTimestamp, TypicalColumnCount> LatestTimestamps_;
    TCompactVector<int, TypicalColumnCount> IdMapping_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
