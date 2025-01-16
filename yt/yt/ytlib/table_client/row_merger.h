#pragma once

#include "nested_row_merger.h"

#include <yt/yt/client/table_client/timestamped_schema_helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

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
        TTimestamp retentionTimestamp = NullTimestamp,
        const TTimestampColumnMapping& timestampColumnMapping = {},
        TNestedColumnsSchema nestedColumnsSchema = {});

    void AddPartialRow(TVersionedRow row);
    void AddPartialRow(TVersionedRow row, TTimestamp upperTimestampLimit);
    TMutableUnversionedRow BuildMergedRow();
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;
    const TTimestamp RetentionTimestamp_;
    const TNestedColumnsSchema NestedColumnsSchema_;

    TMutableUnversionedRow MergedRow_;
    TCompactVector<TTimestamp, TypicalColumnCount> MergedTimestamps_;

    TCompactVector<int, TypicalColumnCount> ColumnIds_;
    TCompactVector<int, TypicalColumnCount> ColumnIdToIndex_;
    TCompactVector<int, TypicalColumnCount> ColumnIdToTimestampColumnId_;

    TCompactVector<bool, TypicalColumnCount> IsTimestampColumn_;

    std::vector<int> AggregateColumnIds_;
    std::vector<std::vector<TVersionedValue>> AggregateValues_;

    TNestedTableMerger NestedMerger_;
    std::vector<TMutableRange<TVersionedValue>> NestedKeyColumns_;
    std::vector<TMutableRange<TVersionedValue>> NestedValueColumns_;

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
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        TNestedColumnsSchema nestedColumnsSchema = {});

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
    const TNestedColumnsSchema NestedColumnsSchema_;

    TMutableUnversionedRow MergedRow_;
    TCompactVector<bool, TypicalColumnCount> ValidValues_;

    TNestedTableMerger NestedMerger_;
    std::vector<std::vector<TVersionedValue>> NestedKeyColumns_;
    std::vector<std::vector<TVersionedValue>> NestedValueColumns_;
    int PartialRowCount_ = 0;
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
