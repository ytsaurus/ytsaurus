#pragma once

#include "nested_row_merger.h"

#include <yt/yt/client/table_client/timestamped_schema_helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowMerger
{
public:
    using TResultingRow = NTableClient::TUnversionedRow;

    TSchemafulRowMerger(
        NTableClient::TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        const NTableClient::TColumnFilter& columnFilter,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        NTableClient::TTimestamp retentionTimestamp = NTableClient::NullTimestamp,
        const NTableClient::TTimestampColumnMapping& timestampColumnMapping = {},
        TNestedColumnsSchema nestedColumnsSchema = {});

    void AddPartialRow(NTableClient::TVersionedRow row);
    void AddPartialRow(NTableClient::TVersionedRow row, NTableClient::TTimestamp upperTimestampLimit);
    NTableClient::TMutableUnversionedRow BuildMergedRow();
    void Reset();

private:
    const NTableClient::TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;
    const NTableClient::TTimestamp RetentionTimestamp_;
    const std::vector<int> ColumnIds_;
    const TNestedColumnsSchema NestedColumnsSchema_;

    NTableClient::TMutableUnversionedRow MergedRow_;
    std::vector<NTableClient::TTimestamp> MergedTimestamps_;

    std::vector<int> ColumnIdToIndex_;
    std::vector<int> ColumnIdToTimestampColumnId_;

    TCompactVector<bool, NTableClient::TypicalColumnCount> IsTimestampColumn_;

    std::vector<int> AggregateColumnIds_;
    std::vector<std::vector<NTableClient::TVersionedValue>> AggregateValues_;

    TNestedTableMerger NestedMerger_{/*orderNestedRows*/ false};
    std::vector<TRange<NTableClient::TVersionedValue>> NestedKeyColumns_;
    std::vector<TMutableRange<NTableClient::TVersionedValue>> NestedValueColumns_;

    NTableClient::TTimestamp LatestWrite_;
    NTableClient::TTimestamp LatestDelete_;

    bool Started_ = false;

    void Cleanup();
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMerger
{
public:
    using TResultingRow = NTableClient::TUnversionedRow;

    TUnversionedRowMerger(
        NTableClient::TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        TNestedColumnsSchema nestedColumnsSchema = {});

    void AddPartialRow(NTableClient::TUnversionedRow row);
    void DeletePartialRow(NTableClient::TUnversionedRow row);
    void InitPartialRow(NTableClient::TUnversionedRow row);
    NTableClient::TMutableUnversionedRow BuildDeleteRow();
    NTableClient::TMutableUnversionedRow BuildMergedRow();

private:
    const NTableClient::TRowBufferPtr RowBuffer_;
    const int ColumnCount_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;
    const TNestedColumnsSchema NestedColumnsSchema_;

    NTableClient::TMutableUnversionedRow MergedRow_;
    TCompactVector<bool, NTableClient::TypicalColumnCount> ValidValues_;

    TNestedTableMerger NestedMerger_{/*orderNestedRows*/ true};
    std::vector<std::vector<NTableClient::TVersionedValue>> NestedKeyColumns_;
    std::vector<std::vector<NTableClient::TVersionedValue>> NestedValueColumns_;
    int PartialRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSamplingRowMerger
{
public:
    TSamplingRowMerger(
        NTableClient::TRowBufferPtr rowBuffer,
        NTableClient::TTableSchemaPtr schema);

    NTableClient::TMutableUnversionedRow MergeRow(NTableClient::TVersionedRow row);
    void Reset();

private:
    const NTableClient::TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;

    int SampledColumnCount_ = 0;

    TCompactVector<NTableClient::TTimestamp, NTableClient::TypicalColumnCount> LatestTimestamps_;
    TCompactVector<int, NTableClient::TypicalColumnCount> IdMapping_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
