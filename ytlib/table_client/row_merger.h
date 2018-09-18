#pragma once

#include "public.h"

#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/small_vector.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/client/api/public.h>

namespace NYT {
namespace NTableClient {

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
        NQueryClient::TColumnEvaluatorPtr columnEvaluator);

    void AddPartialRow(TVersionedRow row);
    TUnversionedRow BuildMergedRow();
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int ColumnCount_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    TMutableUnversionedRow MergedRow_;
    SmallVector<TTimestamp, TypicalColumnCount> MergedTimestamps_;

    SmallVector<int, TypicalColumnCount> ColumnIds_;
    SmallVector<int, TypicalColumnCount> ColumnIdToIndex_;

    SmallVector<TVersionedValue, TypicalColumnCount> AggregateValues_;

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
    TUnversionedRow BuildMergedRow();
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int ColumnCount_;
    const int KeyColumnCount_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    bool Started_ = false;
    bool Deleted_ = false;

    TMutableUnversionedRow MergedRow_;
    SmallVector<bool, TypicalColumnCount> ValidValues_;

    void InitPartialRow(TUnversionedRow row);
    void Cleanup();
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
{
public:
    using TResultingRow = TVersionedRow;

    TVersionedRowMerger(
        TRowBufferPtr rowBuffer,
        int columnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter,
        TRetentionConfigPtr config,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp,
        NQueryClient::TColumnEvaluatorPtr columnEvaluator,
        bool lookup,
        bool mergeRowsOnFlush);

    void AddPartialRow(TVersionedRow row);
    TVersionedRow BuildMergedRow();
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;
    const TRetentionConfigPtr Config_;
    const bool IgnoreMajorTimestamp_;
    const TTimestamp CurrentTimestamp_;
    const TTimestamp MajorTimestamp_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;
    const bool Lookup_ = true;
    const bool MergeRowsOnFlush_;

    bool Started_ = false;

    SmallVector<int, TypicalColumnCount> ColumnIds_;
    SmallVector<int, TypicalColumnCount> ColumnIdToIndex_;
    SmallVector<TUnversionedValue, TypicalColumnCount> Keys_;

    std::vector<TVersionedValue> PartialValues_;
    std::vector<TVersionedValue> ColumnValues_;
    std::vector<TVersionedValue> MergedValues_;

    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

    void Cleanup();
};

////////////////////////////////////////////////////////////////////////////////

class TSamplingRowMerger
{
public:
    TSamplingRowMerger(
        TRowBufferPtr rowBuffer,
        const TTableSchema& schema);

    TUnversionedRow MergeRow(TVersionedRow row);
    void Reset();

private:
    const TRowBufferPtr RowBuffer_;
    const int KeyColumnCount_;

    int SampledColumnCount_ = 0;

    SmallVector<TTimestamp, TypicalColumnCount> LatestTimestamps_;
    SmallVector<int, TypicalColumnCount> IdMapping_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
