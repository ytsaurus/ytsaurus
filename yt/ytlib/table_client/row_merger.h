#pragma once

#include "public.h"
#include "versioned_row.h"
#include "unversioned_row.h"

#include <core/misc/small_vector.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowMerger
    : public TRefCounted
{
public:
    using TResultingRow = TUnversionedRow;

    TSchemafulRowMerger(
        TRowBufferPtr rowBuffer,
        int schemaColumnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter);

    void AddPartialRow(TVersionedRow row);
    TUnversionedRow BuildMergedRow();
    void Reset();

private:
    TRowBufferPtr RowBuffer_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    TUnversionedRow MergedRow_;
    SmallVector<TTimestamp, TypicalColumnCount> MergedTimestamps_;

    SmallVector<int, TypicalColumnCount> ColumnIds_;
    SmallVector<int, TypicalColumnCount> ColumnIdToIndex_;

    TTimestamp LatestWrite_;
    TTimestamp LatestDelete_;
    bool Started_ = false;

    void Cleanup();

};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMerger
    : public TRefCounted
{
public:
    using TResultingRow = TUnversionedRow;

    TUnversionedRowMerger(
        TRowBufferPtr rowBuffer,
        int schemaColumnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter);

    void AddPartialRow(TUnversionedRow row);
    void DeletePartialRow(TUnversionedRow row);
    TUnversionedRow BuildMergedRow();
    void Reset();

private:
    TRowBufferPtr RowBuffer_;
    int SchemaColumnCount_;
    int KeyColumnCount_;
    bool Started_;
    bool Deleted_;

    TUnversionedRow MergedRow_;
    SmallVector<bool, TypicalColumnCount> ValidValues_;

    SmallVector<int, TypicalColumnCount> ColumnIds_;
    SmallVector<int, TypicalColumnCount> ColumnIdToIndex_;

    void InitPartialRow(TUnversionedRow row);
    void Cleanup();
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
    : public TRefCounted
{
public:
    using TResultingRow = TVersionedRow;

    TVersionedRowMerger(
        TRowBufferPtr rowBuffer,
        int keyColumnCount,
        TRetentionConfigPtr config,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp);

    void AddPartialRow(TVersionedRow row);
    TVersionedRow BuildMergedRow();
    void Reset();

    TTimestamp GetCurrentTimestamp() const;
    TTimestamp GetMajorTimestamp() const;

private:
    TRowBufferPtr RowBuffer_;
    int KeyColumnCount_;
    TRetentionConfigPtr Config_;
    TTimestamp CurrentTimestamp_;
    TTimestamp MajorTimestamp_;

    bool Started_;
    SmallVector<TUnversionedValue, TypicalColumnCount> Keys_;

    std::vector<TVersionedValue> PartialValues_;
    std::vector<TVersionedValue> ColumnValues_;
    std::vector<TVersionedValue> MergedValues_;

    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

    void Cleanup();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
