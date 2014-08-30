#pragma once

#include "public.h"

#include <core/misc/public.h>
#include <core/misc/small_vector.h>

#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMerger
{
public:
    TUnversionedRowMerger(
        TChunkedMemoryPool* pool,
        int schemaColumnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter);

    void AddPartialRow(TVersionedRow row);
    TUnversionedRow BuildMergedRowAndReset();
    void Reset();

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    NVersionedTableClient::TKeyComparer KeyComparer_;

    NVersionedTableClient::TUnversionedRow MergedRow_;
    SmallVector<NVersionedTableClient::TTimestamp, NVersionedTableClient::TypicalColumnCount> MergedTimestamps_;

    SmallVector<int, NVersionedTableClient::TypicalColumnCount> ColumnIds_;
    SmallVector<int, NVersionedTableClient::TypicalColumnCount> ColumnIdToIndex_;
    
    TTimestamp LatestWrite_;
    TTimestamp LatestDelete_;
    bool Started_ = false;


    void Cleanup();

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
{
public:
    TVersionedRowMerger(
        TChunkedMemoryPool* pool,
        int keyColumnCount,
        TRetentionConfigPtr config,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp);

    void AddPartialRow(TVersionedRow row);
    TVersionedRow BuildMergedRowAndReset();
    void Reset();

private:
    TChunkedMemoryPool* Pool_;
    int KeyColumnCount_;
    TRetentionConfigPtr Config_;
    TTimestamp CurrentTimestamp_;
    TTimestamp MajorTimestamp_;

    NVersionedTableClient::TKeyComparer KeyComparer_;

    bool Started_;
    SmallVector<TUnversionedValue, NVersionedTableClient::TypicalColumnCount> Keys_;

    std::vector<TVersionedValue> PartialValues_;
    std::vector<TVersionedValue> ColumnValues_;
    std::vector<TVersionedValue> MergedValues_;

    std::vector<TTimestamp> PartialDeleteTimestamps_;
    std::vector<TTimestamp> WriteTimestamps_;
    std::vector<TTimestamp> DeleteTimestamps_;

    void Cleanup();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
