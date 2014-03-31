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

    void Start(const TUnversionedValue* keyBegin);

    void AddPartialRow(TVersionedRow row);

    TUnversionedRow BuildMergedRow();

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    NVersionedTableClient::TKeyComparer KeyComparer_;
    SmallVector<TVersionedValue, NVersionedTableClient::TypicalColumnCount> MergedValues_;
    SmallVector<bool, NVersionedTableClient::TypicalColumnCount> ColumnFlags_;
    SmallVector<int, NVersionedTableClient::TypicalColumnCount> ColumnIds_;
    
    TTimestamp LatestWrite_;
    TTimestamp LatestDelete_;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
{
public:
    TVersionedRowMerger(
        TChunkedMemoryPool* pool,
        int schemaColumnCount,
        int keyColumnCount);

    void Start(const TUnversionedValue* keyBegin);

    void AddPartialRow(TVersionedRow row);

    TVersionedRow BuildMergedRow();

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    NVersionedTableClient::TKeyComparer KeyComparer_;

    SmallVector<TUnversionedValue, NVersionedTableClient::TypicalColumnCount> Keys_;
    
    std::vector<TVersionedValue> Values_;
    std::vector<TVersionedValue> MergedValues_;

    std::vector<TTimestamp> Timestamps_;
    std::vector<TTimestamp> MergedTimestamps_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
