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
        const NVersionedTableClient::TColumnFilter& columnFilter);

    void Start(const NVersionedTableClient::TUnversionedValue* keyBegin);

    void AddPartialRow(NVersionedTableClient::TVersionedRow row);

    NVersionedTableClient::TUnversionedRow BuildMergedRow(bool skipNulls = false);

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    NVersionedTableClient::TKeyComparer KeyComparer_;
    SmallVector<NVersionedTableClient::TVersionedValue, NVersionedTableClient::TypicalColumnCount> MergedValues_;
    SmallVector<bool, NVersionedTableClient::TypicalColumnCount> ColumnFlags_;
    SmallVector<int, NVersionedTableClient::TypicalColumnCount> ColumnIds_;
    
    TTimestamp CurrentTimestamp_;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMerger
{
public:
    TVersionedRowMerger(
        TChunkedMemoryPool* pool,
        int schemaColumnCount,
        int keyColumnCount);

    void Start(const NVersionedTableClient::TUnversionedValue* keyBegin);

    void AddPartialRow(NVersionedTableClient::TVersionedRow row);

    NVersionedTableClient::TVersionedRow BuildMergedRow();

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    NVersionedTableClient::TKeyComparer KeyComparer_;

    SmallVector<NVersionedTableClient::TUnversionedValue, NVersionedTableClient::TypicalColumnCount> Keys_;
    
    std::vector<NVersionedTableClient::TVersionedValue> Values_;
    std::vector<NVersionedTableClient::TVersionedValue> MergedValues_;

    std::vector<NVersionedTableClient::TTimestamp> Timestamps_;
    std::vector<NVersionedTableClient::TTimestamp> MergedTimestamps_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
