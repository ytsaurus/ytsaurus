#pragma once

#include "public.h"
#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessRowReorderer
    : public TNonCopyable
{
public:
    TSchemalessRowReorderer(
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns);

    TUnversionedRow ReorderRow(TUnversionedRow row, TChunkedMemoryPool* memoryPool);
    TUnversionedOwningRow ReorderRow(TUnversionedRow row);

private:
    TKeyColumns KeyColumns_;
    TNameTablePtr NameTable_;

    std::vector<int> IdMapping_;
    std::vector<TUnversionedValue> EmptyKey_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
