#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemafulReaderPtr(const TTableSchema&, const TColumnFilter&)> TSchemafulReaderFactory;

ISchemalessReaderPtr CreateSchemalessReaderAdapter(
    TSchemafulReaderFactory createReader,
    TTableReaderOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter,
    int tableIndex,
    int rangeIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
