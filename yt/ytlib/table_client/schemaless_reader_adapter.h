#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessReaderPtr CreateSchemalessReaderAdapter(
    ISchemafulReaderPtr underlyingReader,
    TTableReaderOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    int tableIndex,
    int rangeIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
