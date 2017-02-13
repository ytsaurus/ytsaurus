#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemafulReaderPtr(const TTableSchema&, const TColumnFilter&)> TSchemafulReaderFactory;

ISchemalessReaderPtr CreateSchemalessReaderAdapter(
    TSchemafulReaderFactory createReader,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
