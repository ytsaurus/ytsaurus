#pragma once

#include "public.h"

#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessReaderPtr(TNameTablePtr, TColumnFilter)> TSchemalessReaderFactory;

ISchemafulReaderPtr CreateSchemafulReaderAdapter(TSchemalessReaderFactory createReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
