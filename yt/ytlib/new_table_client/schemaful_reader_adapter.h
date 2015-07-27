#pragma once

#include "public.h"

#include "unversioned_row.h"

#include <core/actions/future.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessReaderPtr(TNameTablePtr, TColumnFilter)> TSchemalessReaderFactory;

TFuture<ISchemafulReaderPtr> CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
