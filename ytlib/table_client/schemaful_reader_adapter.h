#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessReaderPtr(TNameTablePtr, TColumnFilter)> TSchemalessReaderFactory;

TFuture<ISchemafulReaderPtr> CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
