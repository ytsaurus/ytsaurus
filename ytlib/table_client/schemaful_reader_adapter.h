#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessReaderPtr(TNameTablePtr, const TColumnFilter&)> TSchemalessReaderFactory;

ISchemafulReaderPtr CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter = TColumnFilter());

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
