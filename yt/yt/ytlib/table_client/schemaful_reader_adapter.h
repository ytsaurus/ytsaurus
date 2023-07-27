#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

using TSchemalessReaderFactory = std::function<ISchemalessUnversionedReaderPtr(
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter)> ;

ISchemafulUnversionedReaderPtr CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    TTableSchemaPtr schema,
    const TColumnFilter& columnFilter = {},
    bool ignoreRequired = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
