#pragma once

#include "public.h"

#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemafulReaderPtr(const TTableSchema&, const TColumnFilter&)> TSchemafulReaderFactory;

IVersionedReaderPtr CreateVersionedReaderAdapter(
    TSchemafulReaderFactory createReader,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
