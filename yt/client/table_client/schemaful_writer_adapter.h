#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessWriterPtr(TNameTablePtr)> TSchemalessWriterFactory;

ISchemafulWriterPtr CreateSchemafulWriterAdapter(ISchemalessWriterPtr underlyingWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
