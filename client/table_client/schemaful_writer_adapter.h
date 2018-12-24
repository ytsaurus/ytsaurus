#pragma once

#include "public.h"

#include <yt/client/table_client/unversioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<IUnversionedWriterPtr(TNameTablePtr)> TSchemalessWriterFactory;

IUnversionedRowsetWriterPtr CreateSchemafulWriterAdapter(IUnversionedWriterPtr underlyingWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
