#pragma once

#include "public.h"

#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessWriterPtr(NVersionedTableClient::TNameTablePtr)> TSchemalessWriterFactory;

ISchemafulWriterPtr CreateSchemafulWriterAdapter(TSchemalessWriterFactory createWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
