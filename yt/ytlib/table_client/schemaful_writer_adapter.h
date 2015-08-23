#pragma once

#include "public.h"

#include "unversioned_row.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<ISchemalessWriterPtr(TNameTablePtr)> TSchemalessWriterFactory;

ISchemafulWriterPtr CreateSchemafulWriterAdapter(TSchemalessWriterFactory createWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
