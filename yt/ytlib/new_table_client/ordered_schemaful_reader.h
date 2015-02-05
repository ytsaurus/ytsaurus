#pragma once

#include "public.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateOrderedSchemafulReader(const std::function<ISchemafulReaderPtr()>& getNextReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
