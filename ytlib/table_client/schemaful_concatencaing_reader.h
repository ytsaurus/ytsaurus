#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that concatenates the rowsets returned by the
//! underlying readers.
ISchemafulReaderPtr CreateSchemafulConcatenatingReader(
    std::vector<std::function<ISchemafulReaderPtr()>> underlyingReaderFactories);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
