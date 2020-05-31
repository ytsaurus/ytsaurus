#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that concatenates the rowsets returned by the
//! underlying readers.
ISchemafulUnversionedReaderPtr CreateSchemafulConcatenatingReader(
    std::vector<std::function<ISchemafulUnversionedReaderPtr()>> underlyingReaderFactories);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
