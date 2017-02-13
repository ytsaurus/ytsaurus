#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that concatenates the rowsets returned by the
//! underlying readers.
ISchemafulReaderPtr CreateSchemafulConcatencatingReader(
    std::vector<ISchemafulReaderPtr> underlyingReaders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
