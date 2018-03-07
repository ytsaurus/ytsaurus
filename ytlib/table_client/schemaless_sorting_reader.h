#pragma once

#include "public.h"
#include "schemaless_chunk_reader.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    TKeyColumns keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
