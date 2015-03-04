#pragma once

#include "public.h"

#include "schemaless_chunk_reader.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    TKeyColumns keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
