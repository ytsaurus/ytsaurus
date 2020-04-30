#pragma once

#include "public.h"
#include "schemaless_multi_chunk_reader.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    TKeyColumns keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
