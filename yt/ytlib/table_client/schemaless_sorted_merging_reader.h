#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int keyColumnCount,
    bool enableTableIndex = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
