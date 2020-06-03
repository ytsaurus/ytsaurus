#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int sortKeyColumnCount,
    int reduceKeyColumnCount,
    bool interruptAtKeyEdge);

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    int reduceKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount,
    bool interruptAtKeyEdge);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
