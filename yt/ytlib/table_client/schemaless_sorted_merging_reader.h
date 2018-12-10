#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int sortKeyColumnCount,
    int reduceKeyColumnCount);

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    int reduceKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount);

ISchemalessMultiChunkReaderPtr CreateSchemalessJoinReduceJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    int reduceKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
