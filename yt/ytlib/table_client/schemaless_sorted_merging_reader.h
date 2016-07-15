#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int keyColumnCount);

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount);

ISchemalessMultiChunkReaderPtr CreateSchemalessJoinReduceJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
