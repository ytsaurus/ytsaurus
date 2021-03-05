#pragma once

#include "public.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    TComparator sortComparator,
    TComparator reduceComparator,
    bool interruptAtKeyEdge);

ISchemalessMultiChunkReaderPtr CreateSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    TComparator primaryComparator,
    TComparator reduceComparator,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    TComparator foreignComparator,
    bool interruptAtKeyEdge);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
