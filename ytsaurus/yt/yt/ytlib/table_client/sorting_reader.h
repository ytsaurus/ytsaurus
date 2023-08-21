#pragma once

#include "public.h"
#include "schemaless_multi_chunk_reader.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Create a reader, that reads rows from #underlyingReader till the end
//! and then sorts them by #keyColumns using #comparator.
ISchemalessMultiChunkReaderPtr CreateSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    TKeyColumns keyColumns,
    TComparator comparator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
