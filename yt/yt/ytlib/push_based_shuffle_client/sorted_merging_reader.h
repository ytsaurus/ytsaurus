#pragma once

#include "record_format.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/comparator.h>

#include <vector>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessMultiChunkReaderPtr CreateIdentityAwareSortedMergingReader(
    const std::vector<NTableClient::ISchemalessMultiChunkReaderPtr>& readers,
    NTableClient::TComparator sortComparator,
    TIdentityColumnIds identityColumnIds,
    TValidWriterIds validWriterIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
