#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/core/misc/shared_range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRowLookupReader(
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::TClientBlockReadOptions blockReadOptions,
    TSharedRange<TKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
