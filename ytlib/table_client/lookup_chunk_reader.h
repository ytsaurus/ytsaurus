#pragma once

#include <yt/server/node/tablet_node/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRowLookupReader(
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::TClientBlockReadOptions blockReadOptions,
    TSharedRange<TKey> lookupKeys,
    NTabletNode::TTabletSnapshotPtr tabletSnapshot,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
