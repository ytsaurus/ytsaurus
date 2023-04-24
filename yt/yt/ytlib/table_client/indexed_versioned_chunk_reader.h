#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateIndexedVersionedChunkReader(
    NChunkClient::TClientChunkReadOptions options,
    IChunkIndexReadControllerPtr controller,
    NChunkClient::IChunkReaderPtr chunkReader,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
