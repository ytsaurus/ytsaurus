#pragma once

#include "public.h"

#include <yt/yt/library/s3/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////

NChunkClient::IChunkReaderPtr CreateS3RegularChunkReader(
    NS3::IClientPtr client,
    const NChunkClient::TS3MediumDescriptorPtr& mediumDescriptor,
    TS3ReaderConfigPtr config,
    NChunkClient::TChunkId chunkId);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
