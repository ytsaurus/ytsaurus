#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/s3/public.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////

NChunkClient::IChunkWriterPtr CreateS3RegularChunkWriter(
    NS3::IClientPtr client,
    NChunkClient::TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    NChunkClient::TSessionId sessionId);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
