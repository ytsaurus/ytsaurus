#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/s3/public.h>
#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////

NIO::IWrapperFairShareChunkWriterPtr CreateS3RegularChunkWriter(
    NS3::IClientPtr client,
    NChunkClient::TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    NChunkClient::TSessionId sessionId);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
