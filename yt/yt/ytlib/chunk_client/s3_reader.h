#pragma once

#include "public.h"
#include "s3_common.h"
#include "medium_directory.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateS3Reader(
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3ReaderConfigPtr config,
    TChunkId chunkId,
    EChunkFormat chunkFormat,
    std::string_view objectKey);

IChunkReaderPtr TryCreateS3ReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient