#pragma once

#include "public.h"

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TRepairingErasureReaderTestingOptions
{
    NErasure::TPartIndexList ErasedIndices;
};

// This reader can adaptively exclude and include underlying part readers
// depending on read timeouts and read efficiency.
IChunkReaderPtr CreateAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    TErasureReaderConfigPtr config,
    std::vector<IChunkReaderAllowingRepairPtr> partReaders,
    std::optional<TRepairingErasureReaderTestingOptions> testingOptions,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
