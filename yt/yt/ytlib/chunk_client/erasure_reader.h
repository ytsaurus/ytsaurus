#pragma once

#include "public.h"

#include <yt/library/erasure/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// This reader can adaptively exclude and include underlying part readers
// depending on read timeouts and read efficiency.
IChunkReaderPtr CreateAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& partReaders,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
