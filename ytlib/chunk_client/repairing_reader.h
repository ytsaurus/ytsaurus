#pragma once

#include "public.h"

#include <yt/library/erasure/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRepairingReader(
    NErasure::ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& dataBlocksReaders,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
