#pragma once

#include "public.h"
#include "chunk_reader_allowing_repair.h"

#include <yt/core/erasure/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRepairingReader(
    NErasure::ICodec *codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& dataBlocksReaders,
    const NLogging::TLogger& logger = NLogging::TLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
