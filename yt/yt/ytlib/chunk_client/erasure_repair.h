#pragma once

#include "public.h"

#include "chunk_reader_allowing_repair.h"

#include <yt/yt/library/erasure/impl/public.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedParts(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    const std::vector<IChunkWriterPtr>& writers,
    const IChunkReader::TReadBlocksOptions& options);

IChunkReaderPtr CreateRepairingErasureReader(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    // This list must consist of readers for all data parts and repair parts sorted by part index.
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

using TPartWriterFactory = std::function<NChunkClient::IChunkWriterPtr(int partIndex)>;

TFuture<void> AdaptiveRepairErasedParts(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    TErasureReaderConfigPtr config,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    TPartWriterFactory writerFactory,
    const IChunkReader::TReadBlocksOptions& options,
    const NLogging::TLogger& logger = {},
    NProfiling::TCounter adaptivelyRepairedCounter = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
