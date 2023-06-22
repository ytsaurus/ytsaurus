#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"
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

//! We try to read #blockIndexes blocks with #readers (that are considered available).
//! Repair of blocks corresponding to other (erased) readers is performed if necessary.
TFuture<std::vector<TBlock>> ExecuteErasureRepairingSession(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    NErasure::TPartIndexList erasedIndices,
    std::vector<IChunkReaderAllowingRepairPtr> readers,
    std::vector<int> blockIndexes,
    IChunkReader::TReadBlocksOptions options,
    IInvokerPtr readerInvoker,
    NLogging::TLogger logger,
    NChunkClient::NProto::TErasurePlacementExt placementExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
