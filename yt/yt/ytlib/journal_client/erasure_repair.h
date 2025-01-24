#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedParts(
    TChunkReaderConfigPtr config,
    NErasure::ICodec* codec,
    i64 rowCount,
    const NErasure::TPartIndexList& erasedIndices,
    std::vector<NChunkClient::IChunkReaderPtr> readers,
    std::vector<NChunkClient::IChunkWriterPtr> writers,
    NChunkClient::TClientChunkReadOptions options,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
