#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/library/erasure/public.h>

#include <yt/core/actions/future-inl.h>

#include <yt/core/logging/log.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedParts(
    TChunkReaderConfigPtr config,
    NErasure::ECodec codecId,
    i64 rowCount,
    const NErasure::TPartIndexList& erasedIndices,
    std::vector<NChunkClient::IChunkReaderPtr> readers,
    std::vector<NChunkClient::IChunkWriterPtr> writers,
    NChunkClient::TClientBlockReadOptions options,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
