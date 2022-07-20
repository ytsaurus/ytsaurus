#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

class TErasurePartsReader
    : public TRefCounted
{
public:
    TErasurePartsReader(
        TChunkReaderConfigPtr config,
        NErasure::ICodec* codec,
        std::vector<NChunkClient::IChunkReaderPtr> readers,
        const NErasure::TPartIndexList& partIndices,
        NLogging::TLogger logger);

    TFuture<std::vector<std::vector<TSharedRef>>> ReadRows(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstRowIndex,
        int rowCount,
        bool enableFastPath);

private:
    const TChunkReaderConfigPtr Config_;
    NErasure::ICodec* const Codec_;
    const std::vector<NChunkClient::IChunkReaderPtr> ChunkReaders_;
    const NErasure::TPartIndexList PartIndices_;
    const NLogging::TLogger Logger;

    const NChunkClient::TChunkId ChunkId_;

    class TReadRowsSession;
};

DEFINE_REFCOUNTED_TYPE(TErasurePartsReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
