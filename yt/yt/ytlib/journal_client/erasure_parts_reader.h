#pragma once

#include "private.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/misc/ref.h>

#include <yt/core/actions/future.h>

#include <yt/core/logging/log.h>

#include <yt/library/erasure/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

class TErasurePartsReader
    : public TRefCounted
{
public:
    TErasurePartsReader(
        TChunkReaderConfigPtr config,
        NErasure::ECodec codecId,
        std::vector<NChunkClient::IChunkReaderPtr> readers,
        const NErasure::TPartIndexList& partIndices,
        NLogging::TLogger logger);

    TFuture<std::vector<std::vector<TSharedRef>>> ReadRows(
        const NChunkClient::TClientBlockReadOptions& options,
        int firstRowIndex,
        int rowCount);

private:
    const TChunkReaderConfigPtr Config_;
    const NErasure::ECodec CodecId_;
    const std::vector<NChunkClient::IChunkReaderPtr> ChunkReaders_;
    const NErasure::TPartIndexList PartIndices_;
    const NLogging::TLogger Logger;

    const NChunkClient::TChunkId ChunkId_;

    class TReadRowsSession;
};

DEFINE_REFCOUNTED_TYPE(TErasurePartsReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
