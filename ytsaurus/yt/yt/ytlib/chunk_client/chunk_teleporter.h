#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkTeleporter
    : public TRefCounted
{
public:
    TChunkTeleporter(
        TChunkTeleporterConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NTransactionClient::TTransactionId transactionId,
        const NLogging::TLogger& logger);

    void RegisterChunk(
        NChunkClient::TChunkId chunkId,
        NObjectClient::TCellTag destinationCellTag);

    TFuture<void> Run();

private:
    const TChunkTeleporterConfigPtr Config_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NTransactionClient::TTransactionId TransactionId_;

    NLogging::TLogger Logger;

    struct TChunkEntry
    {
        TChunkEntry(
            NChunkClient::TChunkId chunkId,
            NObjectClient::TCellTag destinationCellTag)
            : ChunkId(chunkId)
            , DestinationCellTag(destinationCellTag)
        { }

        NChunkClient::TChunkId ChunkId;
        NObjectClient::TCellTag DestinationCellTag;
        NChunkClient::NProto::TChunkImportData Data;
    };

    std::vector<TChunkEntry> Chunks_;

    void DoRun();

    int GetExportedObjectCount(NObjectClient::TCellTag cellTag);
    void Export();
    void DoExport(NObjectClient::TCellTag cellTag, const std::vector<TChunkEntry*>& chunks);

    int GetImportedObjectCount(NObjectClient::TCellTag cellTag);
    void Import();
    void DoImport(NObjectClient::TCellTag cellTag, const std::vector<TChunkEntry*>& chunks);

};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
