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
        const NLogging::TLogger& logger,
        std::optional<bool> useLazyTransactionExternalizationOverride = std::nullopt);

    void RegisterChunk(
        NChunkClient::TChunkId chunkId,
        NObjectClient::TCellTag destinationCellTag,
        NTransactionClient::TTransactionId sourceTransactionId,
        NTransactionClient::TTransactionId destinationTransactionId);

    void RegisterChunk(NChunkClient::TChunkId chunkId, NObjectClient::TCellTag destinationCellTag);

    TFuture<void> Run();

private:
    const TChunkTeleporterConfigPtr Config_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NTransactionClient::TTransactionId TransactionId_;
    const bool UseLazyTransactionExternalization_;

    NLogging::TLogger Logger;

    struct TChunkEntry
    {
        TChunkEntry(
            NChunkClient::TChunkId chunkId,
            NTransactionClient::TTransactionId sourceTransactionId,
            NObjectClient::TCellTag destinationCellTag,
            NTransactionClient::TTransactionId destinationTransactionId)
            : ChunkId(chunkId)
            , SourceTransactionId(sourceTransactionId)
            , DestinationCellTag(destinationCellTag)
            , DestinationTransactionId(destinationTransactionId)
        { }

        NChunkClient::TChunkId ChunkId;
        NTransactionClient::TTransactionId SourceTransactionId;
        NObjectClient::TCellTag DestinationCellTag;
        NTransactionClient::TTransactionId DestinationTransactionId;
        NChunkClient::NProto::TChunkImportData Data;
    };

    std::vector<TChunkEntry> Chunks_;

    void DoRun();

    int GetExportedObjectCount(NObjectClient::TCellTag cellTag, NTransactionClient::TTransactionId transactionId);
    void Export();
    void DoExport(
        NObjectClient::TCellTag cellTag,
        const std::vector<TChunkEntry*>& chunks,
        NTransactionClient::TTransactionId sourceTransactionId);

    int GetImportedObjectCount(NObjectClient::TCellTag cellTag, NTransactionClient::TTransactionId transactionId);
    void Import();
    void DoImport(
        NObjectClient::TCellTag cellTag,
        const std::vector<TChunkEntry*>& chunks,
        NTransactionClient::TTransactionId destinationTransactionId);

};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
