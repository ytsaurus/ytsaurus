#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/logging/log.h>

#include <ytlib/api/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/chunk_service.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TChunkTeleporter
    : public TRefCounted
{
public:
    TChunkTeleporter(
        TSchedulerConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        const NTransactionClient::TTransactionId& transactionId,
        const NLogging::TLogger& logger);

    void RegisterChunk(
        const NChunkClient::TChunkId& chunkId,
        NObjectClient::TCellTag destinationCellTag);

    TFuture<void> Run();

private:
    const TSchedulerConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NTransactionClient::TTransactionId TransactionId_;

    NLogging::TLogger Logger;

    struct TChunkEntry
    {
        TChunkEntry(
            const NChunkClient::TChunkId& chunkId,
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
    void Export();
    void Import();

};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
