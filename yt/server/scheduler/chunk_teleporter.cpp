#include "stdafx.h"
#include "chunk_teleporter.h"
#include "config.h"

#include <ytlib/api/client.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////

TChunkTeleporter::TChunkTeleporter(
    TSchedulerConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker,
    const TTransactionId& transactionId,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Client_(client)
    , Invoker_(invoker)
    , TransactionId_(transactionId)
    , Logger(logger)
{ }

void TChunkTeleporter::RegisterChunk(
    const TChunkId& chunkId,
    TCellTag destinationCellTag)
{
    if (CellTagFromId(chunkId) != destinationCellTag) {
        Chunks_.emplace_back(chunkId, destinationCellTag);
    }
}

TFuture<void> TChunkTeleporter::Run()
{
    return BIND(&TChunkTeleporter::DoRun, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TChunkTeleporter::DoRun()
{
    LOG_INFO("Chunk teleport started (ChunkCount: %v)",
        Chunks_.size());
    Export();
    Import();
    LOG_INFO("Chunk teleport completed");
}

void TChunkTeleporter::Export()
{
    yhash_map<TCellTag, std::vector<TChunkEntry*>> exportMap;
    for (auto& chunk : Chunks_) {
        exportMap[CellTagFromId(chunk.ChunkId)].push_back(&chunk);
    }

    for (const auto& pair : exportMap) {
        auto cellTag = pair.first;
        const auto& chunks = pair.second;

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunks.size(); beginIndex += Config_->MaxTeleportChunksPerRequest) {
            int endIndex = std::min(
                beginIndex + Config_->MaxTeleportChunksPerRequest,
                static_cast<int>(chunks.size()));

            auto req = proxy.ExportChunks();
            ToProto(req->mutable_transaction_id(), TransactionId_);
            for (int index = beginIndex; index < endIndex; ++index) {
                ToProto(req->add_chunk_ids(), chunks[index]->ChunkId);
            }

            LOG_INFO("Exporting chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->chunk_ids_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error exporting chunks from cell %v",
                cellTag);
            const auto& rsp = rspOrError.Value();

            YCHECK(rsp->chunks_size() == endIndex - beginIndex);
            for (int index = beginIndex; index < endIndex; ++index) {
                chunks[index]->Data.Swap(rsp->mutable_chunks(index - beginIndex));
            }
        }
    }
}

void TChunkTeleporter::Import()
{
    yhash_map<TCellTag, std::vector<TChunkEntry*>> importMap;
    for (auto& chunk : Chunks_) {
        importMap[chunk.DestinationCellTag].push_back(&chunk);
    }

    for (const auto& pair : importMap) {
        auto cellTag = pair.first;
        const auto& chunks = pair.second;

        auto channel = Client_->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunks.size(); beginIndex += Config_->MaxTeleportChunksPerRequest) {
            int endIndex = std::min(
                beginIndex + Config_->MaxTeleportChunksPerRequest,
                static_cast<int>(chunks.size()));

            auto req = proxy.ImportChunks();
            ToProto(req->mutable_transaction_id(), TransactionId_);
            for (int index = beginIndex; index < endIndex; ++index) {
                req->add_chunks()->Swap(&chunks[index]->Data);
            }

            LOG_INFO("Exporting chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->chunks_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error import chunks into cell %v",
                cellTag);
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

