#include "chunk_teleporter.h"
#include "config.h"
#include "chunk_service_proxy.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/helpers.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TChunkTeleporter::TChunkTeleporter(
    TChunkTeleporterConfigPtr config,
    NNative::IClientPtr client,
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

int TChunkTeleporter::GetExportedObjectCount(TCellTag cellTag)
{
    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TObjectServiceProxy proxy(channel);

    auto req = TObjectYPathProxy::Get(FromObjectId(TransactionId_) + "/@exported_object_count");
    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting exported object count for transaction %v in cell %v",
        TransactionId_,
        cellTag);

    const auto& rsp = rspOrError.Value();
    return ConvertTo<int>(TYsonString(rsp->value()));
}

void TChunkTeleporter::Export()
{
    THashMap<TCellTag, std::vector<TChunkEntry*>> exportMap;
    for (auto& chunk : Chunks_) {
        exportMap[CellTagFromId(chunk.ChunkId)].push_back(&chunk);
    }

    for (const auto& pair : exportMap) {
        auto cellTag = pair.first;
        const auto& chunks = pair.second;

        int oldExportedCount = GetExportedObjectCount(cellTag);

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunks.size(); beginIndex += Config_->MaxTeleportChunksPerRequest) {
            int endIndex = std::min(
                beginIndex + Config_->MaxTeleportChunksPerRequest,
                static_cast<int>(chunks.size()));

            auto req = proxy.ExportChunks();
            GenerateMutationId(req);
            ToProto(req->mutable_transaction_id(), TransactionId_);
            for (int index = beginIndex; index < endIndex; ++index) {
                auto* protoData = req->add_chunks();
                const auto* entry = chunks[index];
                ToProto(protoData->mutable_id(), entry->ChunkId);
                protoData->set_destination_cell_tag(entry->DestinationCellTag);
            }

            LOG_INFO("Exporting chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->chunks_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error exporting chunks in transaction %v in cell %v",
                TransactionId_,
                cellTag);
            const auto& rsp = rspOrError.Value();

            YCHECK(rsp->chunks_size() == endIndex - beginIndex);
            for (int index = beginIndex; index < endIndex; ++index) {
                chunks[index]->Data.Swap(rsp->mutable_chunks(index - beginIndex));
            }
        }

        int newExportedCount = GetExportedObjectCount(cellTag);
        int expectedExportedCount = oldExportedCount + static_cast<int>(chunks.size());
        if (newExportedCount != expectedExportedCount) {
            THROW_ERROR_EXCEPTION("Exported object count mismatch for transaction %v in cell %v: expected %v, got %v",
                TransactionId_,
                cellTag,
                expectedExportedCount,
                newExportedCount);
        }
    }
}

int TChunkTeleporter::GetImportedObjectCount(TCellTag cellTag)
{
    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TObjectServiceProxy proxy(channel);

    auto req = TObjectYPathProxy::Get(FromObjectId(TransactionId_) + "/@imported_object_count");
    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting imported object count for transaction %v in cell %v",
        TransactionId_,
        cellTag);

    const auto& rsp = rspOrError.Value();
    return ConvertTo<int>(TYsonString(rsp->value()));
}

void TChunkTeleporter::Import()
{
    THashMap<TCellTag, std::vector<TChunkEntry*>> importMap;
    for (auto& chunk : Chunks_) {
        importMap[chunk.DestinationCellTag].push_back(&chunk);
    }

    for (const auto& pair : importMap) {
        auto cellTag = pair.first;
        const auto& chunks = pair.second;

        int oldImportedCount = GetImportedObjectCount(cellTag);

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunks.size(); beginIndex += Config_->MaxTeleportChunksPerRequest) {
            int endIndex = std::min(
                beginIndex + Config_->MaxTeleportChunksPerRequest,
                static_cast<int>(chunks.size()));

            auto req = proxy.ImportChunks();
            GenerateMutationId(req);
            ToProto(req->mutable_transaction_id(), TransactionId_);
            for (int index = beginIndex; index < endIndex; ++index) {
                req->add_chunks()->Swap(&chunks[index]->Data);
            }

            LOG_INFO("Importing chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->chunks_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error importing chunks in transaction %v in cell %v",
                TransactionId_,
                cellTag);
        }

        int newImportedCount = GetImportedObjectCount(cellTag);
        int expectedImportedCount = oldImportedCount + static_cast<int>(chunks.size());
        if (newImportedCount != expectedImportedCount) {
            THROW_ERROR_EXCEPTION("Imported object count mismatch for transaction %v in cell %v: expected %v, got %v",
                TransactionId_,
                cellTag,
                expectedImportedCount,
                newImportedCount);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

