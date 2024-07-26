#include "chunk_teleporter.h"
#include "config.h"
#include "chunk_service_proxy.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NObjectClient;
using namespace NCypressClient;
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
    TTransactionId transactionId,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , TransactionId_(transactionId)
    , Logger(logger)
{ }

void TChunkTeleporter::RegisterChunk(
    TChunkId chunkId,
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
    YT_LOG_INFO("Chunk teleport started (ChunkCount: %v)",
        Chunks_.size());
    Export();
    Import();
    YT_LOG_INFO("Chunk teleport completed");
}

int TChunkTeleporter::GetExportedObjectCount(TCellTag cellTag)
{
    auto proxy = CreateObjectServiceWriteProxy(Client_, cellTag);

    auto req = TObjectYPathProxy::Get("&#" + ToString(TransactionId_) + "/@local_exported_object_count");
    AddCellTagToSyncWith(req, CellTagFromId(TransactionId_));

    auto rspOrError = WaitFor(proxy.Execute(req));
    // COMPAT(gritukan): Remove it after local_exported_object_count is supported everywhere.
    if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        req = TObjectYPathProxy::Get("&#" + ToString(TransactionId_) + "/@exported_object_count");
        AddCellTagToSyncWith(req, CellTagFromId(TransactionId_));
        rspOrError = WaitFor(proxy.Execute(req));
    }

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
        auto cellTag = CellTagFromId(chunk.ChunkId);
        exportMap[cellTag].push_back(&chunk);
    }

    std::vector<TFuture<void>> exportTasks;
    for (const auto& [cellTag, chunks] : exportMap) {
        exportTasks.push_back(BIND(&TChunkTeleporter::DoExport, MakeStrong(this), cellTag, chunks)
            .AsyncVia(Invoker_)
            .Run());
    }
    WaitFor(AllSucceeded(std::move(exportTasks)))
        .ThrowOnError();
}

void TChunkTeleporter::DoExport(TCellTag cellTag, const std::vector<TChunkEntry*>& chunks)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    int oldExportedCount = GetExportedObjectCount(cellTag);

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    for (int beginIndex = 0; beginIndex < std::ssize(chunks); beginIndex += Config_->MaxTeleportChunksPerRequest) {
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
            protoData->set_destination_cell_tag(ToProto<int>(entry->DestinationCellTag));
        }

        YT_LOG_INFO("Exporting chunks (CellTag: %v, ChunkCount: %v)",
            cellTag,
            req->chunks_size());

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error exporting chunks in transaction %v in cell %v",
            TransactionId_,
            cellTag);
        const auto& rsp = rspOrError.Value();

        YT_VERIFY(rsp->chunks_size() == endIndex - beginIndex);
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

int TChunkTeleporter::GetImportedObjectCount(TCellTag cellTag)
{
    auto proxy = CreateObjectServiceWriteProxy(Client_, cellTag);

    // COMPAT(shakurov)
    // Replace this with a newer syntax "&#OBJECT_ID" for redirect suppression.
    auto req = TObjectYPathProxy::Get("//sys/transactions/" + ToString(TransactionId_) + "/@imported_object_count");

    AddCellTagToSyncWith(req, CellTagFromId(TransactionId_));

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

    std::vector<TFuture<void>> importTasks;
    for (const auto& [cellTag, chunks] : importMap) {
        importTasks.push_back(BIND(&TChunkTeleporter::DoImport, MakeStrong(this), cellTag, chunks)
            .AsyncVia(Invoker_)
            .Run());
    }
    WaitFor(AllSucceeded(std::move(importTasks)))
        .ThrowOnError();
}

void TChunkTeleporter::DoImport(TCellTag cellTag, const std::vector<TChunkEntry*>& chunks)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    int oldImportedCount = GetImportedObjectCount(cellTag);

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    for (int beginIndex = 0; beginIndex < std::ssize(chunks); beginIndex += Config_->MaxTeleportChunksPerRequest) {
        int endIndex = std::min(
            beginIndex + Config_->MaxTeleportChunksPerRequest,
            static_cast<int>(chunks.size()));

        auto req = proxy.ImportChunks();
        GenerateMutationId(req);
        ToProto(req->mutable_transaction_id(), TransactionId_);
        for (int index = beginIndex; index < endIndex; ++index) {
            req->add_chunks()->Swap(&chunks[index]->Data);
        }

        YT_LOG_INFO("Importing chunks (CellTag: %v, ChunkCount: %v)",
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
