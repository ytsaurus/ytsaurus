#include "chunk_teleporter.h"
#include "config.h"
#include "chunk_service_proxy.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

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

namespace {

////////////////////////////////////////////////////////////////////////////////

bool HasNoSuchTransactionError(const TError& responseError, TTransactionId transactionId)
{
    return responseError.FindMatching([&] (const TError& error) {
        return
            error.GetCode() == NYTree::EErrorCode::ResolveError &&
            error.HasAttributes() &&
            error.Attributes().Find<TObjectId>("missing_object_id") == transactionId;
    }).has_value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChunkTeleporter::TChunkTeleporter(
    TChunkTeleporterConfigPtr config,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    TTransactionId transactionId,
    const NLogging::TLogger& logger,
    std::optional<bool> useLazyTransactionExternalizationOverride)
    : Config_(std::move(config))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , TransactionId_(transactionId)
    , UseLazyTransactionExternalization_(useLazyTransactionExternalizationOverride.value_or(Config_->UseLazyTransactionExternalization))
    , Logger(logger)
{ }

void TChunkTeleporter::RegisterChunk(
    TChunkId chunkId,
    TCellTag destinationCellTag,
    TTransactionId sourceTransactionId,
    TTransactionId destinationTransactionId)
{
    if (CellTagFromId(chunkId) == destinationCellTag) {
        return;
    }

    if (!UseLazyTransactionExternalization_) {
        sourceTransactionId = TransactionId_;
        destinationTransactionId = TransactionId_;
    }

    Chunks_.emplace_back(
        chunkId,
        sourceTransactionId,
        destinationCellTag,
        destinationTransactionId);
}

void TChunkTeleporter::RegisterChunk(TChunkId chunkId, TCellTag destinationCellTag)
{
    RegisterChunk(chunkId, destinationCellTag, TransactionId_, TransactionId_);
}

TFuture<void> TChunkTeleporter::Run()
{
    return BIND(&TChunkTeleporter::DoRun, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TChunkTeleporter::DoRun()
{
    YT_TLOG_INFO("Chunk teleport started")
        .With("ChunkCount", Chunks_.size())
        .With("UseLazyTransactionExternalization", UseLazyTransactionExternalization_);
    Export();
    Import();
    YT_TLOG_INFO("Chunk teleport completed");
}

int TChunkTeleporter::GetExportedObjectCount(TCellTag cellTag, TTransactionId transactionId)
{
    auto proxy = CreateObjectServiceWriteProxy(Client_, cellTag);

    auto req = TObjectYPathProxy::Get("&#" + ToString(transactionId) + "/@local_exported_object_count");
    AddCellTagToSyncWith(req, CellTagFromId(TransactionId_));

    auto rspOrError = WaitFor(proxy.Execute(req));
    if (UseLazyTransactionExternalization_ && HasNoSuchTransactionError(rspOrError, transactionId)) {
        return 0;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting exported object count for transaction %v in cell %v",
        transactionId,
        cellTag);

    const auto& rsp = rspOrError.Value();
    return ConvertTo<int>(TYsonString(rsp->value()));
}

void TChunkTeleporter::Export()
{
    THashMap<std::pair<TCellTag, TTransactionId>, std::vector<TChunkEntry*>> exportMap;
    for (auto& chunk : Chunks_) {
        // NB: Ensure that journal hunk chunks won't be teleported.
        if (IsJournalChunkId(chunk.ChunkId)) {
            THROW_ERROR_EXCEPTION("Cannot teleport journal chunk %v",
                chunk.ChunkId);
        }

        auto cellTag = CellTagFromId(chunk.ChunkId);
        auto transactionId = UseLazyTransactionExternalization_
            ? chunk.SourceTransactionId
            : TransactionId_;
        exportMap[std::pair(cellTag, transactionId)].push_back(&chunk);
    }

    std::vector<TFuture<void>> exportTasks;
    for (const auto& [key, chunks] : exportMap) {
        exportTasks.push_back(
            BIND(&TChunkTeleporter::DoExport, MakeStrong(this), key.first, chunks, key.second)
                .AsyncVia(Invoker_)
                .Run());
    }
    WaitFor(AllSucceeded(std::move(exportTasks)))
        .ThrowOnError();
}

void TChunkTeleporter::DoExport(
    TCellTag cellTag,
    const std::vector<TChunkEntry*>& chunks,
    TTransactionId sourceTransactionId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    int oldExportedCount = GetExportedObjectCount(cellTag, sourceTransactionId);

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    for (int beginIndex = 0; beginIndex < std::ssize(chunks); beginIndex += Config_->MaxTeleportChunksPerRequest) {
        int endIndex = std::min(
            beginIndex + Config_->MaxTeleportChunksPerRequest,
            std::ssize(chunks));

        auto req = proxy.ExportChunks();
        GenerateMutationId(req);
        ToProto(req->mutable_transaction_id(), sourceTransactionId);
        for (int index = beginIndex; index < endIndex; ++index) {
            auto* protoData = req->add_chunks();
            const auto* entry = chunks[index];
            ToProto(protoData->mutable_id(), entry->ChunkId);
            protoData->set_destination_cell_tag(ToProto(entry->DestinationCellTag));
        }

        YT_TLOG_INFO("Exporting chunks")
            .With("CellTag", cellTag)
            .With("ChunkCount", req->chunks_size());

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error exporting chunks in transaction %v in cell %v",
            sourceTransactionId,
            cellTag);
        const auto& rsp = rspOrError.Value();

        YT_VERIFY(rsp->chunks_size() == endIndex - beginIndex);
        for (int index = beginIndex; index < endIndex; ++index) {
            chunks[index]->Data.Swap(rsp->mutable_chunks(index - beginIndex));
        }
    }

    int newExportedCount = GetExportedObjectCount(cellTag, sourceTransactionId);
    int expectedExportedCount = oldExportedCount + std::ssize(chunks);
    if (newExportedCount != expectedExportedCount) {
        THROW_ERROR_EXCEPTION("Exported object count mismatch for transaction %v in cell %v: expected %v, got %v",
            sourceTransactionId,
            cellTag,
            expectedExportedCount,
            newExportedCount);
    }
}

int TChunkTeleporter::GetImportedObjectCount(TCellTag cellTag, TTransactionId transactionId)
{
    auto proxy = CreateObjectServiceWriteProxy(Client_, cellTag);

    auto req = TObjectYPathProxy::Get("&#" + ToString(transactionId) + "/@imported_object_count");

    AddCellTagToSyncWith(req, CellTagFromId(TransactionId_));

    auto rspOrError = WaitFor(proxy.Execute(req));
    if (UseLazyTransactionExternalization_ && HasNoSuchTransactionError(rspOrError, transactionId)) {
        return 0;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting imported object count for transaction %v in cell %v",
        transactionId,
        cellTag);

    const auto& rsp = rspOrError.Value();
    return ConvertTo<int>(TYsonString(rsp->value()));
}

void TChunkTeleporter::Import()
{
    THashMap<std::pair<TCellTag, TTransactionId>, std::vector<TChunkEntry*>> importMap;
    for (auto& chunk : Chunks_) {
        auto cellTag = chunk.DestinationCellTag;
        auto transactionId = UseLazyTransactionExternalization_
            ? chunk.DestinationTransactionId
            : TransactionId_;
        importMap[std::pair(cellTag, transactionId)].push_back(&chunk);
    }

    std::vector<TFuture<void>> importTasks;
    for (const auto& [key, chunks] : importMap) {
        importTasks.push_back(BIND(&TChunkTeleporter::DoImport, MakeStrong(this), key.first, chunks, key.second)
            .AsyncVia(Invoker_)
            .Run());
    }
    WaitFor(AllSucceeded(std::move(importTasks)))
        .ThrowOnError();
}

void TChunkTeleporter::DoImport(
    TCellTag cellTag,
    const std::vector<TChunkEntry*>& chunks,
    TTransactionId destinationTransactionId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    int oldImportedCount = GetImportedObjectCount(cellTag, destinationTransactionId);

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    for (int beginIndex = 0; beginIndex < std::ssize(chunks); beginIndex += Config_->MaxTeleportChunksPerRequest) {
        int endIndex = std::min(
            beginIndex + Config_->MaxTeleportChunksPerRequest,
            std::ssize(chunks));

        auto req = proxy.ImportChunks();
        GenerateMutationId(req);
        ToProto(req->mutable_transaction_id(), destinationTransactionId);
        for (int index = beginIndex; index < endIndex; ++index) {
            req->add_chunks()->Swap(&chunks[index]->Data);
        }

        YT_TLOG_INFO("Importing chunks")
            .With("CellTag", cellTag)
            .With("ChunkCount", req->chunks_size());

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error importing chunks in transaction %v in cell %v",
            destinationTransactionId,
            cellTag);
    }

    int newImportedCount = GetImportedObjectCount(cellTag, destinationTransactionId);
    int expectedImportedCount = oldImportedCount + std::ssize(chunks);
    if (newImportedCount != expectedImportedCount) {
        THROW_ERROR_EXCEPTION("Imported object count mismatch for transaction %v in cell %v: expected %v, got %v",
            destinationTransactionId,
            cellTag,
            expectedImportedCount,
            newImportedCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
