#include "snapshot_writer.h"

#include <yt/yt/server/lib/nbd/journal/records/snapshot_block.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_upload_options.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

const std::vector<std::string>& GetSnapshotTableUploadOptionsAttributeKeys()
{
    static const std::vector<std::string> Result = [] {
        auto result = GetTableUploadOptionsAttributeKeys();
        result.insert(result.end(), {
            "account",
            "primary_medium",
            "replication_factor",
            "vital",
        });
        return result;
    }();
    return Result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void WriteJournalSnapshot(
    const NApi::NNative::IClientPtr& client,
    const TUserObject& userObject,
    TRange<TSnapshotBlock> blocks,
    TRange<TChunkId> hunkChunkIds,
    const NLogging::TLogger& Logger)
{
    if (blocks.empty()) {
        return;
    }

    auto schema = NRecords::TSnapshotBlockDescriptor::Get()->GetSchema();
    const auto& idMapping = NRecords::TSnapshotBlockDescriptor::Get()->GetIdMapping();

    YT_VERIFY(userObject.TransactionId);
    auto transactionId = *userObject.TransactionId;
    auto path = userObject.GetPath();
    auto objectId = userObject.ObjectId;
    auto nativeCellTag = CellTagFromId(objectId);
    auto externalCellTag = userObject.ExternalCellTag;
    auto objectIdPath = FromObjectId(objectId);

    YT_LOG_INFO("Writing snapshot (Path: %v, BlockCount: %v, HunkChunkCount: %v)",
        path,
        blocks.size(),
        hunkChunkIds.size());

    {
        IAttributeDictionaryPtr attributes;
        {
            auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower, nativeCellTag);
            auto req = TCypressYPathProxy::Get(objectIdPath + "/@");
            AddCellTagToSyncWith(req, objectId);
            ToProto(req->mutable_attributes()->mutable_keys(), GetSnapshotTableUploadOptionsAttributeKeys());
            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            attributes = ConvertToAttributes(TYsonString(rsp->value()));
        }
        auto tableUploadOptions = GetTableUploadOptions(
            NYPath::TRichYPath(path),
            *attributes,
            schema,
            /*rowCount*/ 0);

        // Begin upload.
        TTransactionId uploadTransactionId;
        TMasterTableSchemaId chunkSchemaId;
        {
            auto proxy = CreateObjectServiceWriteProxy(client, nativeCellTag);
            auto req = TTableYPathProxy::BeginUpload(objectIdPath);
            req->set_update_mode(ToProto(tableUploadOptions.UpdateMode));
            req->set_lock_mode(ToProto(tableUploadOptions.LockMode));
            ToProto(req->mutable_table_schema(), schema);
            req->set_schema_mode(ToProto(tableUploadOptions.SchemaMode));
            req->set_upload_transaction_title(Format("Writing NBD snapshot to %v", path));
            if (externalCellTag != nativeCellTag) {
                req->add_upload_transaction_secondary_cell_tags(ToProto(externalCellTag));
            }
            req->set_upload_transaction_timeout(
                ToProto(client->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
            SetTransactionId(req, transactionId);
            GenerateMutationId(req);

            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());
        }

        TTransactionAttachOptions attachOptions;
        attachOptions.AutoAbort = true;
        attachOptions.Ping = true;
        attachOptions.PingAncestors = true;
        auto uploadTransaction = client->AttachTransaction(uploadTransactionId, attachOptions);

        // Fetch the main and hunk chunk lists.
        TChunkListId mainChunkListId;
        TChunkListId hunkChunkListId;
        {
            auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower, externalCellTag);
            auto req = TTableYPathProxy::GetUploadParams(objectIdPath);
            req->set_fetch_hunk_chunk_list_id(true);
            SetTransactionId(req, uploadTransactionId);

            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            mainChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            if (!rsp->has_hunk_chunk_list_id()) {
                THROW_ERROR_EXCEPTION("Table %v has no hunk chunk list and cannot hold a journal snapshot",
                    path);
            }
            hunkChunkListId = FromProto<TChunkListId>(rsp->hunk_chunk_list_id());
        }

        auto writerConfig = New<NTableClient::TTableWriterConfig>();

        auto writerOptions = New<NTableClient::TTableWriterOptions>();
        writerOptions->Account = attributes->Get<std::string>("account");
        writerOptions->MediumName = attributes->Get<std::string>("primary_medium");
        writerOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
        writerOptions->ChunksVital = attributes->Get<bool>("vital");
        writerOptions->CompressionCodec = tableUploadOptions.CompressionCodec;
        writerOptions->ErasureCodec = tableUploadOptions.ErasureCodec;
        writerOptions->OptimizeFor = tableUploadOptions.OptimizeFor;
        writerOptions->ChunkFormat = tableUploadOptions.ChunkFormat;

        TDataSink dataSink;
        dataSink.SetPath(path);
        dataSink.SetObjectId(objectId);
        dataSink.SetAccount(writerOptions->Account);

        auto writer = CreateSchemalessMultiChunkWriter(
            writerConfig,
            writerOptions,
            NRecords::TSnapshotBlockDescriptor::Get()->GetNameTable(),
            schema,
            /*lastKey*/ TLegacyOwningKey(),
            client,
            std::string(NNet::GetLocalHostName()),
            externalCellTag,
            uploadTransactionId,
            chunkSchemaId,
            dataSink,
            /*writeBlocksOptions*/ {},
            mainChunkListId);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;
        rows.reserve(blocks.size());
        for (const auto& block : blocks) {
            auto hunkRef = WriteHunkValue(rowBuffer->GetPool(), TGlobalRefHunkValue{
                .ChunkId = block.Ref.ChunkId,
                .ErasureCodec = NErasure::ECodec::None,
                .BlockIndex = block.Ref.RecordIndex,
                .BlockOffset = block.Ref.RecordOffset,
                .Length = block.Ref.PayloadLength,
            });

            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(block.Index, idMapping.BlockIndex));
            builder.AddValue(MakeUnversionedStringValue(hunkRef.ToStringBuf(), idMapping.Payload, EValueFlags::Hunk));
            rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
        }

        WriteRowBatch(writer, rows);
        WaitFor(writer->Close())
            .ThrowOnError();

        auto dataStatistics = writer->GetDataStatistics();

        // Attach the journal (hunk) chunks to the table's hunk chunk list.
        {
            TChunkServiceProxy proxy(client->GetMasterChannelOrThrow(
                EMasterChannelKind::Leader,
                externalCellTag));
            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            SetTransactionId(batchReq, uploadTransactionId);
            SetSuppressUpstreamSync(&batchReq->Header(), true);

            auto hunkReq = batchReq->add_attach_chunk_trees_subrequests();
            ToProto(hunkReq->mutable_parent_id(), hunkChunkListId);
            ToProto(hunkReq->mutable_child_ids(), hunkChunkIds);
            hunkReq->set_request_statistics(true);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error attaching hunk chunks to %v",
                path);
            const auto& subRsp = batchRspOrError.Value()->attach_chunk_trees_subresponses(0);
            dataStatistics += subRsp.statistics();
        }

        // End upload.
        {
            auto proxy = CreateObjectServiceWriteProxy(client, nativeCellTag);
            auto req = TTableYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = dataStatistics;
            if (tableUploadOptions.ChunkFormat) {
                req->set_chunk_format(ToProto(*tableUploadOptions.ChunkFormat));
            }
            req->set_compression_codec(ToProto(tableUploadOptions.CompressionCodec));
            req->set_erasure_codec(ToProto(tableUploadOptions.ErasureCodec));
            req->set_optimize_for(ToProto(tableUploadOptions.OptimizeFor));
            SetTransactionId(req, uploadTransactionId);
            GenerateMutationId(req);
            WaitFor(proxy.Execute(req))
                .ThrowOnError();
        }

        uploadTransaction->Detach();
    }

    YT_LOG_INFO("Snapshot written (Path: %v)", path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
