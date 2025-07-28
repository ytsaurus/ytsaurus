#include "schemaless_table_uploader.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "helpers.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "skynet_column_evaluator.h"
#include "table_ypath_proxy.h"
#include "versioned_chunk_writer.h"

#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>
#include <yt/yt/ytlib/table_chunk_format/schemaless_column_writer.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_upload_options.h>
#include <yt/yt/client/table_client/timestamped_schema_helpers.h>

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/random.h>

#include <util/generic/cast.h>
#include <util/generic/ylimits.h>

#include <utility>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableChunkFormat;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::TRange;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TTableSchemaPtr GetChunkSchema(
    const TRichYPath& richPath,
    const TTableUploadOptions& options)
{
    auto chunkSchema = options.TableSchema.Get();

    bool tableUniqueKeys = chunkSchema->IsUniqueKeys();
    auto chunkUniqueKeys = richPath.GetChunkUniqueKeys();
    if (chunkUniqueKeys) {
        if (!*chunkUniqueKeys && tableUniqueKeys) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
                "Table schema forces keys to be unique while chunk schema does not");
        }

        chunkSchema = chunkSchema->SetUniqueKeys(*chunkUniqueKeys);
    }

    auto chunkSortColumns = richPath.GetChunkSortColumns();
    if (chunkSortColumns) {
        auto tableSchemaSortColumns = chunkSchema->GetSortColumns();
        if (chunkSortColumns->size() < tableSchemaSortColumns.size()) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
                "Chunk sort columns list is shorter than table schema sort columns")
                << TErrorAttribute("chunk_sort_columns_count", chunkSortColumns->size())
                << TErrorAttribute("table_sort_column_count", tableSchemaSortColumns.size());
        }

        if (tableUniqueKeys && !tableSchemaSortColumns.empty()) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
                "Chunk sort columns cannot be set when table is sorted with unique keys");
        }

        for (int columnIndex = 0; columnIndex < std::ssize(tableSchemaSortColumns); ++columnIndex) {
            if ((*chunkSortColumns)[columnIndex] != tableSchemaSortColumns[columnIndex]) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::IncompatibleKeyColumns,
                    "Incompatible sort columns: chunk sort columns %v, table sort columns %v",
                    chunkSortColumns,
                    tableSchemaSortColumns);
            }
        }

        chunkSchema = chunkSchema->ToSorted(*chunkSortColumns);
    }

    if (chunkSchema->IsUniqueKeys() && !chunkSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::InvalidSchemaValue,
            "Non-sorted schema can't have unique keys requirement");
    }

    return chunkSchema;
}

void PatchWriterOptions(
    const TTableWriterOptionsPtr& options,
    const IAttributeDictionary& attributes,
    const TTableUploadOptions& tableUploadOptions,
    const TTableSchemaPtr& chunkSchema,
    const TTableSchemaPtr& tableSchema,
    const NLogging::TLogger& Logger)
{
    options->ReplicationFactor = attributes.Get<int>("replication_factor");
    options->MediumName = attributes.Get<TString>("primary_medium");
    options->CompressionCodec = tableUploadOptions.CompressionCodec;
    options->ErasureCodec = tableUploadOptions.ErasureCodec;
    options->EnableStripedErasure = tableUploadOptions.EnableStripedErasure;
    options->Account = attributes.Get<TString>("account");
    options->ChunksVital = attributes.Get<bool>("vital");
    options->EnableSkynetSharing = attributes.Get<bool>("enable_skynet_sharing", false);

    // Table's schema is never stricter than chunk's schema.
    options->ValidateSorted = chunkSchema->IsSorted();
    options->ValidateUniqueKeys = chunkSchema->IsUniqueKeys();

    options->OptimizeFor = tableUploadOptions.OptimizeFor;
    options->ChunkFormat = tableUploadOptions.ChunkFormat;
    options->EvaluateComputedColumns = tableUploadOptions.TableSchema->HasMaterializedComputedColumns();
    options->TableSchema = tableSchema;
    options->VersionedWriteOptions = tableUploadOptions.VersionedWriteOptions;

    YT_LOG_DEBUG("Table upload options generated, table writer options and config patched "
        "(Account: %v, CompressionCodec: %v, ErasureCodec: %v, EnableStripedErasure: %v, EnableSkynetSharing: %v)",
        options->Account,
        options->CompressionCodec,
        options->ErasureCodec,
        options->EnableStripedErasure,
        options->EnableSkynetSharing);
}

////////////////////////////////////////////////////////////////////////////////

INodePtr GetTableAttributes(
    const NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag externalCellTag,
    const NYPath::TYPath& objectIdPath,
    const TUserObject& userObject)
{
    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower,
        externalCellTag);

    static const auto AttributeKeys = [] {
        return ConcatVectors(
            GetTableUploadOptionsAttributeKeys(),
            std::vector<TString>{
                "account",
                "chunk_writer",
                "primary_medium",
                "replication_factor",
                "row_count",
                "schema",
                "vital",
                "enable_skynet_sharing"
            });
    } ();

    auto req = TCypressYPathProxy::Get(objectIdPath);
    AddCellTagToSyncWith(req, userObject.ObjectId);
    NCypressClient::SetTransactionId(req, userObject.ExternalTransactionId);
    // TODO(danilalexeev): Figure out why request ignores the Sequoia resolve.
    NCypressClient::SetAllowResolveFromSequoiaObject(req, true);
    ToProto(req->mutable_attributes()->mutable_keys(), AttributeKeys);

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        "Error requesting extended attributes of table %v",
        path);

    const auto& rsp = rspOrError.Value();
    return ConvertToNode(TYsonString(rsp->value()));
}

std::tuple<TMasterTableSchemaId, TTransactionId> BeginTableUpload(
    const NNative::IClientPtr& client,
    const TRichYPath path,
    TCellTag nativeCellTag,
    NYPath::TYPath objectIdPath,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions,
    const TTableSchemaPtr& chunkSchema,
    const NLogging::TLogger& Logger,
    bool setUploadTxTimeout)
{
    auto proxy = NObjectClient::CreateObjectServiceWriteProxy(client, nativeCellTag);
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TTableYPathProxy::BeginUpload(objectIdPath);
        ToProto(req->mutable_table_schema(), tableUploadOptions.TableSchema.Get());
        // Only time this can be true is when RichPath_ has extra chunk sort columns.
        if (chunkSchema != tableUploadOptions.TableSchema.Get()) {
            auto checkResult = CheckTableSchemaCompatibility(
                *chunkSchema,
                *tableUploadOptions.TableSchema.Get(),
                {.AllowTimestampColumns = tableUploadOptions.VersionedWriteOptions.WriteMode == EVersionedIOMode::LatestTimestamp});

            if (!checkResult.second.IsOK()) {
                YT_LOG_FATAL(
                    checkResult.second,
                    "Chunk schema is incompatible with a table schema (ChunkSchema: %v, TableSchema: %v)",
                    *chunkSchema,
                    *tableUploadOptions.TableSchema.Get());
            }
            ToProto(req->mutable_chunk_schema(), chunkSchema);
        }
        req->set_schema_mode(ToProto(tableUploadOptions.SchemaMode));
        req->set_optimize_for(ToProto(tableUploadOptions.OptimizeFor));
        req->set_update_mode(ToProto(tableUploadOptions.UpdateMode));
        req->set_lock_mode(ToProto(tableUploadOptions.LockMode));
        req->set_upload_transaction_title(Format("Upload to %v", path));
        if (setUploadTxTimeout) {
            req->set_upload_transaction_timeout(ToProto(client->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
        }
        NCypressClient::SetTransactionId(req, transactionId);
        GenerateMutationId(req);
        batchReq->AddRequest(req, "begin_upload");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error starting upload to table %v",
        path);
    const auto& batchRsp = batchRspOrError.Value();

    auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspBeginUpload>("begin_upload").Value();
    auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
    auto chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());

    YT_LOG_DEBUG("Table upload started (UploadTransactionId: %v)",
        uploadTransactionId);

    return std::tuple(chunkSchemaId, uploadTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

std::tuple<TLegacyOwningKey, TChunkListId, int> GetTableUploadParams(
    const NNative::IClientPtr& client,
    const TRichYPath path,
    TCellTag externalCellTag,
    NYPath::TYPath objectIdPath,
    TTransactionId uploadTxId,
    const TTableUploadOptions& tableUploadOptions,
    const NLogging::TLogger& Logger)
{
    TLegacyOwningKey writerLastKey;
    TChunkListId chunkListId;
    int maxColumnCount;

    YT_LOG_DEBUG("Requesting table upload parameters");

    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower,
        externalCellTag);

    auto req =  TTableYPathProxy::GetUploadParams(objectIdPath);
    req->set_fetch_last_key(
        tableUploadOptions.UpdateMode == EUpdateMode::Append &&
        tableUploadOptions.TableSchema->IsSorted());
    SetTransactionId(req, uploadTxId);

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        "Error requesting upload parameters for table %v",
        path);

    const auto& rsp = rspOrError.Value();
    chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
    if (auto lastKey = FromProto<TLegacyOwningKey>(rsp->last_key())) {
        writerLastKey = TLegacyOwningKey(lastKey.FirstNElements(tableUploadOptions.TableSchema->GetKeyColumnCount()));
    }

    maxColumnCount = rsp->max_heavy_columns();

    YT_LOG_DEBUG("Table upload parameters received (ChunkListId: %v, HasLastKey: %v, MaxHeavyColumns: %v)",
        chunkListId,
        static_cast<bool>(writerLastKey),
        maxColumnCount);

    return std::tuple(std::move(writerLastKey), chunkListId, maxColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

void EndTableUpload(
    const NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag nativeCellTag,
    TTableYPathProxy::TReqEndUploadPtr req,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions)
{
    auto proxy = CreateObjectServiceWriteProxy(
        client,
        nativeCellTag);
    auto batchReq = proxy.ExecuteBatch();

    {
        if (tableUploadOptions.ChunkFormat) {
            req->set_chunk_format(ToProto(*tableUploadOptions.ChunkFormat));
        }
        req->set_compression_codec(ToProto(tableUploadOptions.CompressionCodec));
        req->set_erasure_codec(ToProto(tableUploadOptions.ErasureCodec));
        req->set_optimize_for(ToProto(tableUploadOptions.OptimizeFor));

        // COMPAT(h0pless): remove this when all masters are 24.2.
        req->set_schema_mode(ToProto(tableUploadOptions.SchemaMode));

        if (tableUploadOptions.SecurityTags) {
            ToProto(req->mutable_security_tags()->mutable_items(), *tableUploadOptions.SecurityTags);
        }

        SetTransactionId(req, transactionId);
        GenerateMutationId(req);
        batchReq->AddRequest(req, "end_upload");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error finishing upload to table %v",
        path);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableUploader::TSchemalessTableUploader(
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    NNative::IClientPtr client,
    TTransactionId transactionId)
    : Options_(std::move(options))
    , RichPath_(richPath)
    , Client_(std::move(client))
    , Logger(TableClientLogger().WithTag("Path: %v, TransactionId: %v", richPath.GetPath(), transactionId))
{
    const auto& path = RichPath_.GetPath();

    UserObject = TUserObject(path);

    GetUserObjectBasicAttributes(
        Client_,
        {&UserObject},
        transactionId,
        Logger,
        EPermission::Write);

    if (UserObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            UserObject.Type);
    }

    ObjectId_ = UserObject.ObjectId;
    auto nativeCellTag = CellTagFromId(ObjectId_);
    auto externalCellTag = UserObject.ExternalCellTag;
    auto objectIdPath = FromObjectId(ObjectId_);

    {
        YT_LOG_DEBUG("Requesting extended table attributes");

        Attributes = NDetail::GetTableAttributes(
            Client_,
            path,
            externalCellTag,
            objectIdPath,
            UserObject);

        const auto& attributes = Attributes->Attributes();

        if (attributes.Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("\"write_table\" API is not supported for dynamic tables; use \"insert_rows\" instead");
        }

        TableUploadOptions = GetTableUploadOptions(
            RichPath_,
            attributes,
            attributes.Get<TTableSchemaPtr>("schema"),
            attributes.Get<i64>("row_count"));

        ChunkSchema = GetChunkSchema();

        NDetail::PatchWriterOptions(
            Options_,
            attributes,
            TableUploadOptions,
            ChunkSchema,
            GetSchema(),
            Logger);
    }

    auto [chunkSchemaId, UploadTransactionId] = NDetail::BeginTableUpload(
        Client_,
        path,
        nativeCellTag,
        objectIdPath,
        transactionId,
        TableUploadOptions,
        ChunkSchema,
        Logger,
        /*setUploadTxTimeout*/ true);
    ChunkSchemaId = chunkSchemaId;

    UploadTransaction = Client_->AttachTransaction(UploadTransactionId, TTransactionAttachOptions{
        .AutoAbort = true,
        .PingPeriod = Client_->GetNativeConnection()->GetConfig()->UploadTransactionPingPeriod,
    });

    std::tie(WriterLastKey, ChunkListId, Options_->MaxHeavyColumns)
        = NDetail::GetTableUploadParams(
            Client_,
            path,
            externalCellTag,
            objectIdPath,
            UploadTransaction->GetId(),
            TableUploadOptions,
            Logger);
}

const TTableSchemaPtr& TSchemalessTableUploader::GetSchema() const
{
    return TableUploadOptions.TableSchema.Get();
}

TTableSchemaPtr TSchemalessTableUploader::GetChunkSchema() const
{
    return NDetail::GetChunkSchema(RichPath_, TableUploadOptions);
}

void TSchemalessTableUploader::EndUpload(TTableYPathProxy::TReqEndUploadPtr endUpload)
{
    YT_LOG_DEBUG("Closing table");

    NDetail::EndTableUpload(
        Client_,
        RichPath_.GetPath(),
        CellTagFromId(ObjectId_),
        endUpload,
        UploadTransaction->GetId(),
        TableUploadOptions);

    UploadTransaction->Detach();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
