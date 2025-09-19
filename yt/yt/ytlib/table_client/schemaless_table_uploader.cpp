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

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/ytlib/table_client/table_upload_options.h>
#include <yt/yt/ytlib/table_client/timestamped_schema_helpers.h>

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
    , TransactionId_(transactionId)
    , Logger(TableClientLogger().WithTag("Path: %v, TransactionId: %v", richPath.GetPath(), TransactionId_))
{
    const auto& path = RichPath_.GetPath();

    UserObject = TUserObject(path);

    GetUserObjectBasicAttributes(
        Client_,
        {&UserObject},
        TransactionId_,
        Logger,
        EPermission::Write);

    if (UserObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            UserObject.Type);
    }

    ObjectId_ = UserObject.ObjectId;
    ObjectIdPath_ = FromObjectId(ObjectId_);
    NativeCellTag_ = CellTagFromId(ObjectId_);
    ExternalCellTag_ = UserObject.ExternalCellTag;

    {
        YT_LOG_DEBUG("Requesting extended table attributes");

        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            ExternalCellTag_);

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
        }();

        auto req = TCypressYPathProxy::Get(ObjectIdPath_);
        AddCellTagToSyncWith(req, ObjectId_);
        NCypressClient::SetTransactionId(req, UserObject.ExternalTransactionId);
        ToProto(req->mutable_attributes()->mutable_keys(), AttributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        Attributes = ConvertToNode(TYsonString(rsp->value()));

        const auto& attributes = Attributes->Attributes();

        if (attributes.Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Uploading data to dynamic tables is not supported via this API; use `insert_rows` instead");
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
}

void TSchemalessTableUploader::BeginUpload()
{
    const auto& path = RichPath_.GetPath();

    {
        YT_LOG_DEBUG("Starting table upload");
        auto proxy = NObjectClient::CreateObjectServiceWriteProxy(Client_, NativeCellTag_);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TTableYPathProxy::BeginUpload(ObjectIdPath_);
            ToProto(req->mutable_table_schema(), TableUploadOptions.TableSchema.Get());
            // Only time this can be true is when RichPath_ has extra chunk sort columns.
            if (ChunkSchema != TableUploadOptions.TableSchema.Get()) {
                auto checkResult = CheckTableSchemaCompatibility(
                    *ChunkSchema,
                    *TableUploadOptions.TableSchema.Get(),
                    {.AllowTimestampColumns = TableUploadOptions.VersionedWriteOptions.WriteMode == EVersionedIOMode::LatestTimestamp});

                if (!checkResult.second.IsOK()) {
                    YT_LOG_FATAL(
                        checkResult.second,
                        "Chunk schema is incompatible with a table schema (ChunkSchema: %v, TableSchema: %v)",
                        *ChunkSchema,
                        *TableUploadOptions.TableSchema.Get());
                }
                ToProto(req->mutable_chunk_schema(), ChunkSchema);
            }
            req->set_schema_mode(static_cast<int>(TableUploadOptions.SchemaMode));
            req->set_optimize_for(static_cast<int>(TableUploadOptions.OptimizeFor));
            req->set_update_mode(static_cast<int>(TableUploadOptions.UpdateMode));
            req->set_lock_mode(static_cast<int>(TableUploadOptions.LockMode));
            req->set_upload_transaction_title(Format("Upload to %v", path));
            req->set_upload_transaction_timeout(ToProto<i64>(Client_->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
            NCypressClient::SetTransactionId(req, TransactionId_);
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
        auto UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        auto chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());

        YT_LOG_DEBUG("Table upload started (UploadTransactionId: %v)",
            UploadTransactionId);

        ChunkSchemaId = chunkSchemaId;

        UploadTransaction = Client_->AttachTransaction(UploadTransactionId, TTransactionAttachOptions{
            .AutoAbort = true,
        });
    }

    {
        YT_LOG_DEBUG("Requesting table upload parameters");

        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            ExternalCellTag_);

        auto req = TTableYPathProxy::GetUploadParams(ObjectIdPath_);
        req->set_fetch_last_key(
            TableUploadOptions.UpdateMode == EUpdateMode::Append &&
            TableUploadOptions.TableSchema->IsSorted());
        SetTransactionId(req, UploadTransaction->GetId());

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting upload parameters for table %v",
            path);

        const auto& rsp = rspOrError.Value();
        ChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        if (auto lastKey = FromProto<TLegacyOwningKey>(rsp->last_key())) {
            WriterLastKey = TLegacyOwningKey(lastKey.FirstNElements(TableUploadOptions.TableSchema->GetKeyColumnCount()));
        }

        Options_->MaxHeavyColumns = rsp->max_heavy_columns();

        YT_LOG_DEBUG("Table upload parameters received (ChunkListId: %v, HasLastKey: %v, MaxHeavyColumns: %v)",
            ChunkListId,
            static_cast<bool>(WriterLastKey),
            Options_->MaxHeavyColumns);
    }
}

const TTableSchemaPtr& TSchemalessTableUploader::GetSchema() const
{
    return TableUploadOptions.TableSchema.Get();
}

TTableSchemaPtr TSchemalessTableUploader::GetChunkSchema() const
{
    auto chunkSchema = GetSchema();

    bool tableUniqueKeys = chunkSchema->IsUniqueKeys();
    auto chunkUniqueKeys = RichPath_.GetChunkUniqueKeys();
    if (chunkUniqueKeys) {
        if (!*chunkUniqueKeys && tableUniqueKeys) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Table schema forces keys to be unique while chunk schema does not");
        }

        chunkSchema = chunkSchema->SetUniqueKeys(*chunkUniqueKeys);
    }

    auto chunkSortColumns = RichPath_.GetChunkSortColumns();
    if (chunkSortColumns) {
        auto tableSchemaSortColumns = GetSchema()->GetSortColumns();
        if (chunkSortColumns->size() < tableSchemaSortColumns.size()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Chunk sort columns list is shorter than table schema sort columns")
                << TErrorAttribute("chunk_sort_columns_count", chunkSortColumns->size())
                << TErrorAttribute("table_sort_column_count", tableSchemaSortColumns.size());
        }

        if (tableUniqueKeys && !tableSchemaSortColumns.empty()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Chunk sort columns cannot be set when table is sorted with unique keys");
        }

        for (int columnIndex = 0; columnIndex < std::ssize(tableSchemaSortColumns); ++columnIndex) {
            if ((*chunkSortColumns)[columnIndex] != tableSchemaSortColumns[columnIndex]) {
                THROW_ERROR_EXCEPTION(EErrorCode::IncompatibleKeyColumns,
                    "Incompatible sort columns: chunk sort columns %v, table sort columns %v",
                    chunkSortColumns,
                    tableSchemaSortColumns);
            }
        }

        chunkSchema = chunkSchema->ToSorted(*chunkSortColumns);
    }

    if (chunkSchema->IsUniqueKeys() && !chunkSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::InvalidSchemaValue,
            "Non-sorted schema can't have unique keys requirement");
    }

    return chunkSchema;
}

void TSchemalessTableUploader::EndUpload(TTableYPathProxy::TReqEndUploadPtr req)
{
    auto proxy = CreateObjectServiceWriteProxy(
        Client_,
        CellTagFromId(ObjectId_));
    auto batchReq = proxy.ExecuteBatch();

    {
        if (TableUploadOptions.ChunkFormat) {
            req->set_chunk_format(ToProto<int>(*TableUploadOptions.ChunkFormat));
        }
        req->set_compression_codec(ToProto<int>(TableUploadOptions.CompressionCodec));
        req->set_erasure_codec(ToProto<int>(TableUploadOptions.ErasureCodec));
        req->set_optimize_for(ToProto<int>(TableUploadOptions.OptimizeFor));

        // COMPAT(h0pless): remove this when all masters are 24.2.
        req->set_schema_mode(ToProto<int>(TableUploadOptions.SchemaMode));

        if (TableUploadOptions.SecurityTags) {
            ToProto(req->mutable_security_tags()->mutable_items(), *TableUploadOptions.SecurityTags);
        }

        SetTransactionId(req, UploadTransaction->GetId());
        GenerateMutationId(req);
        batchReq->AddRequest(req, "end_upload");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error finishing upload to table %v",
        RichPath_.GetPath());

    UploadTransaction->Detach();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
