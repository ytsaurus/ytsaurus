#include "schemaless_table_uploader.h"

#include "config.h"
#include "table_ypath_proxy.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/private.h>
#include <yt/yt/client/table_client/table_upload_options.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// These helpers are intentionally shared with the ordinary schemaless writer.
// They stay internal to table_client, but are non-static functions in its
// NDetail namespace so alternate chunk producers use the identical protocol.
namespace NDetail {

TTableSchemaPtr GetChunkSchema(const TRichYPath& richPath, const TTableUploadOptions& options);

void PatchWriterConfigs(
    const TTableWriterOptionsPtr& options,
    const TTableWriterConfigPtr& writerConfig,
    const IAttributeDictionary& attributes,
    const TTableUploadOptions& tableUploadOptions,
    const TTableSchemaPtr& chunkSchema,
    const TTableSchemaPtr& tableSchema,
    const NLogging::TLogger& logger);

INodePtr GetTableAttributes(
    const NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag externalCellTag,
    const TYPath& objectIdPath,
    const TUserObject& userObject);

std::tuple<TMasterTableSchemaId, TTransactionId> BeginTableUpload(
    const NNative::IClientPtr& client,
    TRichYPath path,
    TCellTag nativeCellTag,
    TYPath objectIdPath,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions,
    const TTableSchemaPtr& chunkSchema,
    const NLogging::TLogger& logger,
    bool setUploadTxTimeout);

std::tuple<TLegacyOwningKey, TChunkListId, int> GetTableUploadParams(
    const NNative::IClientPtr& client,
    TRichYPath path,
    TCellTag externalCellTag,
    TYPath objectIdPath,
    TTransactionId uploadTransactionId,
    const TTableUploadOptions& tableUploadOptions,
    const NLogging::TLogger& logger);

void EndTableUpload(
    const NNative::IClientPtr& client,
    const TRichYPath& path,
    TCellTag nativeCellTag,
    TYPath objectIdPath,
    TTransactionId transactionId,
    const TTableUploadOptions& tableUploadOptions,
    NChunkClient::NProto::TDataStatistics dataStatistics);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableUploader::TSchemalessTableUploader(
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    NNative::IClientPtr client,
    TTransactionId transactionId)
    : Options(std::move(options))
    , RichPath_(richPath)
    , Client_(std::move(client))
    , Logger(TableClientLogger()
        .WithTag("Path", richPath.GetPath())
        .WithTag("TransactionId", transactionId))
{
    const auto& path = RichPath_.GetPath();
    UserObject = TUserObject(path);
    GetUserObjectBasicAttributes(Client_, {&UserObject}, transactionId, Logger, EPermission::Write);

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

    auto attributes = NDetail::GetTableAttributes(
        Client_,
        RichPath_,
        externalCellTag,
        objectIdPath,
        UserObject);
    const auto& attributeDictionary = attributes->Attributes();

    if (attributeDictionary.Get<bool>("dynamic")) {
        THROW_ERROR_EXCEPTION("Attaching external data to dynamic tables is not supported");
    }

    TableUploadOptions = GetTableUploadOptions(
        RichPath_,
        attributeDictionary,
        attributeDictionary.Get<TTableSchemaPtr>("schema"),
        attributeDictionary.Get<i64>("row_count"));
    ChunkSchema = NDetail::GetChunkSchema(RichPath_, TableUploadOptions);

    // Attach does not write blocks, but the standard option patching determines
    // the account, medium and chunk properties passed to CreateChunk.
    NDetail::PatchWriterConfigs(
        Options,
        New<TTableWriterConfig>(),
        attributeDictionary,
        TableUploadOptions,
        ChunkSchema,
        GetSchema(),
        Logger);

    auto [chunkSchemaId, uploadTransactionId] = NDetail::BeginTableUpload(
        Client_,
        RichPath_,
        nativeCellTag,
        objectIdPath,
        transactionId,
        TableUploadOptions,
        ChunkSchema,
        Logger,
        /*setUploadTxTimeout*/ true);
    ChunkSchemaId = chunkSchemaId;

    TTransactionAttachOptions attachOptions;
    attachOptions.AutoAbort = true;
    attachOptions.PingPeriod = Client_->GetNativeConnection()->GetConfig()->UploadTransactionPingPeriod;
    UploadTransaction = Client_->AttachTransaction(uploadTransactionId, attachOptions);

    TLegacyOwningKey writerLastKey;
    std::tie(writerLastKey, ChunkListId, Options->MaxHeavyColumns) = NDetail::GetTableUploadParams(
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

void TSchemalessTableUploader::EndUpload(NChunkClient::NProto::TDataStatistics dataStatistics)
{
    NDetail::EndTableUpload(
        Client_,
        RichPath_,
        CellTagFromId(ObjectId_),
        FromObjectId(ObjectId_),
        UploadTransaction ? UploadTransaction->GetId() : NullTransactionId,
        TableUploadOptions,
        std::move(dataStatistics));
    UploadTransaction->Detach();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
