#include "attach_table.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "schemaless_table_uploader.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/external_parquet.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/chunk_client/helpers.h>
#include <yt/yt/client/node_tracker_client/public.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/library/s3/client.h>
#include <yt/yt/library/s3/credential_provider.h>
#include <yt/yt/library/s3/object.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

const THashSet<int>& GetExternalParquetMasterChunkMetaExtensionTagsFilter()
{
    static const THashSet<int> Result{
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NChunkClient::NProto::TBlocksExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TDataBlockMetaExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TParquetFormatMetaExt>::Value,
    };
    return Result;
}

class TTableAttacher
    : public TTransactionListener
{
public:
    TTableAttacher(
        TRichYPath richPath,
        std::vector<std::string> sourceUris,
        NApi::TAttachTableOptions options,
        NApi::NNative::IClientPtr client,
        NApi::ITransactionPtr transaction)
        : RichPath_(std::move(richPath))
        , SourceUris_(std::move(sourceUris))
        , AttachOptions_(std::move(options))
        , Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , TransactionId_(Transaction_ ? Transaction_->GetId() : NullTransactionId)
        , Logger(TableClientLogger()
            .WithTag("Path", RichPath_.GetPath())
            .WithTag("TransactionId", TransactionId_))
    {
        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }
    }

    ~TTableAttacher()
    {
        // A live upload transaction holds all chunks created during attach. Abort
        // it on failure; Abort is harmless after EndUpload detached it.
        if (Uploader_ && Uploader_->UploadTransaction) {
            Y_UNUSED(WaitFor(Uploader_->UploadTransaction->Abort()));
        }
    }

    TFuture<void> Run()
    {
        return BIND(&TTableAttacher::DoRun, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const TRichYPath RichPath_;
    const std::vector<std::string> SourceUris_;
    const NApi::TAttachTableOptions AttachOptions_;
    const NApi::NNative::IClientPtr Client_;
    const NApi::ITransactionPtr Transaction_;
    const TTransactionId TransactionId_;
    const NLogging::TLogger Logger;

    std::optional<TSchemalessTableUploader> Uploader_;
    TS3MediumDescriptorPtr S3MediumDescriptor_;
    NS3::IClientPtr S3Client_;
    int ChunkCount_ = 0;
    i64 RowCount_ = 0;
    i64 DataSize_ = 0;

    NS3::IClientPtr CreateS3Client() const
    {
        const auto& mediumConfig = S3MediumDescriptor_->GetConfig();
        auto credentials = NS3::CreateStaticCredentialProvider(
            mediumConfig->AccessKeyId,
            mediumConfig->SecretAccessKey);

        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = mediumConfig->Url;
        clientConfig->Region = mediumConfig->Region;

        auto client = NS3::CreateClient(
            std::move(clientConfig),
            std::move(credentials),
            /*sslContextConfig*/ nullptr,
            NBus::NTcp::TDispatcher::Get()->GetXferPoller(),
            TDispatcher::Get()->GetWriterInvoker());
        WaitFor(client->Start())
            .ThrowOnError();
        return client;
    }

    void Open()
    {
        THROW_ERROR_EXCEPTION_IF(SourceUris_.empty(), "Cannot attach an empty source URI list");
        ValidateAborted();

        Uploader_.emplace(New<TTableWriterOptions>(), RichPath_, Client_, TransactionId_);
        StartListenTransaction(Uploader_->UploadTransaction);

        THROW_ERROR_EXCEPTION_IF(
            Uploader_->GetSchema()->IsSorted(),
            "Attaching external data to sorted tables is not supported");
        THROW_ERROR_EXCEPTION_IF(
            Uploader_->Options->ErasureCodec != NErasure::ECodec::None,
            "Cannot attach external data to a table with erasure codec %Qlv",
            Uploader_->Options->ErasureCodec);
        THROW_ERROR_EXCEPTION_IF(
            AttachOptions_.Medium && *AttachOptions_.Medium != Uploader_->Options->MediumName,
            "Requested medium %Qv does not match the table medium %Qv",
            *AttachOptions_.Medium,
            Uploader_->Options->MediumName);

        const auto& connection = Client_->GetNativeConnection();
        WaitFor(connection->GetMediumDirectorySynchronizer()->NextSync(/*force*/ true))
            .ThrowOnError();
        auto medium = connection->GetMediumDirectory()->GetByNameOrThrow(Uploader_->Options->MediumName);
        THROW_ERROR_EXCEPTION_IF(
            !medium->IsOffshore(),
            "Cannot attach external data to a table on non-S3 medium %Qv",
            Uploader_->Options->MediumName);
        S3MediumDescriptor_ = medium->As<TS3MediumDescriptor>();

        S3Client_ = CreateS3Client();

        YT_LOG_DEBUG(
            "Table opened for attaching external data (UploadTransactionId: %v, Medium: %v)",
            Uploader_->UploadTransaction->GetId(),
            Uploader_->Options->MediumName);
    }

    void AttachSource(const std::string& sourceUri)
    {
        YT_VERIFY(Uploader_);
        ValidateAborted();

        const auto source = NS3::TObjectDescriptor::FromUri(sourceUri);
        const auto sourceFormat = AttachOptions_.SourceFormat
            ? *AttachOptions_.SourceFormat
            : DeduceExternalSourceFormatOrThrow(source.Key());
        const auto chunkFormat = GetChunkFormatFromExternalSourceFormat(sourceFormat);

        auto generator = CreateArrowTableChunkMetaGenerator(
            chunkFormat,
            std::make_shared<TS3ArrowRandomAccessFile>(source, S3Client_));
        generator->Generate();

        auto [compatibility, compatibilityError] = CheckTableSchemaCompatibility(
            *generator->GetChunkSchema(),
            *Uploader_->ChunkSchema,
            TTableSchemaCompatibilityOptions{});
        if (compatibility != ESchemaCompatibility::FullyCompatible && !AttachOptions_.AllowIncompatibleSourceSchemas) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::IncompatibleSchemas,
                "Inferred schema for source %Qv is not compatible with the table schema",
                sourceUri)
                << compatibilityError;
        }

        auto sessionId = CreateChunk(
            Client_,
            Uploader_->UserObject.ExternalCellTag,
            Uploader_->Options,
            Uploader_->UploadTransaction->GetId(),
            Uploader_->ChunkListId,
            Logger);

        auto replica = TChunkReplicaWithMedium(
            OffshoreNodeId,
            GenericChunkReplicaIndex,
            sessionId.MediumIndex,
            sourceUri);

        auto channel = Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            Uploader_->UserObject.ExternalCellTag);
        TChunkServiceProxy proxy(channel);
        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);

        ToProto(req->mutable_chunk_id(), sessionId.ChunkId);
        *req->mutable_chunk_info() = NChunkClient::NProto::TChunkInfo();
        *req->mutable_chunk_meta() = *generator->GetChunkMeta();

        auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
            Uploader_->Options->MemoryUsageTracker,
            req->mutable_chunk_meta()->ByteSize());
        FilterProtoExtensions(
            req->mutable_chunk_meta()->mutable_extensions(),
            GetExternalParquetMasterChunkMetaExtensionTagsFilter());
        req->set_request_statistics(true);
        req->set_location_uuids_supported(true);

        auto* replicaInfo = req->add_replicas();
        ToProto(replicaInfo->mutable_replica_spec(), replica);
        ToProto(replicaInfo->mutable_location_uuid(), InvalidChunkLocationUuid);
        ui64 encodedReplica;
        ToProto(&encodedReplica, replica);
        replicaInfo->set_replica(encodedReplica);

        if (Uploader_->ChunkSchemaId != NullTableSchemaId) {
            ToProto(req->mutable_schema_id(), Uploader_->ChunkSchemaId);
        }

        auto* multicellSyncExt = req->Header().MutableExtension(
            NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        multicellSyncExt->set_suppress_upstream_sync(true);

        auto responseOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            responseOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to confirm attached chunk %v",
            sessionId.ChunkId);

        ++ChunkCount_;
        RowCount_ += generator->GetRowCount();
        DataSize_ += generator->GetUncompressedDataSize();

        YT_LOG_DEBUG(
            "Attached external Parquet chunk (ChunkId: %v, SourceUri: %v, RowCount: %v)",
            sessionId.ChunkId,
            sourceUri,
            generator->GetRowCount());
    }

    void Close()
    {
        YT_VERIFY(Uploader_);
        ValidateAborted();

        StopListenTransaction(Uploader_->UploadTransaction);

        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(ChunkCount_);
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_compressed_data_size(DataSize_);
        dataStatistics.set_uncompressed_data_size(DataSize_);
        dataStatistics.set_data_weight(DataSize_);
        Uploader_->EndUpload(dataStatistics);

        YT_LOG_DEBUG("Table closed after attaching external data (DataStatistics: %v)", dataStatistics);
    }

    void DoRun()
    {
        Open();
        for (const auto& sourceUri : SourceUris_) {
            AttachSource(sourceUri);
        }
        Close();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AttachTable(
    const TRichYPath& richPath,
    std::vector<std::string> sourceUris,
    const NApi::TAttachTableOptions& options,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction)
{
    return New<TTableAttacher>(
        richPath,
        std::move(sourceUris),
        options,
        std::move(client),
        std::move(transaction))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
