#include "attach_table.h"
#include "schemaless_table_uploader.h"
#include "schemaless_block_writer.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_generator.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/s3_common.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/library/s3/object.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;

class TTableAttacher
    : public TTransactionListener
{
public:
    TTableAttacher(
        const TRichYPath& richPath,
        NNative::IClientPtr client,
        ITransactionPtr transaction,
        std::vector<std::string> sourceUris)
        : Options_(New<NTableClient::TTableWriterOptions>())
        , RichPath_(richPath)
        , Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , TransactionId_(Transaction_ ? Transaction_->GetId() : NullTransactionId)
        , SourceUris_(std::move(sourceUris))
        , Logger(TableClientLogger().WithTag("Path: %v, TransactionId: %v",
            richPath.GetPath(),
            TransactionId_))
    {
        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }
    }

    ~TTableAttacher()
    {
        // It is possible for a transatction with AutoAbort = true to survive beyond the lifetime
        // of this object via the strong ref in a Ping request. Thus it is best to abort the
        // transaction in case of errors.
        // It is fine to abort the transaction unconditionally, since if we are done with it,
        // it would be in Detached state and abort is no-op and just returns an error.
        if (Uploader_ && Uploader_->UploadTransaction) {
            Y_UNUSED(WaitFor(Uploader_->UploadTransaction->Abort()));
        }
    }

    TFuture<void> Run()
    {
        return BIND(&TTableAttacher::DoRun, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const NNative::IClientPtr Client_;
    const ITransactionPtr Transaction_;
    const TTransactionId TransactionId_;
    const std::vector<std::string> SourceUris_;

    const NLogging::TLogger Logger;

    std::optional<TSchemalessTableUploader> Uploader_;
    int ChunkCount_ = 0;
    i64 DataSize_ = 0;
    i64 RowCount_ = 0;

    void DoOpen()
    {
        ValidateAborted();

        Uploader_.emplace(Options_, RichPath_, Client_, TransactionId_);
        StartListenTransaction(Uploader_->UploadTransaction);

        if (Options_->ErasureCodec != NErasure::ECodec::None) {
            THROW_ERROR_EXCEPTION("Cannot attach external data to table with erasure codec %Qlv", Options_->ErasureCodec);
        }

        if (!Client_->GetNativeConnection()->GetMediumDirectory()->GetByNameOrThrow(Options_->MediumName)->As<NYT::NChunkClient::TS3MediumDescriptor>()) {
            THROW_ERROR_EXCEPTION("Cannot attach external data to table on non-offshore medium %Qv", Options_->MediumName);
        }

        YT_LOG_DEBUG(
            "Table opened for attaching external data (UploadTransactionId: %v)",
            Uploader_->UploadTransaction->GetId());
    }

    void AttachChunk(const std::string& sourceUri)
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        // TODO(achulkov2): We can move path-related validation to a step before we actually call BeginUpload, so that
        // we don't start an unnecessary transaction if we are going to throw an error anyway.

        // TODO(achulkov2): We might want to do something else once we support multiple different schemas, not just S3.
        // Probably it should go into a separate method that lives somewhere near chunk meta generators and dispatches
        // to the correct one from there.
        auto sourceS3Descriptor = NS3::TObjectDescriptor::FromUri(sourceUri);

        // TODO(achulkov2): Add constants for extensions.
        EChunkFormat chunkFormat;
        if (sourceS3Descriptor.Key().EndsWith(".parquet")) {
            chunkFormat = EChunkFormat::TableUnversionedArrowParquet;
        } else if (sourceS3Descriptor.Key().EndsWith(".jsonl")) {
            chunkFormat = EChunkFormat::TableUnversionedArrowJsonLines;
        } else if (sourceS3Descriptor.Key().EndsWith(".csv")) {
            chunkFormat = EChunkFormat::TableUnversionedArrowCsv;
        } else {
            THROW_ERROR_EXCEPTION("Cannot attach file %Qv with unsupported extension; only .parquet, .jsonl and .csv files are supported", sourceS3Descriptor);
        }

        YT_LOG_DEBUG("Attaching chunk to table (SourceUri: %v, ChunkFormat: %lv)",
            sourceUri,
            chunkFormat);

        auto sessionId = NChunkClient::CreateChunk(
            Client_,
            Uploader_->UserObject.ExternalCellTag,
            Options_,
            Uploader_->UploadTransaction->GetId(),
            Uploader_->ChunkListId,
            Logger);

        const auto& mediumDirectory = Client_->GetNativeConnection()->GetMediumDirectory();

        // TODO(achulkov2, faucct): We are always attaching to a single medium, so we could cache this medium descriptor.
        // Also we can check it against Options_->MediumName.
        auto mediumDescriptor = mediumDirectory->FindByIndex(sessionId.MediumIndex);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION(
                "Medium with index %v not found in medium directory; please contact cluster administrators if the problem persists",
                sessionId.MediumIndex);
        }
        YT_VERIFY(mediumDescriptor);

        auto s3MediumDescriptor = mediumDescriptor->As<NYT::NChunkClient::TS3MediumDescriptor>();
        if (!s3MediumDescriptor) {
            THROW_ERROR_EXCEPTION("Cannot attach external data to table on non-offshore medium %Qv", Options_->MediumName);
        }

        // NB: There is a context switch inside the constructor for TS3ArrowRandomAccessFile.
        auto chunkMetaGenerator = CreateArrowTableChunkMetaGenerator(
            chunkFormat,
            std::make_shared<TS3ArrowRandomAccessFile>(
                sourceS3Descriptor,
                s3MediumDescriptor->GetClient()));

        // NB: Possible context switch.
        chunkMetaGenerator->Generate();

        YT_LOG_DEBUG("Inferred schema for source (SourceUri: %v, InferredSchema: %v)",
            sourceUri,
            *chunkMetaGenerator->GetChunkSchema());

        auto [schemaCompatibility, schemaCompatibilityError] = CheckTableSchemaCompatibility(
            *chunkMetaGenerator->GetChunkSchema(),
            *Uploader_->ChunkSchema,
            TTableSchemaCompatibilityOptions{});

        if (schemaCompatibility != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION("Inferred schema for source %Qv is not compatible with table schema", sourceUri)
                << schemaCompatibilityError;
        }

        ChunkCount_++;
        RowCount_ += chunkMetaGenerator->GetRowCount();
        DataSize_ += chunkMetaGenerator->GetUncompressedSize();

        YT_LOG_DEBUG("Confirming attached chunk (SourceUri: %v, ChunkId: %v, ChunkFormat: %lv, RowCount: %v, UncompressedDataSize: %v)",
            sourceUri,
            sessionId.ChunkId,
            chunkFormat,
            chunkMetaGenerator->GetRowCount(),
            chunkMetaGenerator->GetUncompressedSize());

        auto replica = TChunkReplicaWithMedium(
            OffshoreNodeId,
            GenericChunkReplicaIndex,
            sessionId.MediumIndex,
            sourceUri);

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, Uploader_->UserObject.ExternalCellTag);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);
        {
            ToProto(req->mutable_chunk_id(), sessionId.ChunkId);
            *req->mutable_chunk_info() = TChunkInfo();
            *req->mutable_chunk_meta() = *chunkMetaGenerator->GetChunkMeta();

            auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
                Options_->MemoryUsageTracker,
                req->mutable_chunk_meta()->ByteSize());

            FilterProtoExtensions(req->mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
            req->set_request_statistics(true);
            req->add_legacy_replicas(ToProto<ui64>(replica));

            req->set_location_uuids_supported(true);

            auto* replicaInfo = req->add_replicas();
            replicaInfo->set_replica(ToProto<ui64>(replica));
            ToProto(replicaInfo->mutable_location_uuid(), InvalidChunkLocationUuid);
            ToProto(replicaInfo->mutable_replica_spec(), replica);

            if (Uploader_->ChunkSchemaId != NullTableSchemaId) {
                ToProto(req->mutable_schema_id(), Uploader_->ChunkSchemaId);
            }
        }
        auto* multicellSyncExt = req->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        multicellSyncExt->set_suppress_upstream_sync(true);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            sessionId.ChunkId);
    }

    void DoClose()
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        auto objectIdPath = FromObjectId(Uploader_->UserObject.ObjectId);

        YT_LOG_DEBUG("Closing table (UploadTransactionId: %v)", Uploader_->UploadTransaction->GetId());

        StopListenTransaction(Uploader_->UploadTransaction);

        auto endUpload = TTableYPathProxy::EndUpload(objectIdPath);
        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(ChunkCount_);
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_compressed_data_size(DataSize_);
        dataStatistics.set_uncompressed_data_size(DataSize_);
        dataStatistics.set_data_weight(DataSize_);
        *endUpload->mutable_statistics() = dataStatistics;
        Uploader_->EndUpload(endUpload);

        YT_LOG_DEBUG("Table closed (UploadTransactionId: %v)", Uploader_->UploadTransaction->GetId());
        YT_LOG_DEBUG("Attached data statistics (DataStatistics: %v)", dataStatistics);
    }

    // TODO(achulkov2): This whole structure will have to be re-thought once we start creating the destination
    // table if it does not exist yet. E.g. we will need to run the schema inference step first, create the table
    // if it does not exist, and only then instantiate the uploader. Also not sure if we start a transaction by
    // default right now, we might have to start doing that.
    void DoRun()
    {
        DoOpen();
        for (auto& sourceUri : SourceUris_) {
            AttachChunk(sourceUri);
        }
        DoClose();
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AttachTable(
    const TRichYPath& richPath,
    NNative::IClientPtr client,
    ITransactionPtr transaction,
    std::vector<std::string> sourceUris)
{
    return New<TTableAttacher>(
        richPath,
        std::move(client),
        std::move(transaction),
        std::move(sourceUris))->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
