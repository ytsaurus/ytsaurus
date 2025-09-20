#include "attach_table.h"
#include "schemaless_table_uploader.h"
#include "schemaless_block_writer.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_generator.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/s3_common.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/merge_table_schemas.h>

#include <yt/yt/library/s3/object.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TTableAttacher
    : public TTransactionListener
{
public:
    TTableAttacher(
        const TRichYPath& richPath,
        const TAttachTableOptions& options,
        NNative::IClientPtr client,
        ITransactionPtr transaction,
        std::vector<std::string> sourceUris)
        : Options_(options)
        , TableWriterOptions_(New<NTableClient::TTableWriterOptions>())
        , RichPath_(richPath)
        , Client_(std::move(client))
        , SourceUris_(std::move(sourceUris))
        , Logger(TableClientLogger())
    {
        YT_LOG_DEBUG("Attaching external data to table (Path: %v, SourceUris: %v, AllowIncompatibleSourceSchemas: %v, Medium: %v, SourceFormat: %lv)",
            richPath.GetPath(),
            SourceUris_,
            Options_.AllowIncompatibleSourceSchemas,
            Options_.Medium,
            Options_.SourceFormat);

        THROW_ERROR_EXCEPTION_IF(
            SourceUris_.empty(),
            "At least one source must be specified when attaching to a table %Qv",
            RichPath_.GetPath());

        TTransactionStartOptions transactionOptions;
        transactionOptions.ParentId = transaction ? transaction->GetId() : NullTransactionId;

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Attach to table %v", RichPath_.GetPath()));
        transactionOptions.Attributes = std::move(attributes);

        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, transactionOptions))
            .ValueOrThrow();
        TransactionId_ = Transaction_->GetId();

        StartListenTransaction(Transaction_);

        Logger.AddTag("Path: %v, TransactionId: %v",
            richPath.GetPath(),
            TransactionId_);

        YT_LOG_DEBUG("Started nested input transaction for attach table (ParentTransactionId: %v)",
            transactionOptions.ParentId);
    }

    ~TTableAttacher()
    {
        // It is possible for a transaction with AutoAbort = true to survive beyond the lifetime
        // of this object via the strong ref in a Ping request. Thus it is best to abort the
        // transaction in case of errors.
        // It is fine to abort the transaction unconditionally, since if we are done with it,
        // it would be in Committed/Detached state and abort is no-op and just returns an error.
        // This will also abort the transaction we may have started in Uploader as it's a child
        // of this one.
        if (Transaction_) {
            Y_UNUSED(WaitFor(Transaction_->Abort()));
        }
    }

    TFuture<void> Run()
    {
        return BIND(&TTableAttacher::DoRun, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const TAttachTableOptions Options_;
    const TTableWriterOptionsPtr TableWriterOptions_;
    const TRichYPath RichPath_;
    const NNative::IClientPtr Client_;
    const std::vector<std::string> SourceUris_;

    NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    TTransactionId TransactionId_;

    TS3MediumDescriptorPtr S3MediumDescriptor_;

    TTableSchemaPtr InferredSchemaOfSources_;

    struct ChunkInfo
    {
        TTableSchemaPtr Schema;
        EChunkFormat Format;
        TRefCountedChunkMetaPtr Meta;
        i64 RowCount;
        i64 DataSize;
    };
    std::vector<ChunkInfo> ChunkInfos_;

    std::optional<TSchemalessTableUploader> Uploader_;
    int ChunkCount_ = 0;
    i64 DataSize_ = 0;
    i64 RowCount_ = 0;

    void RetrieveS3MediumDescriptor(const TString& medium)
    {
        YT_VERIFY(!S3MediumDescriptor_);
        ValidateAborted();

        // Check that the user has a permission to access this medium.
        const auto& permissionCache = Client_->GetNativeConnection()->GetPermissionCache();
        NSecurityClient::TPermissionKey permissionKey{
            .Object = Format("//sys/media/%v", medium),
            .User = Client_->GetOptions().GetAuthenticatedUser(),
            .Permission = NYTree::EPermission::Use,
        };
        WaitFor(permissionCache->Get(permissionKey))
            .ThrowOnError("User has no access to medium %Qv", medium);

        const auto& mediumDirectory = Client_->GetNativeConnection()->GetMediumDirectory();
        auto mediumDescriptor = mediumDirectory->FindByName(medium);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION(
                "Medium with name %v not found in medium directory; please contact cluster administrators if the problem persists",
                medium);
        }

        S3MediumDescriptor_ = mediumDescriptor->As<NYT::NChunkClient::TS3MediumDescriptor>();
        if (!S3MediumDescriptor_) {
            THROW_ERROR_EXCEPTION("Cannot attach external data to table on non-offshore medium %Qv", medium);
        }
    }

    EChunkFormat GetChunkFormat(const NS3::TObjectDescriptor& descriptor)
    {
        auto externalFormat = [&] {
            if (Options_.SourceFormat) {
                return *Options_.SourceFormat;
            } else {
                return DeduceExternalSourceFormatOrThrow(descriptor.Key());
            }
        }();

        return GetChunkFormatFromExternalSourceFormat(externalFormat);
    }

    void InferCommonSchema()
    {
        YT_VERIFY(S3MediumDescriptor_);
        YT_VERIFY(!SourceUris_.empty());
        ValidateAborted();

        // Firstly, retrieve the schemas of all sources.
        ChunkInfos_.reserve(SourceUris_.size());
        for (const auto& sourceUri: SourceUris_) {
            // TODO(achulkov2): We might want to do something else once we support multiple different schemas, not just S3.
            // Probably it should go into a separate method that lives somewhere near chunk meta generators and dispatches
            // to the correct one from there.
            auto sourceS3Descriptor = NS3::TObjectDescriptor::FromUri(sourceUri);
            auto chunkFormat = GetChunkFormat(sourceS3Descriptor);

            // NB: There is a context switch inside the constructor for TS3ArrowRandomAccessFile.
            auto chunkMetaGenerator = CreateArrowTableChunkMetaGenerator(
                chunkFormat,
                std::make_shared<TS3ArrowRandomAccessFile>(
                    sourceS3Descriptor,
                    S3MediumDescriptor_->GetClient()));

            // NB: Possible context switch.
            chunkMetaGenerator->Generate();

            ChunkInfos_.push_back(ChunkInfo(
                chunkMetaGenerator->GetChunkSchema(),
                chunkFormat,
                chunkMetaGenerator->GetChunkMeta(),
                chunkMetaGenerator->GetRowCount(),
                chunkMetaGenerator->GetUncompressedSize()));

            YT_LOG_DEBUG("Inferred schema for source (SourceUri: %v, InferredSchema: %v)",
                sourceUri,
                chunkMetaGenerator->GetChunkSchema());
        }

        // Secondly, try to infer the common schemas between all sources.
        TTableSchemaPtr commonSchema;
        for (const auto& chunkInfo: ChunkInfos_) {
            if (!commonSchema) {
                commonSchema = chunkInfo.Schema;
                continue;
            }

            try {
                commonSchema = MergeTableSchemas(commonSchema, chunkInfo.Schema);
            } catch (const TErrorException& ex) {
                if (!Options_.AllowIncompatibleSourceSchemas) {
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::IncompatibleSchemas,
                        "Could not infer common schema of all sources; specify allow_incompatible_source_schemas option to allow weak schemas")
                        << ex;
                }

                YT_LOG_DEBUG(
                    ex,
                    "Schemas of sources are incompatible with each other, inferring an empty weak schema for the destination table");
                InferredSchemaOfSources_ = New<TTableSchema>();
                return;
            }
        }

        YT_LOG_DEBUG("Inferred common schema for all sources (Schema: %v)",
            commonSchema);
        InferredSchemaOfSources_ = commonSchema;
    }

    void CreateTable(const TString& medium)
    {
        YT_VERIFY(!medium.empty());
        if (!Options_.AllowIncompatibleSourceSchemas) {
            YT_VERIFY(!InferredSchemaOfSources_->IsEmpty());
        }
        ValidateAborted();

        const auto& tablePath = RichPath_.GetPath();

        YT_LOG_DEBUG("Creating table (TablePath: %v, Schema: %v, Medium: %v)",
            tablePath,
            InferredSchemaOfSources_,
            medium);

        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("schema", InferredSchemaOfSources_);
        options.Attributes->Set("primary_medium", medium);
        options.Attributes->Set("optimize_for", "lookup");

        WaitFor(Transaction_->CreateNode(tablePath, EObjectType::Table, options))
            .ThrowOnError();

        YT_LOG_DEBUG("Created table (TablePath: %v)", tablePath);
    }

    void DoOpen()
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        Uploader_->BeginUpload();
        StartListenTransaction(Uploader_->UploadTransaction);

        YT_LOG_DEBUG(
            "Table opened for attaching external data (UploadTransactionId: %v)",
            Uploader_->UploadTransaction->GetId());
    }

    void AttachChunk(const std::string& sourceUri, const ChunkInfo& chunkInfo)
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        YT_LOG_DEBUG("Attaching chunk to table (SourceUri: %v, ChunkFormat: %lv)",
            sourceUri,
            chunkInfo.Format);

        auto sessionId = NChunkClient::CreateChunk(
            Client_,
            Uploader_->UserObject.ExternalCellTag,
            TableWriterOptions_,
            Uploader_->UploadTransaction->GetId(),
            Uploader_->ChunkListId,
            Logger);

        ChunkCount_++;
        RowCount_ += chunkInfo.RowCount;
        DataSize_ += chunkInfo.DataSize;

        YT_LOG_DEBUG("Confirming attached chunk (SourceUri: %v, ChunkId: %v, ChunkFormat: %lv, RowCount: %v, UncompressedDataSize: %v)",
            sourceUri,
            sessionId.ChunkId,
            chunkInfo.Format,
            chunkInfo.RowCount,
            chunkInfo.DataSize);

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
            *req->mutable_chunk_meta() = *chunkInfo.Meta;

            auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
                TableWriterOptions_->MemoryUsageTracker,
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

        WaitFor(Transaction_->Commit())
            .ThrowOnError();

        YT_LOG_DEBUG("Table closed (UploadTransactionId: %v)", Uploader_->UploadTransaction->GetId());
        YT_LOG_DEBUG("Attached data statistics (DataStatistics: %v)", dataStatistics);
    }

    void DoRun()
    {
        if (Options_.Medium) {
            RetrieveS3MediumDescriptor(*Options_.Medium);
            InferCommonSchema();
            CreateTable(*Options_.Medium);
        }

        try {
            Uploader_.emplace(TableWriterOptions_, RichPath_, Client_, TransactionId_);
        } catch (const TErrorException& ex) {
            THROW_ERROR_EXCEPTION(
                "Cannot attach external data to table %Qv; check that it exists or specify medium in the parameters",
                RichPath_.GetPath())
                << ex;
        }

        // TableWriterOptions_ are updated by the Uploader when it's created.
        if (TableWriterOptions_->ErasureCodec != NErasure::ECodec::None) {
            THROW_ERROR_EXCEPTION("Cannot attach external data to table with erasure codec %Qlv",
                TableWriterOptions_->ErasureCodec);
        }

        auto outputTableMedium = TableWriterOptions_->MediumName;
        if (Options_.Medium) {
            THROW_ERROR_EXCEPTION_IF(
                *Options_.Medium != outputTableMedium,
                "Output table %Qv exists, but its medium %Qv is not the same as the specified one %Qv",
                RichPath_.GetPath(),
                outputTableMedium,
                *Options_.Medium);
        } else {
            // If medium wasn't specified, it means we haven't inferred the schema yet - do it now.
            YT_VERIFY(!S3MediumDescriptor_);
            RetrieveS3MediumDescriptor(outputTableMedium);
            InferCommonSchema();
        }

        auto [schemaCompatibility, schemaCompatibilityError] = CheckTableSchemaCompatibility(
            *InferredSchemaOfSources_,
            *Uploader_->ChunkSchema,
            TTableSchemaCompatibilityOptions{});
        if (schemaCompatibility != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION("Inferred schema for the sources is not compatible with table schema")
                << schemaCompatibilityError;
        }

        DoOpen();

        YT_VERIFY(SourceUris_.size() == ChunkInfos_.size());
        for (ssize_t sourceIndex = 0; sourceIndex < std::ssize(SourceUris_); ++sourceIndex) {
            AttachChunk(SourceUris_[sourceIndex], ChunkInfos_[sourceIndex]);
        }

        DoClose();
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AttachTable(
    const TRichYPath& richPath,
    const TAttachTableOptions& options,
    NNative::IClientPtr client,
    ITransactionPtr transaction,
    std::vector<std::string> sourceUris)
{
    return New<TTableAttacher>(
        richPath,
        options,
        std::move(client),
        std::move(transaction),
        std::move(sourceUris))->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
