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
#include <yt/yt/client/table_client/external_source_spec.h>

#include <yt/yt/library/s3/object.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
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
        TExternalSourceSpec sourceSpec,
        const TAttachTableOptions& options,
        NNative::IClientPtr client,
        ITransactionPtr transaction)
        : RichPath_(richPath)
        , SourceSpec_(std::move(sourceSpec))
        , Options_(options)
        , TableWriterOptions_(New<TTableWriterOptions>())
        , Client_(std::move(client))
        , Logger(TableClientLogger())
    {
        YT_LOG_DEBUG("Attaching external data to table (Path: %v, SourceSpec: %v, AllowIncompatibleSourceSchemas: %v, Medium: %v, SourceFormat: %lv)",
            richPath.GetPath(),
            SourceSpec_,
            Options_.AllowIncompatibleSourceSchemas,
            Options_.Medium,
            Options_.SourceFormat);

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

    TFuture<TAttachTableResult> Run()
    {
        return BIND(&TTableAttacher::DoRun, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const TRichYPath RichPath_;
    const TExternalSourceSpec SourceSpec_;
    const TAttachTableOptions Options_;
    const TTableWriterOptionsPtr TableWriterOptions_;
    const NNative::IClientPtr Client_;

    NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    TTransactionId TransactionId_;

    TS3MediumDescriptorPtr S3MediumDescriptor_;

    std::vector<std::string> SourceUris_;

    TTableSchemaPtr InferredSchemaOfSources_;

    struct TSourceChunkInfo
    {
        TTableSchemaPtr Schema;
        EExternalSourceFormat SourceFormat;
        EChunkFormat ChunkFormat;
        TRefCountedChunkMetaPtr Meta;
        i64 RowCount;
        i64 DataSize;
    };
    std::vector<TSourceChunkInfo> ChunkInfos_;

    std::optional<TSchemalessTableUploader> Uploader_;
    int ChunkCount_ = 0;
    i64 DataSize_ = 0;
    i64 RowCount_ = 0;

    // This struct will be returned as the result of the operation.
    TAttachTableResult AttachTableResult_;

    TAttachTableResult DoRun()
    {
        try {
            GuardedRun();
            return AttachTableResult_;
        } catch (const TFiberCanceledException&) {
            bool waitForCleanUp = false;
            YT_LOG_DEBUG("Attach table operation was canceled, cleaning up (WaitForCleanUp: %v)", waitForCleanUp);
            CleanUp(waitForCleanUp);
            throw;
        } catch (const std::exception& ex) {
            bool waitForCleanUp = true;
            YT_LOG_DEBUG("Attach table operation failed, cleaning up (WaitForCleanUp: %v)", waitForCleanUp);
            CleanUp(waitForCleanUp);
            THROW_ERROR_EXCEPTION("Failed to attach external data to table %Qv", RichPath_.GetPath())
                << ex;
        } catch (...) {
            bool waitForCleanUp = false;
            YT_LOG_DEBUG("Attach table operation failed with unknown error, cleaning up (WaitForCleanUp: %v)", waitForCleanUp);
            CleanUp(waitForCleanUp);
            throw;
        }
    }

    void GuardedRun()
    {
        if (Options_.Medium) {
            // If the medium was specified, we can use it to infer the schema of sources.
            InferSourcesAndSchemasFromMedium(*Options_.Medium);
            CreateTable(*Options_.Medium);
        }

        PrepareUpload();

        ValidateDestinationTableParameters();

        if (!Options_.Medium) {
            // If medium wasn't specified, it means we haven't inferred the schema yet - do it now.
            InferSourcesAndSchemasFromMedium(TableWriterOptions_->MediumName);
        }

        ValidateSchemaCompatibility();

        BeginUpload();
        AttachChunks();
        EndUpload();
    }

    void InferSourcesAndSchemasFromMedium(const TString& medium)
    {
        RetrieveS3MediumDescriptor(medium);
        InferSourceUris();
        InferCommonSchema();
    }

    void InferSourceUris()
    {
        YT_VERIFY(S3MediumDescriptor_);
        ValidateAborted();

        YT_LOG_DEBUG("Inferring source URIs from source spec (Spec: %v)", SourceSpec_);

        switch (SourceSpec_.GetCurrentType()) {
            case EExternalSourceSpecType::Files:
                InferSourceUris(SourceSpec_.TryGetConcrete<TFilesExternalSourceSpec>());
                break;

            case EExternalSourceSpecType::Prefix:
                InferSourceUris(SourceSpec_.TryGetConcrete<TPrefixExternalSourceSpec>());
                break;

            default:
                THROW_ERROR_EXCEPTION("Unsupported source spec type %Qlv",
                    SourceSpec_.GetCurrentType());
        }

        if (SourceUris_.empty()) {
            THROW_ERROR_EXCEPTION("At least one source must be specified or inferred from the source spec")
                << TErrorAttribute("source_spec", SourceSpec_);
        }

        YT_LOG_DEBUG("Inferred source URIs from source spec (SpecType: %lv, SourceUriCount: %v)", SourceSpec_.GetCurrentType(), SourceUris_.size());
    }

    void InferSourceUris(const TFilesExternalSourceSpecPtr& spec)
    {
        YT_VERIFY(spec);

        SourceUris_ = spec->Uris;
    }

    void InferSourceUris(const TPrefixExternalSourceSpecPtr& spec)
    {
        YT_VERIFY(spec);

        auto s3Client = S3MediumDescriptor_->GetClient();

        auto prefixObjectDescriptor = NS3::TObjectDescriptor::FromUri(spec->PrefixUri, /*allowEmptyKey*/ true);

        NS3::TListObjectsResponse rsp;
        do {
            NS3::TListObjectsRequest req;
            req.Bucket = prefixObjectDescriptor.Bucket();
            req.Prefix = prefixObjectDescriptor.Key();

            // Remove extraneous trailing slashes, only one is needed to denote a directory.
            // This helps us cut the prefix from the returned object keys correctly, so we
            // can apply include/exclude patterns to the remaining part of the key.
            while (req.Prefix.EndsWith("//")) {
                req.Prefix.pop_back();
            }

            // Specifying a delimiter leads to listing top-level files only.
            // Trailing slash is required for non-empty prefixes, otherwise files end up being skipped.
            if (!spec->Recursive) {
                if (!req.Prefix.empty() && !req.Prefix.EndsWith('/')) {
                    req.Prefix += '/';
                }
                req.Delimiter = "/";
            }

            req.ContinuationToken = rsp.NextContinuationToken;

            rsp = WaitFor(s3Client->ListObjects(req))
                .ValueOrThrow();

            for (const auto& object: rsp.Objects) {
                auto fullObjectUri = Format("s3://%v/%v", req.Bucket, object.Key);

                if (!object.Key.StartsWith(req.Prefix)) {
                    THROW_ERROR_EXCEPTION(
                        "S3 object key %Qv does not start with the requested prefix %Qv; "
                        "this is a logical error, please contact cluster administrators with the full contents of this error",
                        object.Key,
                        req.Prefix)
                        << TErrorAttribute("bucket", req.Bucket)
                        << TErrorAttribute("prefix", req.Prefix)
                        << TErrorAttribute("delimiter", req.Delimiter)
                        << TErrorAttribute("object_key", object.Key);
                }

                auto relativeObjectKey = object.Key.substr(req.Prefix.size());

                if (!spec->IncludeRegexes.empty() && !MatchesAnyPattern(relativeObjectKey, spec->IncludeRegexes)) {
                    YT_LOG_DEBUG(
                        "Source URI does not match any include pattern, skipping (SourceUri: %v, RelativeKey: %v)",
                        fullObjectUri,
                        relativeObjectKey);
                    continue;
                }

                if (auto matchedPattern = MatchesAnyPattern(relativeObjectKey, spec->ExcludeRegexes); !spec->ExcludeRegexes.empty() && matchedPattern) {
                    YT_LOG_DEBUG(
                        "Source URI matches an exclude pattern, skipping (SourceUri: %v, RelativeKey: %v, MatchedExcludePattern: %v)",
                        fullObjectUri,
                        relativeObjectKey,
                        *matchedPattern);
                    continue;
                }

                SourceUris_.push_back(fullObjectUri);

                YT_LOG_DEBUG("Inferred source URI (SourceUri: %v)", fullObjectUri);
            }
        } while (rsp.NextContinuationToken);
    }

    void ValidateSchemaCompatibility()
    {
        ValidateAborted();

        auto [schemaCompatibility, schemaCompatibilityError] = CheckTableSchemaCompatibility(
            *InferredSchemaOfSources_,
            *Uploader_->ChunkSchema,
            TTableSchemaCompatibilityOptions{});
        if (schemaCompatibility != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION("Inferred schema for the sources is not compatible with table schema")
                << schemaCompatibilityError;
        }
    }

    void PrepareUpload()
    {
        ValidateAborted();

        try {
            Uploader_.emplace(TableWriterOptions_, RichPath_, Client_, TransactionId_);
        } catch (const TErrorException& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to retrieve upload parameters for destination table %Qv; check that it exists or specify medium in the parameters",
                RichPath_.GetPath())
                << ex;
        }
    }

    void ValidateDestinationTableParameters()
    {
        ValidateAborted();

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
        }
    }

    void AttachChunks()
    {
        ValidateAborted();

        YT_VERIFY(SourceUris_.size() == ChunkInfos_.size());
        for (ssize_t sourceIndex = 0; sourceIndex < std::ssize(SourceUris_); ++sourceIndex) {
            AttachChunk(SourceUris_[sourceIndex], ChunkInfos_[sourceIndex]);
        }
    }

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

    EExternalSourceFormat GetExternalSourceFormat(const NS3::TObjectDescriptor& descriptor)
    {
        if (Options_.SourceFormat) {
            return *Options_.SourceFormat;
        } else {
            return DeduceExternalSourceFormatOrThrow(descriptor.Key());
        }
    }

    void InferCommonSchema()
    {
        YT_VERIFY(S3MediumDescriptor_);
        YT_VERIFY(!SourceUris_.empty());
        ValidateAborted();

        // First, retrieve the schemas of all sources.
        ChunkInfos_.reserve(SourceUris_.size());
        for (const auto& sourceUri: SourceUris_) {
            // TODO(achulkov2): We might want to do something else once we support multiple different schemas, not just S3.
            // Probably it should go into a separate method that lives somewhere near chunk meta generators and dispatches
            // to the correct one from there.
            auto sourceS3Descriptor = NS3::TObjectDescriptor::FromUri(sourceUri);
            auto sourceFormat = GetExternalSourceFormat(sourceS3Descriptor);
            auto chunkFormat = GetChunkFormatFromExternalSourceFormat(sourceFormat);

            // NB: There is a context switch inside the constructor for TS3ArrowRandomAccessFile.
            auto chunkMetaGenerator = CreateArrowTableChunkMetaGenerator(
                chunkFormat,
                std::make_shared<TS3ArrowRandomAccessFile>(
                    sourceS3Descriptor,
                    S3MediumDescriptor_->GetClient()));

            // NB: Possible context switch.
            chunkMetaGenerator->Generate();

            ChunkInfos_.push_back(TSourceChunkInfo(
                chunkMetaGenerator->GetChunkSchema(),
                sourceFormat,
                chunkFormat,
                chunkMetaGenerator->GetChunkMeta(),
                chunkMetaGenerator->GetRowCount(),
                chunkMetaGenerator->GetUncompressedSize()));

            YT_LOG_DEBUG("Inferred schema for source (SourceUri: %v, InferredSchema: %v)",
                sourceUri,
                chunkMetaGenerator->GetChunkSchema());
        }

        // Second, try to infer the common schemas of all sources.
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

        YT_LOG_DEBUG("Inferred common schema for all sources (Schema: %v)", commonSchema);
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

    void BeginUpload()
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        Uploader_->BeginUpload();
        StartListenTransaction(Uploader_->UploadTransaction);

        YT_LOG_DEBUG(
            "Table opened for attaching external data (UploadTransactionId: %v)",
            Uploader_->UploadTransaction->GetId());
    }

    void AttachChunk(const std::string& sourceUri, const TSourceChunkInfo& chunkInfo)
    {
        YT_VERIFY(Uploader_);

        ValidateAborted();

        YT_LOG_DEBUG("Attaching chunk to table (SourceUri: %v, ChunkFormat: %lv)",
            sourceUri,
            chunkInfo.ChunkFormat);

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
            chunkInfo.ChunkFormat,
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

        AttachTableResult_.ChunkInfos.push_back({
            .ChunkId = sessionId.ChunkId,
            .RowCount = chunkInfo.RowCount,
            .UncompressedDataSize = chunkInfo.DataSize,
            .SourceUri = sourceUri,
            .SourceFormat = chunkInfo.SourceFormat,
            .ChunkFormat = chunkInfo.ChunkFormat,
        });
    }

    void EndUpload()
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

        AttachTableResult_.TotalChunkCount = ChunkCount_;
        AttachTableResult_.TotalRowCount = RowCount_;
        AttachTableResult_.TotalUncompressedDataSize = DataSize_;

        YT_LOG_DEBUG("Table closed (UploadTransactionId: %v)", Uploader_->UploadTransaction->GetId());
        YT_LOG_DEBUG("Attached data statistics (DataStatistics: %v)", dataStatistics);
    }

    // NB(achulkov2): This method can be called upon fiber cancellation, so we are being extra
    // safe and providing a clean up mode in which we do not call WaitFor.
    void CleanUp(bool wait)
    {
        // It is possible for a transaction with AutoAbort = true to survive beyond the lifetime
        // of this object via the strong ref in a Ping request. Thus it is best to abort the
        // transaction in case of errors.
        // It is fine to abort the transaction unconditionally, since if we are done with it,
        // it would be in Committed/Detached state and abort is no-op and just returns an error.
        // This will also abort the transaction we may have started in Uploader as it's a child
        // of this one.
        if (Transaction_) {
            auto abortFuture = Transaction_->Abort();
            if (wait) {
                auto error = WaitFor(abortFuture);
                if (!error.IsOK() && !error.FindMatching(NTransactionClient::EErrorCode::InvalidTransactionState)) {
                    YT_LOG_WARNING(
                        error,
                        "Failed to abort attach table nested transaction during cleanup (TransactionId: %v)",
                        TransactionId_);
                }
            } else {
                YT_UNUSED_FUTURE(abortFuture);
            }
        }
    }

    static std::optional<NRe2::TRe2Ptr> MatchesAnyPattern(const TString& str, const std::vector<NRe2::TRe2Ptr>& patterns)
    {
        for (const auto& pattern : patterns) {
            // We check against null patterns in YSON postprocessor, but it can still be manually passed here.
            // Easiest just to skip them.
            if (!pattern) {
                continue;
            }

            if (RE2::FullMatch(str, *pattern)) {
                return pattern;
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TAttachTableResult> AttachTable(
    TRichYPath richPath,
    TExternalSourceSpec sourceSpec,
    TAttachTableOptions options,
    NNative::IClientPtr client,
    ITransactionPtr transaction)
{
    return New<TTableAttacher>(
        std::move(richPath),
        std::move(sourceSpec),
        std::move(options),
        std::move(client),
        std::move(transaction))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
