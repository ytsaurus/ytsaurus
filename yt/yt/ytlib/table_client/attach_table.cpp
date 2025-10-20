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

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

//! Thread-safe schema merger that supports downgrading to weak schema on incompatibility.
class TMaybeWeakTableSchema
{
public:
    TMaybeWeakTableSchema() = default;

    void MergeWith(const TTableSchemaPtr& other)
    {
        auto guard = Guard(SpinLock_);

        if (IsWeak_) {
            return;
        }

        if (!Schema_) {
            Schema_ = other;
            return;
        }

        try {
            Schema_ = MergeTableSchemas(Schema_, other);
        } catch (const TErrorException& ex) {
            IncompatibilityError_ = ex;
            Schema_ = New<TTableSchema>();
            IsWeak_ = true;
        }
    }

    bool IsWeak() const
    {
        auto guard = Guard(SpinLock_);
        return IsWeak_;
    }

    TTableSchemaPtr GetSchema() const
    {
        auto guard = Guard(SpinLock_);
        return Schema_;
    }

    TError GetIncompatibilityError() const
    {
        auto guard = Guard(SpinLock_);
        return IncompatibilityError_;
    }

    void Verify(bool allowIncompatibility)
    {
        auto guard = Guard(SpinLock_);
        YT_VERIFY(Schema_);
        YT_VERIFY(!Schema_->IsEmpty() || allowIncompatibility);
        YT_VERIFY(!IsWeak_ || allowIncompatibility);
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TTableSchemaPtr Schema_;
    bool IsWeak_ = false;
    TError IncompatibilityError_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

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
        // This is a no-op, as writer invoker is single-threaded, but it protects us if it ever changes.
        , SerializedInvoker_(CreateSerializedInvoker(NChunkClient::TDispatcher::Get()->GetWriterInvoker()))
        // TODO(achulkov2): Create a separate thread pool for S3-related operations?
        , HeavyInvoker_(NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , ChunkServiceBoundedConcurrencyInvoker_(CreateBoundedConcurrencyInvoker(
            HeavyInvoker_,
            ChunkServiceConcurrencyBound_))
        , Logger(TableClientLogger())
    {
        YT_LOG_DEBUG(
            "Attaching external data to table (Path: %v, SourceSpec: %v, AllowIncompatibleSourceSchemas: %v, Medium: %v, SourceFormat: %lv, AttachMode: %lv, SourceOrder: %lv)",
            richPath.GetPath(),
            SourceSpec_,
            Options_.AllowIncompatibleSourceSchemas,
            Options_.Medium,
            Options_.SourceFormat,
            Options_.AttachMode,
            Options_.SourceOrder);

        TTransactionStartOptions transactionOptions;
        transactionOptions.ParentId = transaction ? transaction->GetId() : NullTransactionId;

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Attach to table %v", RichPath_.GetPath()));
        transactionOptions.Attributes = std::move(attributes);

        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, transactionOptions))
            .ValueOrThrow();
        TransactionId_ = Transaction_->GetId();

        StartListenTransaction(Transaction_);

        Logger.AddTag(
            "Path: %v, TransactionId: %v",
            richPath.GetPath(),
            TransactionId_);

        YT_LOG_DEBUG(
            "Started nested input transaction for attach table (ParentTransactionId: %v)",
            transactionOptions.ParentId);
    }

    TFuture<TAttachTableResult> Run()
    {
        return BIND(&TTableAttacher::DoRun, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    const TRichYPath RichPath_;
    const TExternalSourceSpec SourceSpec_;
    const TAttachTableOptions Options_;
    const TTableWriterOptionsPtr TableWriterOptions_;
    const NNative::IClientPtr Client_;

    // The overall approach to concurrency in this class is as follows:
    //   - Main lightweight operations should be executed in the serialized invoker,
    //     thread safety is guaranteed by its serialization property.
    //   - Heavy operations should be offloaded to multi-threaded invokers.
    //     Make sure to either protect shared state with locks, or synchronize
    //     on the result in the control invoker.
    //   - Bulk requests to master should be executed with bounded concurrency.
    // Make sure to annotate methods with the invoker they are expected to run in.

    //! Used for executing lightweight command logic.
    const IInvokerPtr SerializedInvoker_;
    //! Used for executing source chunk processing callbacks.
    const IInvokerPtr HeavyInvoker_;
    //! Used for executing callbacks that make requests to master's ChunkService.
    //! Even though queue size limit exceeded errors are generally retriable,
    //! concurrency is limited to avoid overloading master.
    static constexpr int ChunkServiceConcurrencyBound_ = 100;
    const IInvokerPtr ChunkServiceBoundedConcurrencyInvoker_;

    NLogging::TLogger Logger;

    ITransactionPtr Transaction_;
    TTransactionId TransactionId_;

    TS3MediumDescriptorPtr S3MediumDescriptor_;

    std::vector<std::string> SourceUris_;

    //! This field *is* accessed from multiple threads concurrently and *is* thread-safe.
    TMaybeWeakTableSchema CommonSourceSchema_;

    struct TProcessedSourceInfo
    {
        TTableSchemaPtr Schema;
        EExternalSourceFormat SourceFormat;
        EChunkFormat ChunkFormat;
        // NB: Only contains extensions stored on master!
        TRefCountedChunkMetaPtr MasterChunkMeta;
        i64 RowCount;
        i64 UncompressedDataSize;
        i64 CompressedDataSize;
    };
    THashMap<std::string, TProcessedSourceInfo> SourceUriToProcessedInfo_;

    std::optional<TSchemalessTableUploader> Uploader_;

    int ChunkCount_ = 0;
    i64 UncompressedDataSize_ = 0;
    i64 CompressedDataSize_ = 0;
    i64 RowCount_ = 0;

    // This struct will be returned as the result of the operation.
    TAttachTableResult AttachTableResult_;

    TAttachTableResult DoRun()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        try {
            return GuardedRun();
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

    TAttachTableResult GuardedRun()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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

        return AttachTableResult_;
    }

    void InferSourcesAndSchemasFromMedium(const TString& medium)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        RetrieveS3MediumDescriptor(medium);
        InferSourceUris();
        ProcessSourcesAndInferCommonSchema();
    }

    void InferSourceUris()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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
                THROW_ERROR_EXCEPTION("Unsupported source spec type %Qlv", SourceSpec_.GetCurrentType());
        }

        if (SourceUris_.empty()) {
            THROW_ERROR_EXCEPTION("At least one source must be specified or inferred from the source spec")
                << TErrorAttribute("source_spec", SourceSpec_);
        }

        YT_LOG_DEBUG("Inferred source URIs from source spec (SpecType: %lv, SourceUriCount: %v)", SourceSpec_.GetCurrentType(), SourceUris_.size());

        switch (Options_.SourceOrder) {
            case EAttachTableSourceOrder::None:
                // Do nothing, keep the order as is.
                break;

            case EAttachTableSourceOrder::LexAsc:
                std::sort(SourceUris_.begin(), SourceUris_.end());
                break;

            case EAttachTableSourceOrder::LexDesc:
                std::sort(SourceUris_.rbegin(), SourceUris_.rend());
                break;

            default:
                THROW_ERROR_EXCEPTION("Unsupported source order %Qlv", Options_.SourceOrder);
        }

        YT_LOG_DEBUG("Sorted source URIs according to source order (SourceOrder: %lv)", Options_.SourceOrder);
    }

    void InferSourceUris(const TFilesExternalSourceSpecPtr& spec)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(spec);

        SourceUris_ = spec->Uris;
    }

    void InferSourceUris(const TPrefixExternalSourceSpecPtr& spec)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        ValidateAborted();

        auto [schemaCompatibility, schemaCompatibilityError] = CheckTableSchemaCompatibility(
            *CommonSourceSchema_.GetSchema(),
            *Uploader_->ChunkSchema,
            TTableSchemaCompatibilityOptions{});
        if (schemaCompatibility != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION("Inferred schema for the sources is not compatible with table schema")
                << schemaCompatibilityError;
        }
    }

    void PrepareUpload()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        ValidateAborted();

        auto asyncAttachedChunkInfos = [&] {
            switch (Options_.AttachMode) {
                case EAttachTableMode::Sequential:
                    return SequentialAttachChunks();
                case EAttachTableMode::Parallel:
                    return ParallelAttachChunks();
                default:
                    THROW_ERROR_EXCEPTION("Unsupported attach mode %Qlv", Options_.AttachMode);
            }
        }();

        auto attachedChunkInfos = WaitFor(asyncAttachedChunkInfos)
            .ValueOrThrow();

        AttachTableResult_.TotalChunkCount = ChunkCount_;
        AttachTableResult_.TotalRowCount = RowCount_;
        AttachTableResult_.TotalUncompressedDataSize = UncompressedDataSize_;
        AttachTableResult_.ChunkInfos = std::move(attachedChunkInfos);
    }

    TFuture<std::vector<TAttachedChunkInfo>> SequentialAttachChunks()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TAttachedChunkInfo> attachedChunkInfos;
        attachedChunkInfos.reserve(SourceUris_.size());

        for (const auto& sourceUri : SourceUris_) {
            auto attachedChunkInfo = WaitFor(AttachChunk(sourceUri, GetOrCrash(SourceUriToProcessedInfo_, sourceUri)))
                .ValueOrThrow();
            attachedChunkInfos.push_back(std::move(attachedChunkInfo));
        }

        return MakeFuture(attachedChunkInfos);
    }

    TFuture<std::vector<TAttachedChunkInfo>> ParallelAttachChunks()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<TAttachedChunkInfo>> asyncAttachedChunkInfos;
        asyncAttachedChunkInfos.reserve(SourceUris_.size());

        for (const auto& sourceUri : SourceUris_) {
            asyncAttachedChunkInfos.push_back(AttachChunk(sourceUri, GetOrCrash(SourceUriToProcessedInfo_, sourceUri)));
        }

        return AllSucceeded(asyncAttachedChunkInfos);
    }

    void RetrieveS3MediumDescriptor(const TString& medium)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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

    void ProcessSourcesAndInferCommonSchema()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(S3MediumDescriptor_);
        YT_VERIFY(!SourceUris_.empty());
        ValidateAborted();

        std::vector<TFuture<TProcessedSourceInfo>> asyncChunkInfos;
        asyncChunkInfos.reserve(SourceUris_.size());

        // Schema inference and merging is done inside ProcessSource calls, which are executed in parallel.
        for (const auto& sourceUri: SourceUris_) {
            asyncChunkInfos.push_back(ProcessSource(sourceUri));
        }

        // Default combiner options will lead to early exit on the first error, e.g. if schemas are incompatible.
        auto chunkInfos = WaitFor(AllSucceeded(asyncChunkInfos))
            .ValueOrThrow();

        // At this point all sources have been processed and their schemas have been merged.
        // If weak schemas were not allowed, we would have thrown from the previous line.
        if (CommonSourceSchema_.IsWeak()) {
            YT_LOG_DEBUG(
                CommonSourceSchema_.GetIncompatibilityError(),
                "Schemas of sources are incompatible with each other, inferring an empty weak schema for the destination table");
        }

        for (int index = 0; index < std::ssize(SourceUris_); ++index) {
            ++ChunkCount_;
            UncompressedDataSize_ += chunkInfos[index].UncompressedDataSize;
            CompressedDataSize_ += chunkInfos[index].CompressedDataSize;
            RowCount_ += chunkInfos[index].RowCount;

            SourceUriToProcessedInfo_.emplace(SourceUris_[index], std::move(chunkInfos[index]));
        }

        YT_LOG_DEBUG(
            "Processed sources (SourceCount: %v, TotalRowCount: %v, TotalUncompressedDataSize: %v, TotalCompressedDataSize: %v, CommonSchema: %v)",
            SourceUris_.size(),
            RowCount_,
            UncompressedDataSize_,
            CompressedDataSize_,
            CommonSourceSchema_.GetSchema());
    }

    TFuture<TProcessedSourceInfo> ProcessSource(const std::string& sourceUri)
    {
        return BIND(&TTableAttacher::DoProcessSource, MakeStrong(this), sourceUri)
            .AsyncVia(HeavyInvoker_)
            .Run();
    }

    TProcessedSourceInfo DoProcessSource(const std::string& sourceUri)
    {
        VERIFY_INVOKER_AFFINITY(HeavyInvoker_);

        YT_VERIFY(S3MediumDescriptor_);

        // TODO(achulkov2): This logic might move if we were to support URI schemas other than S3.
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

        YT_LOG_DEBUG(
            "Inferred schema for source (SourceUri: %v, InferredSchema: %v)",
            sourceUri,
            chunkMetaGenerator->GetChunkSchema());

        // If needed, we could chain this as a callback returning another future, but I don't see the point yet.
        ProcessChunkSchema(chunkMetaGenerator->GetChunkSchema());

        // This is the full meta.
        auto chunkMeta = chunkMetaGenerator->GetChunkMeta();

        // TODO(achulkov2): This is the place where we could store chunk meta somewhere if we wanted to.

        // From this point on chunk meta is pruned to only contain extensions stored on master.
        // This allows us to reduce the memory footprint of the command.
        FilterProtoExtensions(chunkMeta->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());

        return {
            .Schema = chunkMetaGenerator->GetChunkSchema(),
            .SourceFormat = sourceFormat,
            .ChunkFormat = chunkFormat,
            .MasterChunkMeta = chunkMeta,
            .RowCount = chunkMetaGenerator->GetRowCount(),
            .UncompressedDataSize = chunkMetaGenerator->GetUncompressedDataSize(),
            .CompressedDataSize = chunkMetaGenerator->GetCompressedDataSize(),
        };
    }

    // TODO(achulkov2): It would be nice for the error to contain the source URIs that caused the incompatibility,
    // but it is tricky due to concurrency and schema merging. Consider doing this later.
    void ProcessChunkSchema(const TTableSchemaPtr& chunkSchema)
    {
        VERIFY_INVOKER_AFFINITY(HeavyInvoker_);

        CommonSourceSchema_.MergeWith(chunkSchema);

        if (auto error = CommonSourceSchema_.GetIncompatibilityError(); !error.IsOK()) {
            if (!Options_.AllowIncompatibleSourceSchemas) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::IncompatibleSchemas,
                    "Could not infer common schema of all sources; specify allow_incompatible_source_schemas option to allow weak schemas")
                    << error;
            }

            // If incompatible schemas are allowed, we will log this once at the end of the schema inference phase.
        }
    }

    void CreateTable(const TString& medium)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(!medium.empty());

        CommonSourceSchema_.Verify(Options_.AllowIncompatibleSourceSchemas);

        ValidateAborted();

        const auto& tablePath = RichPath_.GetPath();

        YT_LOG_DEBUG(
            "Creating table (TablePath: %v, Schema: %v, IsSchemaWeak: %v, Medium: %v)",
            tablePath,
            CommonSourceSchema_.GetSchema(),
            CommonSourceSchema_.IsWeak(),
            medium);

        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.Attributes = CreateEphemeralAttributes();
        // Setting an empty schema would result in schema_mode strong.
        // In order to create a table with a truly weak schema, we don't set the schema at all.
        if (!CommonSourceSchema_.IsWeak()) {
            options.Attributes->Set("schema", CommonSourceSchema_.GetSchema());
        }
        options.Attributes->Set("primary_medium", medium);
        options.Attributes->Set("optimize_for", "lookup");

        WaitFor(Transaction_->CreateNode(tablePath, EObjectType::Table, options))
            .ThrowOnError();

        YT_LOG_DEBUG("Created table (TablePath: %v)", tablePath);
    }

    void BeginUpload()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(Uploader_);

        ValidateAborted();

        Uploader_->BeginUpload();
        StartListenTransaction(Uploader_->UploadTransaction);

        YT_LOG_DEBUG(
            "Table opened for attaching external data (UploadTransactionId: %v)",
            Uploader_->UploadTransaction->GetId());
    }

    TFuture<TAttachedChunkInfo> AttachChunk(const std::string& sourceUri, const TProcessedSourceInfo& chunkInfo)
    {
        return BIND(&TTableAttacher::DoAttachChunk, MakeStrong(this), sourceUri, chunkInfo)
            .AsyncVia(ChunkServiceBoundedConcurrencyInvoker_)
            .Run();
    }

    TAttachedChunkInfo DoAttachChunk(const std::string& sourceUri, const TProcessedSourceInfo& chunkInfo)
    {
        VERIFY_INVOKER_AFFINITY(ChunkServiceBoundedConcurrencyInvoker_);

        YT_VERIFY(Uploader_);

        // This can cause early exit from the future-combiner if the transaction was aborted.
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
            Logger.WithTag("SourceUri: %v", sourceUri));

        YT_LOG_DEBUG(
            "Confirming attached chunk (SourceUri: %v, ChunkId: %v, ChunkFormat: %lv, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            sourceUri,
            sessionId.ChunkId,
            chunkInfo.ChunkFormat,
            chunkInfo.RowCount,
            chunkInfo.UncompressedDataSize,
            chunkInfo.CompressedDataSize);

        auto replica = TChunkReplicaWithMedium(
            OffshoreNodeId,
            GenericChunkReplicaIndex,
            sessionId.MediumIndex,
            sourceUri);

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, Uploader_->UserObject.ExternalCellTag);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ConfirmChunk();

        // We need to tell master that we are sending source URIs for offshore replicas.
        // Otherwise, it could misinterpret them as "native" offshore replicas on S3 medium,
        // if it supports S3 medium but not source URIs.
        req->RequireServerFeature(EMasterFeature::OffshoreReplicaSourceUri);

        GenerateMutationId(req);

        ToProto(req->mutable_chunk_id(), sessionId.ChunkId);

        // For now, we account the size of the external source file into disk space usage.
        // This may or may not change in the future.
        // NB: This size must match with what is reported in EndUpload.
        req->mutable_chunk_info()->set_disk_space(chunkInfo.CompressedDataSize);

        // This meta is already filtered to only contain extensions stored on master.
        *req->mutable_chunk_meta() = *chunkInfo.MasterChunkMeta;

        auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
            TableWriterOptions_->MemoryUsageTracker,
            req->mutable_chunk_meta()->ByteSize());

        req->set_request_statistics(true);
        req->set_location_uuids_supported(true);

        req->add_legacy_replicas(ToProto<ui64>(replica));

        auto* replicaInfo = req->add_replicas();
        replicaInfo->set_replica(ToProto<ui64>(replica));
        ToProto(replicaInfo->mutable_replica_spec(), replica);
        ToProto(replicaInfo->mutable_location_uuid(), InvalidChunkLocationUuid);

        if (Uploader_->ChunkSchemaId != NullTableSchemaId) {
            ToProto(req->mutable_schema_id(), Uploader_->ChunkSchemaId);
        }

        auto* multicellSyncExt = req->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        multicellSyncExt->set_suppress_upstream_sync(true);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            sessionId.ChunkId);

        YT_LOG_DEBUG("Attached chunk confirmed (SourceUri: %v, ChunkId: %v)", sourceUri, sessionId.ChunkId);

        return {
            .ChunkId = sessionId.ChunkId,
            .RowCount = chunkInfo.RowCount,
            .UncompressedDataSize = chunkInfo.UncompressedDataSize,
            .SourceUri = sourceUri,
            .SourceFormat = chunkInfo.SourceFormat,
            .ChunkFormat = chunkInfo.ChunkFormat,
        };
    }

    void EndUpload()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(Uploader_);

        ValidateAborted();

        auto objectIdPath = FromObjectId(Uploader_->UserObject.ObjectId);

        YT_LOG_DEBUG("Closing table (UploadTransactionId: %v)", Uploader_->UploadTransaction->GetId());

        StopListenTransaction(Uploader_->UploadTransaction);

        auto endUpload = TTableYPathProxy::EndUpload(objectIdPath);
        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(ChunkCount_);
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_compressed_data_size(CompressedDataSize_);
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_);
        dataStatistics.set_regular_disk_space(UncompressedDataSize_);
        // TODO(achulkov2): Retrieve and store data weight properly? Even though it is the same.
        dataStatistics.set_data_weight(UncompressedDataSize_);
        *endUpload->mutable_statistics() = dataStatistics;
        Uploader_->EndUpload(endUpload);

        WaitFor(Transaction_->Commit())
            .ThrowOnError();

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
