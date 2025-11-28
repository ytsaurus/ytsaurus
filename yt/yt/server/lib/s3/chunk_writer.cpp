#include "chunk_writer.h"

#include "public.h"
#include "config.h"

#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/library/s3/client.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NThreading;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////

class TS3UploadSessionBase
    : public TRefCounted
{
public:
    TS3UploadSessionBase(
        IClientPtr client,
        TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        const NLogging::TLogger& logger)
        : Client_(std::move(client))
        , ObjectPlacement_(std::move(objectPlacement))
        , Logger(logger.WithTag("Bucket: %v, Key: %v", ObjectPlacement_.Bucket, ObjectPlacement_.Key))
    { }

    //! Aborts the upload session with the given error.
    //! This will cancel intermediate upload requests in a best-effort manner.
    void SetStateError(const TError& error)
    {
        YT_LOG_ERROR(error, "Error while uploading object to S3");
        StateError_.TrySet(error);
    }

    //! Returns whether the upload was cancelled or already resulted in some sort of error.
    bool IsAborted()
    {
        return StateError_.IsSet();
    }

    //! This future is set when the upload session is aborted for any reason.
    TFuture<void> GetStateFuture()
    {
        return StateError_.ToFuture().ToUncancelable();
    }

    //! This future should be set when the upload session is completed.
    virtual TFuture<void> GetCompletionFuture() const = 0;

    //! Abort upload that is not completed, in there is something
    //! to be done, like for multi-part uploads.
    //! NB: When implementing this method, keep in mind that upload may
    //! have completed since checked before invocation.
    virtual TFuture<void> AbortIncompleteUpload()
    {
        return VoidFuture;
    }

    //! Abort upload that has successfully uploaded the object.
    virtual TFuture<void> AbortCompletedUpload()
    {
        YT_LOG_DEBUG("Deleting uploaded object due to session abort");

        // This method is idempotent and does not throw if the object does not exist.
        return Client_->DeleteObjects(TDeleteObjectsRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Objects = {ObjectPlacement_.Key},
        })
            .AsVoid();
    }

    //! Cancel the upload session.
    //! This method is intended to be best-effort, it is not possible to achieve race-free cancellation.
    //! Some stray parts/objects may remain in S3 and should be removed separately.
    //! Do not call any other methods after this one.
    TFuture<void> Cancel()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (IsAborted()) {
            return VoidFuture;
        }

        YT_LOG_DEBUG("Cancelling S3 upload session");

        SetStateError(TError("S3 upload session cancelled"));

        return IsUploadCompleted()
            ? AbortCompletedUpload()
            : AbortIncompleteUpload();
    }

protected:
    const IClientPtr Client_;
    const TS3MediumDescriptor::TS3ObjectPlacement ObjectPlacement_;
    const NLogging::TLogger Logger;

private:
    //! This promise should never be set successfully, only errors are expected.
    TPromise<void> StateError_ = NewPromise<void>();

    bool IsUploadCompleted()
    {
        auto completionFuture = GetCompletionFuture();
        return completionFuture.IsSet() && completionFuture.Get().IsOK();
    }
};

////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ES3UploadSessionState,
    (Created)
    (Starting)
    (Started)
    (Completing)
    (Completed)
);

class TS3MultiPartUploadSession
    : public TS3UploadSessionBase
{
public:
    struct TOptions
    {
        i64 PartSize = 0;
        i64 UploadWindowSize = 0;
    };

    TS3MultiPartUploadSession(
        IClientPtr client,
        TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        TOptions options,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : TS3UploadSessionBase(
            std::move(client),
            std::move(objectPlacement),
            std::move(logger))
        , Options_(options)
        , Invoker_(std::move(invoker))
        , UploadWindowSemaphore_(New<TAsyncSemaphore>(Options_.UploadWindowSize))
    {
        StartPromise_.TrySetFrom(GetStateFuture());
        CompletePromise_.TrySetFrom(GetStateFuture());
        // This seems safer than using individual callbacks for each part upload future.
        // NB: It is important to specify an invoker here, so that the callback is not
        // executed synchronously in a thread holding our lock.
        GetStateFuture()
            .Subscribe(BIND(&TS3MultiPartUploadSession::CancelPendingUploads, MakeWeak(this)).Via(Invoker_));

        YT_VERIFY(Options_.PartSize >= MinMultiPartUploadPartSize);
        YT_VERIFY(Options_.UploadWindowSize > 0);

        YT_LOG_DEBUG(
            "Created multi-part upload session (MinPartSize: %v, UploadWindowSize: %v)",
            Options_.PartSize,
            Options_.UploadWindowSize);
    }

    TFuture<void> Start()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // There is no need to schedule this callback twice.
        if (TryExchangeState(ES3UploadSessionState::Created, ES3UploadSessionState::Starting)) {
            Invoker_->Invoke(BIND(&TS3MultiPartUploadSession::DoStart, MakeWeak(this)));
        }

        return StartPromise_;
    }

    //! Session must be started before calling Add.
    bool Add(std::vector<TSharedRef> data)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // If we return false, a call to GetReadyEvent will follow which will return the error to the caller.
        if (IsAborted()) {
            return false;
        }

        auto guard = Guard(SpinLock_);

        // You should not add data to a session that has not been started (or that is already completing).
        YT_VERIFY(State_ == ES3UploadSessionState::Started);

        auto size = GetByteSize(data);
        UploadWindowSemaphore_->Acquire(size);

        BufferedData_.insert(BufferedData_.end(), std::make_move_iterator(data.begin()), std::make_move_iterator(data.end()));
        BufferedDataSize_ += size;

        GuardedSchedulePartUploadIfNeeded();

        YT_LOG_DEBUG("Added data to multi-part upload session (Size: %v, BufferedDataSize: %v)",
            size,
            BufferedDataSize_);

        return UploadWindowSemaphore_->IsReady();
    }

    TFuture<void> GetReadyEvent()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto promise = NewPromise<void>();
        promise.TrySetFrom(GetStateFuture());
        promise.TrySetFrom(UploadWindowSemaphore_->GetReadyEvent());
        // Both futures we are setting from are uncancelable, but it is wise to show our intent anyway.
        return promise.ToFuture().ToUncancelable();
    }

    //! After calling this method, no more data can be added to the session.
    TFuture<void> Complete()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // You cannot complete a session that has not been started.
        YT_VERIFY(GetState() != ES3UploadSessionState::Created);

        // We do not need to schedule this callback twice.
        if (TryExchangeState(ES3UploadSessionState::Started, ES3UploadSessionState::Completing)) {
            Invoker_->Invoke(BIND(&TS3MultiPartUploadSession::DoComplete, MakeWeak(this)));
        }

        return CompletePromise_;
    }

    TFuture<void> GetCompletionFuture() const override
    {
        return CompletePromise_;
    }

    //! Returns the current offset within the complete uploaded object.
    //! Used for sanity-checks.
    i64 GetCurrentOffset() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        return CurrentObjectOffset_;
    }

private:
    const TOptions Options_;
    const IInvokerPtr Invoker_;

    TAsyncSemaphorePtr UploadWindowSemaphore_;

    //! Protects the fields below.
    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    //! This state is not an atomic because it lives in the same plane as the data buffer and pending uploads.
    ES3UploadSessionState State_ = ES3UploadSessionState::Created;
    //! Filled after upload is started. Read-only afterwards.
    TString UploadId_;
    //! Stores all part upload futures, even the ones that have already completed.
    //! It is necessary to store all of them complete the upload, actual sent blocks
    //! should not be present here after the corresponding part is uploaded.
    std::vector<TFuture<TUploadPartResponse>> PendingPartUploads_;
    //! Stores the data that is buffered before it forms a new part.
    //! We need it to fulfill the minimum part size requirement.
    std::vector<TSharedRef> BufferedData_;
    //! The total size of the data in the buffer above.
    i64 BufferedDataSize_ = 0;
    //! The current offset in bytes within the whole object being uploaded.
    i64 CurrentObjectOffset_ = 0;

    const TPromise<void> StartPromise_ = NewPromise<void>();
    const TPromise<void> CompletePromise_ = NewPromise<void>();

    ES3UploadSessionState GetState() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        return State_;
    }

    bool TryExchangeState(ES3UploadSessionState expected, ES3UploadSessionState desired)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        if (State_ == expected) {
            State_ = desired;
            return true;
        }
        return false;
    }

    void DoStart()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Starting multi-part upload to S3");

        if (IsAborted()) {
            return;
        }

        // TODO(cherepashka, achulkov2): Cancel this future if the session is aborted.
        auto multiPartUploadOrError = WaitFor(Client_->CreateMultipartUpload(TCreateMultipartUploadRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Key = ObjectPlacement_.Key,
        }));

        if (!multiPartUploadOrError.IsOK()) {
            SetStateError(multiPartUploadOrError);
            return;
        }

        const auto& multiPartUpload = multiPartUploadOrError.Value();

        {
            auto guard = Guard(SpinLock_);

            YT_VERIFY(State_ == ES3UploadSessionState::Starting);

            // At this point, someone may have called cancel already, but it is best to
            // set the upload id and state anyway, so that the session can be properly aborted.

            YT_VERIFY(multiPartUpload.Bucket == ObjectPlacement_.Bucket);
            YT_VERIFY(multiPartUpload.Key == ObjectPlacement_.Key);
            UploadId_ = multiPartUpload.UploadId;

            State_ = ES3UploadSessionState::Started;
        }

        YT_LOG_DEBUG(
            "Multi-part upload started (UploadId: %v)",
            multiPartUpload.UploadId);

        StartPromise_.TrySet();
    }

    void SchedulePartUploadIfNeeded()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        GuardedSchedulePartUploadIfNeeded();
    }

    void GuardedSchedulePartUploadIfNeeded()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(State_ == ES3UploadSessionState::Started || State_ == ES3UploadSessionState::Completing);

        if (!IsAborted() && !BufferedData_.empty() && (BufferedDataSize_ >= Options_.PartSize || State_ == ES3UploadSessionState::Completing)) {
            GuardedSchedulePartUpload();
        }
    }

    void GuardedSchedulePartUpload()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        struct TS3UploadPartTag {};
        auto data = MergeRefsToRef<TS3UploadPartTag>(std::move(BufferedData_));
        BufferedData_.clear();
        BufferedDataSize_ = 0;

        auto md5 = Base64Encode(MD5::CalcRaw(data.ToStringBuf()));
        // Parts are 1-indexed.
        int partIndex = std::ssize(PendingPartUploads_) + 1;
        auto partSize = data.Size();

        YT_LOG_DEBUG(
            "Scheduling part upload (UploadId: %v, PartIndex: %v, ObjectOffset: %v, Size: %v, Md5: %v)",
            UploadId_,
            partIndex,
            CurrentObjectOffset_,
            partSize,
            md5);

        auto uploadFuture = Client_->UploadPart(TUploadPartRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Key = ObjectPlacement_.Key,
            .UploadId = UploadId_,
            .PartIndex = partIndex,
            .Data = std::move(data),
            .ContentMd5 = std::move(md5),
        });

        auto uploadFutureWithHandler = uploadFuture
            .Apply(BIND([weakThis = MakeWeak(this), partIndex, partSize] (const TErrorOr<TUploadPartResponse>& response) {
                if (auto strongThis = weakThis.Lock()) {
                    return strongThis->OnPartUploadCompleted(response, partIndex, partSize);
                }

                // Not sure this can actually happen, but let's be safe.
                THROW_ERROR_EXCEPTION("Multi-part upload session destroyed");
            })
            // We pass the invoker to guard against synchronous invocations of the handler,
            // which may set promises while we are still holding the lock.
            .AsyncVia(Invoker_));

        PendingPartUploads_.push_back(std::move(uploadFutureWithHandler));

        CurrentObjectOffset_ += partSize;
    }

    TUploadPartResponse OnPartUploadCompleted(const TErrorOr<TUploadPartResponse>& response, int partIndex, i64 partSize)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (response.IsOK()) {
            YT_LOG_DEBUG(
                "Part upload completed (PartIndex: %v, PartSize: %v, ETag: %v)",
                partIndex,
                partSize,
                response.Value().ETag);

            UploadWindowSemaphore_->Release(partSize);

            return response.Value();
        }

        auto error = TError("Error uploading part %v", partIndex) << response;
        YT_LOG_ERROR(error);

        // This should lead to cancellation of all other pending uploads.
        // It is intended to be best-effort, some requests may still complete.
        SetStateError(error);

        // This value will not be used, but we need to return something.
        THROW_ERROR error;
    }

    std::vector<TFuture<TUploadPartResponse>> DrainPendingPartUploads()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        std::vector<TFuture<TUploadPartResponse>> pendingPartUploads;

        {
            auto guard = Guard(SpinLock_);
            pendingPartUploads.swap(PendingPartUploads_);
        }

        return pendingPartUploads;
    }

    void CancelPendingUploads(TError error)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // These cancellations should not have synchronous side effects, but we do not like to live dangerously.
        for (auto& partUpload : DrainPendingPartUploads()) {
            partUpload.Cancel(error);
        }
    }

    void DoComplete()
    {
        YT_LOG_DEBUG("Completing multi-part upload to S3");

        // We do not need to do anything if some error was already set, either through
        // session cancellation, or due to an error during one of the previous requests.
        // Completion promise will be set automatically.
        if (IsAborted()) {
            return;
        }

        SchedulePartUploadIfNeeded();

        // Technically, nobody should be writing to the pending uploads vector after the call
        // above, so it is not necessary to use a guarded method, but it does not hurt.
        auto pendingPartUploads = DrainPendingPartUploads();

        std::vector<TCompleteMultipartUploadRequest::TPart> uploadedParts(pendingPartUploads.size());
        for (const auto& [partIndex, partUpload] : Enumerate(pendingPartUploads)) {
            auto partUploadResponseOrError = WaitForFast(partUpload);
            if (!partUploadResponseOrError.IsOK()) {
                // We have already set the session error, which will be propagated to the
                // upload completion promise, so we can just return.
                return;
            }
            uploadedParts[partIndex] = TCompleteMultipartUploadRequest::TPart{
                .PartIndex = static_cast<int>(partIndex) + 1,
                .ETag = partUploadResponseOrError.Value().ETag,
            };
        }

        // There could have been context switches between the last check and this point,
        // so it does not hurt to check again.
        if (IsAborted()) {
            return;
        }

        // TODO(cherepashka, achulkov2): Cancel this future if the session is aborted.
        auto multiPartUploadOrError = WaitFor(Client_->CompleteMultipartUpload(TCompleteMultipartUploadRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Key = ObjectPlacement_.Key,
            .UploadId = UploadId_,
            .Parts = std::move(uploadedParts),
        }));

        if (!multiPartUploadOrError.IsOK()) {
            SetStateError(multiPartUploadOrError);
            return;
        }

        YT_VERIFY(TryExchangeState(ES3UploadSessionState::Completing, ES3UploadSessionState::Completed));

        YT_LOG_DEBUG("Multi-part upload completed (UploadId: %v, ETag: %v)", UploadId_, multiPartUploadOrError.Value().ETag);

        CompletePromise_.TrySet();
    }

    TFuture<void> AbortIncompleteUpload() override
    {
        return BIND(&TS3MultiPartUploadSession::DoAbortUpload, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    void DoAbortUpload()
    {
        {
            auto guard = Guard(SpinLock_);
            // There is no point in aborting if the upload was not started or is already completed.
            // The latter is unlikely because we check for it before running this method.
            if (State_ == ES3UploadSessionState::Created || State_ == ES3UploadSessionState::Starting || State_ == ES3UploadSessionState::Completed) {
                return;
            }
        }

        // Accessing UploadId_ is fine, since the value is read-only after the upload is started.
        YT_VERIFY(!UploadId_.empty());

        YT_LOG_DEBUG("Aborting multi-part upload to S3 (UploadId: %v)", UploadId_);
        WaitFor(Client_->AbortMultipartUpload(TAbortMultipartUploadRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Key = ObjectPlacement_.Key,
            .UploadId = UploadId_,
        }))
            .ValueOrThrow();

        YT_LOG_DEBUG("Multi-part upload aborted");
    }
};

using TS3MultiPartUploadSessionPtr = TIntrusivePtr<TS3MultiPartUploadSession>;

////////////////////////////////////////////////////////////////////////////

class TS3SimpleUploadSession
    : public TS3UploadSessionBase
{
public:
    TS3SimpleUploadSession(
        IClientPtr client,
        TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : TS3UploadSessionBase(
            std::move(client),
            std::move(objectPlacement),
            std::move(logger))
        , Invoker_(std::move(invoker))
    {
        UploadPromise_.TrySetFrom(GetStateFuture());
    }

    TFuture<void> Upload(TSharedRef data)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        Invoker_->Invoke(BIND(&TS3SimpleUploadSession::DoUpload, MakeWeak(this), Passed(std::move(data))));

        return UploadPromise_;
    }

    TFuture<void> GetCompletionFuture() const override
    {
        return UploadPromise_;
    }

private:
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    const TPromise<void> UploadPromise_ = NewPromise<void>();

    void DoUpload(TSharedRef data)
    {
        YT_LOG_DEBUG("Uploading object to S3 (Size: %v)", data.Size());

        if (IsAborted()) {
            return;
        }

        auto putObjectResponse = WaitFor(Client_->PutObject(TPutObjectRequest{
            .Bucket = ObjectPlacement_.Bucket,
            .Key = ObjectPlacement_.Key,
            .Data = std::move(data),
        }));

        if (!putObjectResponse.IsOK()) {
            SetStateError(putObjectResponse);
            return;
        }

        YT_LOG_DEBUG("Object upload completed (ETag: %v)", putObjectResponse.Value().ETag);

        UploadPromise_.TrySet();
    }
};

using TS3ChunkMetaUploadSessionPtr = TIntrusivePtr<TS3SimpleUploadSession>;

////////////////////////////////////////////////////////////////////////////

class TS3Writer
    : public IChunkWriter
{
public:
    TS3Writer(
        IClientPtr client,
        const TS3MediumDescriptorPtr& mediumDescriptor,
        TS3WriterConfigPtr config,
        TSessionId sessionId)
        : Client_(std::move(client))
        , SessionId_(sessionId)
        , Logger(ChunkClientLogger().WithTag("ChunkId: %v", SessionId_.ChunkId))
        , ChunkUploadSession_(New<TS3MultiPartUploadSession>(
            Client_,
            mediumDescriptor->GetChunkPlacement(SessionId_.ChunkId),
            TS3MultiPartUploadSession::TOptions{
                .PartSize = config->UploadPartSize,
                .UploadWindowSize = config->UploadWindowSize,
            },
            TDispatcher::Get()->GetWriterInvoker(),
            Logger))
        , ChunkMetaUploadSession_(New<TS3SimpleUploadSession>(
            Client_,
            mediumDescriptor->GetChunkMetaPlacement(SessionId_.ChunkId),
            TDispatcher::Get()->GetWriterInvoker(),
            Logger))
    { }

    TFuture<void> Open() override
    {
        YT_LOG_INFO("Offshore S3 writer opened");

        return ChunkUploadSession_->Start();
    }

    bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override
    {
        return WriteBlocks(options, workloadDescriptor, {block});
    }

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& /*options*/,
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        const std::vector<TBlock>& blocks) override
    {

        auto writeRequest = SerializeBlocks(DataSize_, blocks, BlocksExt_);
        DataSize_ = writeRequest.EndOffset;
        return ChunkUploadSession_->Add(std::move(writeRequest.Buffers));
    }

    TFuture<void> GetReadyEvent() override
    {
        return ChunkUploadSession_->GetReadyEvent();
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& /*options*/,
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        const TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlockCount) override
    {
        // Journal chunks are not supported.
        YT_VERIFY(chunkMeta);

        if (truncateBlockCount.has_value()) {
            DataSize_ = TruncateBlocks(BlocksExt_, *truncateBlockCount, DataSize_);
        }

        // Some uploads may still be running, but no more blocks can be added, so we can safely
        // finalize the meta in parallel with the completion of the chunk upload itself.
        ChunkMeta_->CopyFrom(*FinalizeChunkMeta(std::move(chunkMeta), BlocksExt_));
        auto chunkMetaBlob = SerializeChunkMeta(GetChunkId(), ChunkMeta_);
        auto closeFutures = std::vector{
            ChunkUploadSession_->Complete(),
            ChunkMetaUploadSession_->Upload(std::move(chunkMetaBlob)),
        };

        return AllSucceeded(std::move(closeFutures));
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ChunkInfo_;
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_UNIMPLEMENTED();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // This method may only be called if the chunk was closed successfully,
        // so we can assume that the upload session was completed.
        TChunkReplicaWithLocation replica(
            OffshoreNodeId,
            GenericChunkReplicaIndex,
            SessionId_.MediumIndex,
            InvalidChunkLocationUuid,
            InvalidChunkLocationIndex);

        return {
            .Replicas = {std::move(replica)},
        };
    }

    TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NErasure::ECodec::None;
    }

    bool IsCloseDemanded() const override
    {
        return false;
    }

    TFuture<void> Cancel() override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IClientPtr Client_;
    const TSessionId SessionId_;

    const NLogging::TLogger Logger;

    const TS3MultiPartUploadSessionPtr ChunkUploadSession_;
    const TS3ChunkMetaUploadSessionPtr ChunkMetaUploadSession_;

    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    i64 DataSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateS3RegularChunkWriter(
    IClientPtr client,
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    TSessionId sessionId)
{
    YT_VERIFY(IsRegularChunkId(sessionId.ChunkId));
    YT_VERIFY(sessionId.MediumIndex == mediumDescriptor->GetIndex());

    return New<TS3Writer>(
        std::move(client),
        std::move(mediumDescriptor),
        std::move(config),
        sessionId);
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
