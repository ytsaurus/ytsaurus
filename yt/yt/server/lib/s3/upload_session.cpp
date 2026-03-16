#include "upload_session.h"

#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////

TS3UploadSessionBase::TS3UploadSessionBase(
    IClientPtr client,
    TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
    : Client_(std::move(client))
    , ObjectPlacement_(std::move(objectPlacement))
    , Logger(logger.WithTag("Bucket: %v, Key: %v", ObjectPlacement_.Bucket, ObjectPlacement_.Key))
    , UnderlyingInvoker_(std::move(invoker))
    , UploadSessionCancelableContext_(New<TCancelableContext>())
{
    CancelableInvoker_ = UploadSessionCancelableContext_->CreateInvoker(UnderlyingInvoker_);
}

TFuture<void> TS3UploadSessionBase::GetCompletionFuture() const
{
    return CompletionPromise_.ToFuture().ToUncancelable();
}

TFuture<void> TS3UploadSessionBase::Abort(TError error)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UploadSessionCancelableContext_->Cancel(error);

    CompletionPromise_.TrySet(error);

    auto completed = IsUploadCompleted();
    YT_LOG_DEBUG("Aborting S3 upload session (State: %v)", completed);
    return completed
        ? AbortCompletedUpload()
        : AbortIncompleteUpload();
}

bool TS3UploadSessionBase::IsUploadCompleted() const
{
    auto completionFuture = GetCompletionFuture();
    return completionFuture.IsSet() && completionFuture.GetOrCrash().IsOK();
}

TFuture<void> TS3UploadSessionBase::AbortCompletedUpload()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Deleting uploaded object due to session abort");

    // This method is idempotent and does not throw if the object does not exist.
    return Client_->DeleteObjects(TDeleteObjectsRequest{
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Objects = {TString(ObjectPlacement_.Key)},
    })
        .AsVoid();
}

////////////////////////////////////////////////////////////////////////////

TS3MultiPartUploadSession::TS3MultiPartUploadSession(
    IClientPtr client,
    TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
    TOptions options,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
    : TS3UploadSessionBase(
        std::move(client),
        std::move(objectPlacement),
        std::move(invoker),
        std::move(logger))
    , Options_(options)
    , UploadWindowSemaphore_(New<TAsyncSemaphore>(Options_.UploadWindowSize))
{
    StartPromise_.TrySetFrom(GetCompletionFuture());

    YT_VERIFY(Options_.PartSize >= MinMultiPartUploadPartSize);
    YT_VERIFY(Options_.UploadWindowSize > 0);

    YT_LOG_DEBUG(
        "Created multi-part upload session (MinPartSize: %v, UploadWindowSize: %v)",
        Options_.PartSize,
        Options_.UploadWindowSize);
}

TFuture<void> TS3MultiPartUploadSession::Start()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    // One multipart upload session can be used only once.
    if (TryExchangeState(ES3UploadSessionState::Created, ES3UploadSessionState::Starting)) {
        UploadSessionCancelableContext_ = New<TCancelableContext>();
        CancelableInvoker_ = UploadSessionCancelableContext_->CreateInvoker(UnderlyingInvoker_);
        CancelableInvoker_->Invoke(BIND(&TS3MultiPartUploadSession::DoStart, MakeWeak(this)));
    }

    return StartPromise_;
}

bool TS3MultiPartUploadSession::Add(std::vector<TSharedRef> data)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);

    // You should not add data to a session that has not been started (or that is already completing or aborting).
    if (State_ != ES3UploadSessionState::Started) {
        return false;
    }

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

TFuture<void> TS3MultiPartUploadSession::Complete()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    THROW_ERROR_EXCEPTION_IF(
        GetState() == ES3UploadSessionState::Created,
        "Attempted to complete multipart upload session which has not been started");

    // We do not need to schedule this callback twice.
    if (TryExchangeState(ES3UploadSessionState::Started, ES3UploadSessionState::Completing)) {
        CancelableInvoker_->Invoke(BIND(&TS3MultiPartUploadSession::DoComplete, MakeWeak(this)));
    }

    return CompletionPromise_;
}

TFuture<void> TS3MultiPartUploadSession::Abort(TError error)
{
    {
        auto guard = WriterGuard(SpinLock_);
        State_ = ES3UploadSessionState::Aborting;
    }
    return TS3UploadSessionBase::Abort(error)
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            YT_VERIFY(TryExchangeState(ES3UploadSessionState::Aborting, ES3UploadSessionState::Aborted));
        }));
}

TFuture<void> TS3MultiPartUploadSession::GetReadyEvent()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    ReadyEventPromise_.TrySetFrom(GetCompletionFuture());
    ReadyEventPromise_.TrySetFrom(UploadWindowSemaphore_->GetReadyEvent());
    // Futures, that we are setting from, are uncancelable, but it is wise to show our intent anyway.
    return ReadyEventPromise_.ToFuture().ToUncancelable();
}

ES3UploadSessionState TS3MultiPartUploadSession::GetState() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);
    return State_;
}

bool TS3MultiPartUploadSession::TryExchangeState(ES3UploadSessionState expected, ES3UploadSessionState desired)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);
    if (State_ == expected) {
        State_ = desired;
        return true;
    }
    return false;
}

void TS3MultiPartUploadSession::SchedulePartUploadIfNeeded()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);
    GuardedSchedulePartUploadIfNeeded();
}

void TS3MultiPartUploadSession::GuardedSchedulePartUploadIfNeeded()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(SpinLock_);

    YT_VERIFY(State_ == ES3UploadSessionState::Started || State_ == ES3UploadSessionState::Completing);

    if (!BufferedData_.empty() && (BufferedDataSize_ >= Options_.PartSize || State_ == ES3UploadSessionState::Completing)) {
        GuardedSchedulePartUpload();
    }
}

void TS3MultiPartUploadSession::GuardedSchedulePartUpload()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(SpinLock_);

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
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Key = TString(ObjectPlacement_.Key),
        .UploadId = UploadId_,
        .PartIndex = partIndex,
        .Data = std::move(data),
        .ContentMd5 = std::move(md5),
    }).Apply(BIND([weakThis = MakeWeak(this), partIndex, partSize] (const TErrorOr<TUploadPartResponse>& response) {
            if (auto strongThis = weakThis.Lock()) {
                return strongThis->OnPartUploadCompleted(response, partIndex, partSize);
            }

            // Not sure this can actually happen, but let's be safe.
            THROW_ERROR_EXCEPTION("Multi-part upload session destroyed");
        })
        // We pass the invoker to guard against synchronous invocations of the handler,
        // which may set promises while we are still holding the lock.
        .AsyncVia(CancelableInvoker_));

    PendingPartUploads_.push_back(std::move(uploadFuture));

    CurrentObjectOffset_ += partSize;
}

TUploadPartResponse TS3MultiPartUploadSession::OnPartUploadCompleted(const TErrorOr<TUploadPartResponse>& response, int partIndex, i64 partSize)
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
    Y_UNUSED(Abort(error));

    // This value will not be used, but we need to return something.
    THROW_ERROR_EXCEPTION(error);
}

std::vector<TFuture<TUploadPartResponse>> TS3MultiPartUploadSession::DrainPendingPartUploads()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    std::vector<TFuture<TUploadPartResponse>> pendingPartUploads;

    {
        auto guard = WriterGuard(SpinLock_);
        pendingPartUploads.swap(PendingPartUploads_);
    }

    return pendingPartUploads;
}

void TS3MultiPartUploadSession::DoStart()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting multi-part upload to S3");

    auto multiPartUploadOrError = WaitFor(Client_->CreateMultipartUpload(TCreateMultipartUploadRequest{
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Key = TString(ObjectPlacement_.Key),
    }));

    if (!multiPartUploadOrError.IsOK()) {
        Y_UNUSED(Abort(multiPartUploadOrError));
        return;
    }

    const auto& multiPartUpload = multiPartUploadOrError.Value();

    {
        auto guard = WriterGuard(SpinLock_);

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

void TS3MultiPartUploadSession::DoComplete()
{
    YT_LOG_DEBUG("Completing multi-part upload to S3");

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

    auto multiPartUploadOrError = WaitFor(Client_->CompleteMultipartUpload(TCompleteMultipartUploadRequest{
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Key = TString(ObjectPlacement_.Key),
        .UploadId = UploadId_,
        .Parts = std::move(uploadedParts),
    }));

    if (!multiPartUploadOrError.IsOK()) {
        Y_UNUSED(Abort(multiPartUploadOrError));
        return;
    }

    if (TryExchangeState(ES3UploadSessionState::Completing, ES3UploadSessionState::Completed)) {
        YT_LOG_DEBUG("Multi-part upload completed (UploadId: %v, ETag: %v)", UploadId_, multiPartUploadOrError.Value().ETag);

        CompletionPromise_.Set();
    }
}

void TS3MultiPartUploadSession::DoAbortIncompleteUpload()
{
    {
        auto guard = ReaderGuard(SpinLock_);
        // There is no point in aborting if the upload was not started or is already completed.
        // The latter is unlikely because we check for it before running this method.
        if (State_ == ES3UploadSessionState::Created || State_ == ES3UploadSessionState::Starting || State_ == ES3UploadSessionState::Completed) {
            return;
        }
    }

    // Accessing UploadId_ is fine, since the value is read-only after the upload is started.
    YT_VERIFY(!UploadId_.empty());

    YT_LOG_DEBUG("Aborting incomplete multi-part upload to S3 (UploadId: %v)", UploadId_);
    WaitFor(Client_->AbortMultipartUpload(TAbortMultipartUploadRequest{
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Key = TString(ObjectPlacement_.Key),
        .UploadId = UploadId_,
    }))
        .ValueOrThrow();

    YT_LOG_DEBUG("Incomplete multi-part upload aborted");
}

TFuture<void> TS3MultiPartUploadSession::AbortIncompleteUpload()
{
    return BIND(&TS3MultiPartUploadSession::DoAbortIncompleteUpload, MakeWeak(this))
        .AsyncVia(UnderlyingInvoker_)
        .Run();
}

////////////////////////////////////////////////////////////////////////////

TS3SimpleUploadSession::TS3SimpleUploadSession(
    IClientPtr client,
    TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
    : TS3UploadSessionBase(
        std::move(client),
        std::move(objectPlacement),
        std::move(invoker),
        std::move(logger))
{
    YT_LOG_DEBUG("Created simple upload session");
}

TFuture<void> TS3SimpleUploadSession::Upload(TSharedRef data)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    CancelableInvoker_->Invoke(BIND(&TS3SimpleUploadSession::DoUpload, MakeWeak(this), Passed(std::move(data))));

    return GetCompletionFuture();
}

void TS3SimpleUploadSession::DoUpload(TSharedRef data)
{
    YT_LOG_DEBUG("Uploading object to S3 (Size: %v)", data.Size());

    auto putObjectResponse = WaitFor(Client_->PutObject(TPutObjectRequest{
        .Bucket = TString(ObjectPlacement_.Bucket),
        .Key = TString(ObjectPlacement_.Key),
        .Data = std::move(data),
    }));

    if (!putObjectResponse.IsOK()) {
        Y_UNUSED(Abort(putObjectResponse));
        return;
    }

    YT_LOG_DEBUG("Object upload completed (ETag: %v)", putObjectResponse.Value().ETag);

    CompletionPromise_.TrySet();
}

TFuture<void> TS3SimpleUploadSession::AbortIncompleteUpload()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return TFuture<void>();
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
