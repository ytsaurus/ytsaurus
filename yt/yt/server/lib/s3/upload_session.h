#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt/yt/library/s3/client.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////

//! Base class for session responsible for uploading data into S3 storage.
// * thread affinity: invoker
class TS3UploadSessionBase
    : public TRefCounted
{
public:
    TS3UploadSessionBase(
        IClientPtr client,
        NChunkClient::TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger);

    //! Returns future that should become set when the upload session is completed.
    TFuture<void> GetCompletionFuture() const;

    //! Aborts the upload session.
    //! This method is intended to be best-effort, it is not possible to achieve race-free cancellation.
    //! Some stray parts/objects may remain in S3 and should be removed separately.
    //! Do not call any other methods after this one.
    TFuture<void> Abort(TError error);

    //! Returns true if upload session was completed successfuly.
    bool IsUploadCompleted() const;

protected:
    const IClientPtr Client_;
    const NChunkClient::TS3MediumDescriptor::TS3ObjectPlacement ObjectPlacement_;
    const NLogging::TLogger Logger;
    const IInvokerPtr UnderlyingInvoker_;

    TCancelableContextPtr UploadSessionCancelableContext_;

    IInvokerPtr CancelableInvoker_;
    TPromise<void> CompletionPromise_ = NewPromise<void>();

    //! Abort upload that is not completed, and there is something
    //! to be done, like for multi-part uploads.
    //! NB: When implementing this method in class inheritor, keep in mind that upload may
    //! have completed since checked before invocation.
    virtual TFuture<void> AbortIncompleteUpload();

    //! Abort upload that has successfully uploaded the object.
    virtual TFuture<void> AbortCompletedUpload();
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
        NChunkClient::TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        TOptions options,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    //! Starts multipart upload session.
    //! Multiple call of this method will cancel all uploads that were done before and were not completed via Complete method.
    TFuture<void> Start();

    //! Adds data into uploading databuffer and schedules flush to S3 storage if needed.
    //! Session must be started before calling this method.
    bool Add(std::vector<TSharedRef> data);

    //! Finalizes upload session by sending buffered data to S3 storage.
    //! After calling this method, no more data can be added to the session.
    TFuture<void> Complete();

    TFuture<void> GetReadyEvent();

private:
    const TOptions Options_;
    NConcurrency::TAsyncSemaphorePtr UploadWindowSemaphore_;

    //! Protects the fields below.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
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

    ES3UploadSessionState GetState() const;

    bool TryExchangeState(ES3UploadSessionState expected, ES3UploadSessionState desired);

    void SchedulePartUploadIfNeeded();
    void GuardedSchedulePartUploadIfNeeded();
    void GuardedSchedulePartUpload();

    TUploadPartResponse OnPartUploadCompleted(const TErrorOr<TUploadPartResponse>& response, int partIndex, i64 partSize);

    std::vector<TFuture<TUploadPartResponse>> DrainPendingPartUploads();

    void DoStart();
    void DoComplete();
    void DoAbortIncompleteUpload();

    TFuture<void> AbortIncompleteUpload() override;
};

DEFINE_REFCOUNTED_TYPE(TS3MultiPartUploadSession);

////////////////////////////////////////////////////////////////////////////

class TS3SimpleUploadSession
    : public TS3UploadSessionBase
{
public:
    TS3SimpleUploadSession(
        IClientPtr client,
        NChunkClient::TS3MediumDescriptor::TS3ObjectPlacement objectPlacement,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    //! Uploads data into S3 storage.
    TFuture<void> Upload(TSharedRef data);

private:
    void DoUpload(TSharedRef data);
};

DEFINE_REFCOUNTED_TYPE(TS3SimpleUploadSession);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3