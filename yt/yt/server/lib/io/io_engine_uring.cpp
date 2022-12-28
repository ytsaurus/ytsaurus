#include "io_engine_uring.h"
#include "io_engine_base.h"
#include "read_request_combiner.h"
#include "private.h"

#include <yt/yt/core/concurrency/notification_handle.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>

#ifdef _linux_
    #include <liburing.h>
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const auto& Logger = IOLogger;

static constexpr int UringEngineNotificationCount = 2;
static constexpr int MaxUringConcurrentRequestsPerThread = 32;

// See SetRequestUserData/GetRequestUserData.
static constexpr int MaxSubrequestCount = 1 << 16;
static constexpr int TypicalSubrequestCount = 64;

// NB: -1 is reserved for LIBURING_UDATA_TIMEOUT.
static constexpr intptr_t StopNotificationUserData = -2;
static constexpr intptr_t RequestNotificationUserData = -3;

static constexpr int RequestNotificationIndex = 1;

DEFINE_ENUM(EUringRequestType,
    (FlushFile)
    (Read)
    (Write)
    (Allocate)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUringIOEngineConfig)

class TUringIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int UringThreadCount;

    //! Limits the number of concurrent (outstanding) #IIOEngine requests per a single uring thread.
    int MaxConcurrentRequestsPerThread;

    int DirectIOPageSize;

    REGISTER_YSON_STRUCT(TUringIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uring_thread_count", &TThis::UringThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("max_concurrent_requests_per_thread", &TThis::MaxConcurrentRequestsPerThread)
            .GreaterThan(0)
            .LessThanOrEqual(MaxUringConcurrentRequestsPerThread)
            .Default(22);

        registrar.Parameter("direct_io_page_size", &TThis::DirectIOPageSize)
            .GreaterThan(0)
            .Default(DefaultPageSize);
    }
};

DEFINE_REFCOUNTED_TYPE(TUringIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TUring
    : private TNonCopyable
{
public:
    explicit TUring(int queueSize)
    {
        auto result = HandleUringEintr(io_uring_queue_init, queueSize, &Uring_, /* flags */ 0);
        if (result < 0) {
            THROW_ERROR_EXCEPTION("Failed to initialize uring")
                << TError::FromSystem(-result);
        }

        CheckUringResult(
            io_uring_ring_dontfork(&Uring_),
            TStringBuf("io_uring_ring_dontfork"));
    }

    ~TUring()
    {
        io_uring_queue_exit(&Uring_);
    }

    TError TryRegisterBuffers(TRange<iovec> iovs)
    {
        int result = HandleUringEintr(io_uring_register_buffers, &Uring_, iovs.Begin(), iovs.Size());
        return result == 0 ? TError() : TError::FromSystem(-result);
    }

    io_uring_cqe* WaitCqe()
    {
        io_uring_cqe* cqe;
        CheckUringResult(
            HandleUringEintr(io_uring_wait_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_wait_cqe"));
        return cqe;
    }

    io_uring_cqe* PeekCqe()
    {
        io_uring_cqe* cqe;
        bool result = ValidateUringNonBlockingResult(
            HandleUringEintr(io_uring_peek_cqe, &Uring_, &cqe),
            TStringBuf("io_uring_peek_cqe"));
        return result ? cqe : nullptr;
    }

    int GetSQSpaceLeft()
    {
        return io_uring_sq_space_left(&Uring_);
    }

    io_uring_sqe* TryGetSqe()
    {
        return io_uring_get_sqe(&Uring_);
    }

    void CqeSeen(io_uring_cqe* cqe)
    {
        io_uring_cqe_seen(&Uring_, cqe);
    }

    int Submit()
    {
        int count = 0;
        while (true) {
            int result = HandleUringEintr(io_uring_submit, &Uring_);
            CheckUringResult(result, TStringBuf("io_uring_submit"));
            if (result == 0) {
                break;
            }
            count += result;
        }
        return count;
    }

private:
    io_uring Uring_;

    template <class F,  class... Args>
    static auto HandleUringEintr(F f, Args&&... args) -> decltype(f(args...))
    {
        while (true) {
            auto result = f(std::forward<Args>(args)...);
            if (result != -EINTR) {
                return result;
            }
        }
    }

    static void CheckUringResult(int result, TStringBuf callName)
    {
        YT_LOG_FATAL_IF(result < 0, TError::FromSystem(-result), "Uring %Qv call failed",
            callName);
    }

    static bool ValidateUringNonBlockingResult(int result, TStringBuf callName)
    {
        if (result == -EAGAIN) {
            return false;
        }
        CheckUringResult(result, callName);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TUringIovBuffer = std::array<iovec, MaxIovCountPerRequest>;

struct TUringRequest
    : public TIntrusiveLinkedListNode<TUringRequest>
{
    struct TRequestToNode
    {
        TIntrusiveLinkedListNode<TUringRequest>* operator() (TUringRequest* request) const {
            return request;
        }
    };

    EUringRequestType Type;
    TIOEngineSensors::TRequestSensors Sensors;

    virtual ~TUringRequest() = 0;

    void StartTimeTracker() {
        RequestTimeGuard_.emplace(Sensors);
    }

    void StopTimeTracker() {
        RequestTimeGuard_.reset();
    }

private:
    std::optional<TRequestStatsGuard> RequestTimeGuard_;
};

TUringRequest::~TUringRequest() { }

using TUringRequestPtr = std::unique_ptr<TUringRequest>;

template <typename TResponse>
struct TUringRequestGeneric
    : public TUringRequest
{
    const TPromise<TResponse> Promise = NewPromise<TResponse>();

    virtual ~TUringRequestGeneric();

    void TrySetSucceeded()
    {
        if (Promise.TrySet()) {
            YT_LOG_TRACE("Request succeeded (Request: %p)",
                this);
        }
    }

    void TrySetFailed(TError error)
    {
        if (Promise.TrySet(std::move(error))) {
            YT_LOG_TRACE(error, "Request failed (Request: %p)",
                this);
        }
    }

    void TrySetFailed(const io_uring_cqe* cqe)
    {
        YT_VERIFY(cqe->res < 0);
        TrySetFailed(TError::FromSystem(-cqe->res));
    }

    void TrySetFinished(const io_uring_cqe* cqe)
    {
        if (cqe->res >= 0) {
            TrySetSucceeded();
        } else {
            TrySetFailed(cqe);
        }
    }
};

template <typename TResponse>
TUringRequestGeneric<TResponse>::~TUringRequestGeneric()
{ }

struct TFlushFileUringRequest
    : public TUringRequestGeneric<void>
{
    IIOEngine::TFlushFileRequest FlushFileRequest;
};

struct TAllocateUringRequest
    : public TUringRequestGeneric<void>
{
    IIOEngine::TAllocateRequest AllocateRequest;
};

struct TWriteUringRequest
    : public TUringRequestGeneric<void>
{
    IIOEngine::TWriteRequest WriteRequest;
    int CurrentWriteSubrequestIndex = 0;
    TUringIovBuffer* WriteIovBuffer = nullptr;

    int FinishedSubrequestCount = 0;
};

struct TReadUringRequest
    : public TUringRequestGeneric<IIOEngine::TReadResponse>
{
    struct TReadSubrequestState
    {
        iovec Iov;
        TMutableRef Buffer;
    };

    std::vector<IIOEngine::TReadRequest> ReadSubrequests;
    TCompactVector<TReadSubrequestState, TypicalSubrequestCount> ReadSubrequestStates;
    TCompactVector<int, TypicalSubrequestCount> PendingReadSubrequestIndexes;
    IReadRequestCombinerPtr ReadRequestCombiner;

    i64 PaddedBytes = 0;
    int FinishedSubrequestCount = 0;

    explicit TReadUringRequest(bool useDedicatedAllocations)
    {
        ReadRequestCombiner = useDedicatedAllocations
            ? CreateDummyReadRequestCombiner()
            : CreateReadRequestCombiner();
    }

    void TrySetReadSucceeded()
    {
        IIOEngine::TReadResponse response{
            .OutputBuffers = std::move(ReadRequestCombiner->ReleaseOutputBuffers()),
            .PaddedBytes = PaddedBytes,
            .IORequests = FinishedSubrequestCount,
        };
        if (Promise.TrySet(std::move(response))) {
            YT_LOG_TRACE("Request succeeded (Request: %p)",
                this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IUringThreadPool
{
    virtual ~IUringThreadPool() {}

    virtual const TString& Name() const = 0;

    virtual TUringRequestPtr TryDequeueRequest() = 0;

    virtual const TNotificationHandle& GetNotificationHandle() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TUringThread
    : public TThread
{
public:
    TUringThread(IUringThreadPool* threadPool, int index, TUringIOEngineConfigPtr config, TIOEngineSensorsPtr sensors)
        : TThread(Format("%v:%v", threadPool->Name(), index))
        , ThreadPool_(threadPool)
        , Config_(std::move(config))
        , Uring_(Config_->MaxConcurrentRequestsPerThread + UringEngineNotificationCount)
        , Sensors_(std::move(sensors))
    {
        InitIovBuffers();
        Start();
    }

private:
    IUringThreadPool* const ThreadPool_;
    const TUringIOEngineConfigPtr Config_;

    TUring Uring_;

    // Linked list of requests that have subrequests yet to be started.
    TIntrusiveLinkedList<TUringRequest, TUringRequest::TRequestToNode> UndersubmittedRequests_;

    int PendingSubmissionsCount_ = 0;
    bool RequestNotificationReadArmed_ = false;

    TNotificationHandle StopNotificationHandle_{true};
    bool Stopping_ = false;

    std::array<TUringIovBuffer, MaxUringConcurrentRequestsPerThread + UringEngineNotificationCount> AllIovBuffers_;
    std::vector<TUringIovBuffer*> FreeIovBuffers_;

    static constexpr int StopNotificationIndex = 0;

    std::array<ui64, UringEngineNotificationCount> NotificationReadBuffer_;
    std::array<iovec, UringEngineNotificationCount> NotificationIov_;

    const TIOEngineSensorsPtr Sensors_;


    void InitIovBuffers()
    {
        FreeIovBuffers_.reserve(AllIovBuffers_.size());
        for (auto& buffer : AllIovBuffers_) {
            FreeIovBuffers_.push_back(&buffer);
        }
    }

    void StopPrologue() override
    {
        StopNotificationHandle_.Raise();
    }

    void ThreadMain() override
    {
        YT_LOG_INFO("Uring thread started");

        ArmStopNotificationRead();
        ArmRequestNotificationRead();
        SubmitSqes();

        do {
            ThreadMainStep();
        } while (!IsUringDrained());

        YT_LOG_INFO("Uring thread stopped");
    }

    bool IsUringDrained()
    {
        return Stopping_ && PendingSubmissionsCount_ == 0;
    }

    void ThreadMainStep()
    {
        io_uring_cqe* cqe = GetCqe(true);
        while (cqe) {
            auto* userData = io_uring_cqe_get_data(cqe);
            if (userData == reinterpret_cast<void*>(StopNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                HandleStop();
            } else if (userData == reinterpret_cast<void*>(RequestNotificationUserData)) {
                YT_VERIFY(cqe->res == sizeof(ui64));
                YT_VERIFY(RequestNotificationReadArmed_);
                RequestNotificationReadArmed_ = false;
            } else {
                HandleCompletion(&*cqe);
            }

            cqe = GetCqe(false);
        }

        while (UndersubmittedRequests_.GetSize() > 0 && CanHandleMoreSubmissions()) {
            YT_LOG_TRACE("Submitting extra request from undersubmitted list.");
            auto* request = UndersubmittedRequests_.GetFront();
            HandleRequest(request);
        }

        HandleSubmissions();
        SubmitSqes();
    }

    TUringRequestPtr TryDequeueRequest()
    {
        auto request = ThreadPool_->TryDequeueRequest();
        if (!request) {
            return nullptr;
        }

        if (Stopping_) {
            YT_LOG_TRACE("Request dropped (Request: %v)",
                request.get());
            return nullptr;
        } else {
            YT_LOG_TRACE("Request dequeued (Request: %v)",
                request.get());
            return request;
        }
    }

    void HandleStop()
    {
        YT_LOG_INFO("Stop received by uring thread (PendingRequestCount: %v)",
            PendingSubmissionsCount_);

        YT_VERIFY(!Stopping_);
        Stopping_ = true;
    }

    bool CanHandleMoreSubmissions()
    {
        bool result = PendingSubmissionsCount_ < Config_->MaxConcurrentRequestsPerThread;

        YT_VERIFY(!result || Uring_.GetSQSpaceLeft() > 0);

        return result;
    }

    void HandleSubmissions()
    {
        while (true) {
            if (!CanHandleMoreSubmissions()) {
                YT_LOG_TRACE("Cannot handle more submissions");
                break;
            }

            auto request = TryDequeueRequest();
            if (!request) {
                break;
            }

            HandleRequest(request.release());
        }

        ArmRequestNotificationRead();
    }

    void HandleRequest(TUringRequest* request)
    {
        switch (request->Type) {
            case EUringRequestType::Read:
                HandleReadRequest(static_cast<TReadUringRequest*>(request));
                break;
            case EUringRequestType::Write:
                HandleWriteRequest(static_cast<TWriteUringRequest*>(request));
                break;
            case EUringRequestType::FlushFile:
                HandleFlushFileRequest(static_cast<TFlushFileUringRequest*>(request));
                break;
            case EUringRequestType::Allocate:
                HandleAllocateRequest(static_cast<TAllocateUringRequest*>(request));
                break;
            default:
                YT_ABORT();
        }
    }

    void HandleReadRequest(TReadUringRequest* request)
    {
        auto totalSubrequestCount = std::ssize(request->ReadSubrequests);

        YT_LOG_TRACE("Handling read request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
            request,
            request->FinishedSubrequestCount,
            totalSubrequestCount);

        if (request->Prev || UndersubmittedRequests_.GetFront() == request) {
            UndersubmittedRequests_.Remove(request);
        }

        if (request->FinishedSubrequestCount == totalSubrequestCount) {
            request->TrySetReadSucceeded();
            DisposeRequest(request);
            return;
        } else if (request->PendingReadSubrequestIndexes.empty()) {
            return;
        }

        YT_VERIFY(CanHandleMoreSubmissions());
        while (!request->PendingReadSubrequestIndexes.empty() && CanHandleMoreSubmissions())
        {
            auto subrequestIndex = request->PendingReadSubrequestIndexes.back();
            request->PendingReadSubrequestIndexes.pop_back();

            const auto& subrequest = request->ReadSubrequests[subrequestIndex];
            auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];
            auto& buffer = subrequestState.Buffer;

            auto* sqe = AllocateSqe();
            subrequestState.Iov = {
                .iov_base = buffer.Begin(),
                .iov_len = Min<size_t>(buffer.Size(), Config_->MaxBytesPerRead)
            };

            YT_LOG_TRACE("Submitting read operation (Request: %p/%v, FD: %v, Offset: %v, Buffer: %p@%v)",
                request,
                subrequestIndex,
                static_cast<FHANDLE>(*subrequest.Handle),
                subrequest.Offset,
                subrequestState.Iov.iov_base,
                subrequestState.Iov.iov_len);

            io_uring_prep_readv(
                sqe,
                *subrequest.Handle,
                &subrequestState.Iov,
                1,
                subrequest.Offset);

            SetRequestUserData(sqe, request, subrequestIndex);
        }

        if (!request->PendingReadSubrequestIndexes.empty()) {
            UndersubmittedRequests_.PushBack(request);
        }
    }

    void HandleWriteRequest(TWriteUringRequest* request)
    {
        auto totalSubrequestCount = std::ssize(request->WriteRequest.Buffers);

        YT_LOG_TRACE("Handling write request (Request: %p, FinishedSubrequestCount: %v, TotalSubrequestCount: %v)",
            request,
            request->FinishedSubrequestCount,
            totalSubrequestCount);

        if (request->CurrentWriteSubrequestIndex == totalSubrequestCount) {
            ReleaseIovBuffer(request->WriteIovBuffer);
            request->TrySetSucceeded();
            DisposeRequest(request);
            return;
        }

        if (!request->WriteIovBuffer) {
            request->WriteIovBuffer = AllocateIovBuffer();
        }

        int iovCount = 0;
        i64 toWrite = 0;
        while (request->CurrentWriteSubrequestIndex + iovCount < totalSubrequestCount &&
            iovCount < std::ssize(*request->WriteIovBuffer) &&
            toWrite < Config_->MaxBytesPerWrite)
        {
            const auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex + iovCount];
            auto& iov = (*request->WriteIovBuffer)[iovCount];
            iov = {
                .iov_base = const_cast<char*>(buffer.Begin()),
                .iov_len = buffer.Size()
            };
            if (toWrite + static_cast<i64>(iov.iov_len) > Config_->MaxBytesPerWrite) {
                iov.iov_len = Config_->MaxBytesPerWrite - toWrite;
            }
            toWrite += iov.iov_len;
            ++iovCount;
        }

        YT_LOG_TRACE("Submitting write operation (Request: %p, FD: %v, Offset: %v, Buffers: %v)",
            request,
            static_cast<FHANDLE>(*request->WriteRequest.Handle),
            request->WriteRequest.Offset,
            MakeFormattableView(
                xrange(request->WriteIovBuffer->begin(), request->WriteIovBuffer->begin() + iovCount),
                [] (auto* builder, const auto* iov) {
                    builder->AppendFormat("%p@%v",
                        iov->iov_base,
                        iov->iov_len);
                }));

        auto* sqe = AllocateSqe();
        io_uring_prep_writev(
            sqe,
            *request->WriteRequest.Handle,
            &request->WriteIovBuffer->front(),
            iovCount,
            request->WriteRequest.Offset);

        SetRequestUserData(sqe, request);
    }

    static ui32 GetSyncFlags(EFlushFileMode mode)
    {
        switch (mode) {
            case EFlushFileMode::All:
                return 0;
            case EFlushFileMode::Data:
                return IORING_FSYNC_DATASYNC;
            default:
                YT_ABORT();
        }
    }

    void HandleFlushFileRequest(TFlushFileUringRequest* request)
    {
        YT_LOG_TRACE("Handling flush file request (Request: %p)",
            request);

        YT_LOG_TRACE("Submitting flush file request (Request: %p, FD: %v, Mode: %v)",
            request,
            static_cast<FHANDLE>(*request->FlushFileRequest.Handle),
            request->FlushFileRequest.Mode);

        auto* sqe = AllocateSqe();
        io_uring_prep_fsync(sqe, *request->FlushFileRequest.Handle, GetSyncFlags(request->FlushFileRequest.Mode));
        SetRequestUserData(sqe, request);
    }

    void HandleAllocateRequest(TAllocateUringRequest* request)
    {
        YT_LOG_TRACE("Handling allocate request (Request: %p)",
            request);

        YT_LOG_TRACE("Submitting allocate request (Request: %p, FD: %v, Size: %v)",
            request,
            static_cast<FHANDLE>(*request->AllocateRequest.Handle),
            request->AllocateRequest.Size);

        auto* sqe = AllocateSqe();
        io_uring_prep_fallocate(sqe, *request->AllocateRequest.Handle, FALLOC_FL_CONVERT_UNWRITTEN, 0, request->AllocateRequest.Size);
        SetRequestUserData(sqe, request);
    }


    void HandleCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TUringRequest>(cqe);
        request->StopTimeTracker();
        switch (request->Type) {
            case EUringRequestType::Read:
                HandleReadCompletion(cqe);
                break;
            case EUringRequestType::Write:
                HandleWriteCompletion(cqe);
                break;
            case EUringRequestType::FlushFile:
                HandleFlushFileCompletion(cqe);
                break;
            case EUringRequestType::Allocate:
                HandleAllocateCompletion(cqe);
                break;
            default:
                YT_ABORT();
        }
    }

    void HandleReadCompletion(const io_uring_cqe* cqe)
    {
        auto [request, subrequestIndex] = GetRequestUserData<TReadUringRequest>(cqe);

        YT_LOG_TRACE("Handling read completion (Request: %p/%v)",
            request,
            subrequestIndex);

        auto& subrequest = request->ReadSubrequests[subrequestIndex];
        auto& subrequestState = request->ReadSubrequestStates[subrequestIndex];

        if (cqe->res == 0 && subrequestState.Buffer.Size() > 0) {
            auto error = request->ReadRequestCombiner->CheckEof(subrequestState.Buffer);
            if (!error.IsOK()) {
                YT_LOG_TRACE("Read subrequest failed at EOF (Request: %p/%v, Remaining: %v)",
                    request,
                    subrequestIndex,
                    subrequestState.Buffer.Size());
                request->TrySetFailed(std::move(error));
            } else {
                YT_LOG_TRACE("Read subrequest succeeded at EOF (Request: %p/%v, Remaining: %v)",
                    request,
                    subrequestIndex,
                    subrequestState.Buffer.Size());
            }
            subrequestState.Buffer = {};
            ++request->FinishedSubrequestCount;
        } else if (cqe->res < 0) {
            ++request->FinishedSubrequestCount;
            request->TrySetFailed(cqe);
        } else {
            i64 readSize = cqe->res;
            if (Config_->SimulatedMaxBytesPerRead) {
                readSize = Min(readSize, *Config_->SimulatedMaxBytesPerRead);
            }
            Sensors_->RegisterReadBytes(readSize);
            auto bufferSize = static_cast<i64>(subrequestState.Buffer.Size());
            subrequest.Offset += readSize;
            if (bufferSize == readSize) {
                YT_LOG_TRACE("Read subrequest fully succeeded (Request: %p/%v, Size: %v)",
                    request,
                    subrequestIndex,
                    readSize);
                subrequestState.Buffer = {};
                ++request->FinishedSubrequestCount;
            } else {
                YT_LOG_TRACE("Read subrequest partially succeeded (Request: %p/%v, Size: %v)",
                    request,
                    subrequestIndex,
                    readSize);
                subrequestState.Buffer = subrequestState.Buffer.Slice(readSize, bufferSize);
                request->PendingReadSubrequestIndexes.push_back(subrequestIndex);
            }
        }

        HandleReadRequest(request);
    }

    void HandleWriteCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TWriteUringRequest>(cqe);

        YT_LOG_TRACE("Handling write completion (Request: %p)",
            request);

        if (cqe->res < 0) {
            request->TrySetFailed(cqe);
            DisposeRequest(request);
            return;
        }

        i64 writtenSize = cqe->res;
        if (Config_->SimulatedMaxBytesPerWrite) {
            writtenSize = Min(writtenSize, *Config_->SimulatedMaxBytesPerWrite);
        }
        Sensors_->RegisterWrittenBytes(writtenSize);
        request->WriteRequest.Offset += writtenSize;

        while (writtenSize > 0) {
            auto& buffer = request->WriteRequest.Buffers[request->CurrentWriteSubrequestIndex];
            auto bufferSize = static_cast<i64>(buffer.Size());
            if (bufferSize <= writtenSize) {
                YT_LOG_TRACE("Write subrequest fully succeeded (Request: %p/%v, Size: %v)",
                    request,
                    request->CurrentWriteSubrequestIndex,
                    writtenSize);
                writtenSize -= bufferSize;
                buffer = {};
                ++request->CurrentWriteSubrequestIndex;
            } else {
                YT_LOG_TRACE("Write subrequest partially succeeded (Request: %p/%v, Size: %v)",
                    request,
                    request->CurrentWriteSubrequestIndex,
                    writtenSize);
                buffer = buffer.Slice(writtenSize, bufferSize);
                writtenSize = 0;
            }
        }

        HandleWriteRequest(request);
    }

    void HandleFlushFileCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TFlushFileUringRequest>(cqe);

        YT_LOG_TRACE("Handling sync completion (Request: %p)",
            request);

        request->TrySetFinished(cqe);
        DisposeRequest(request);
    }

    void HandleAllocateCompletion(const io_uring_cqe* cqe)
    {
        auto [request, _] = GetRequestUserData<TAllocateUringRequest>(cqe);

        YT_LOG_TRACE("Handling allocate completion (Request: %p)",
            request);

        request->TrySetFinished(cqe);
        DisposeRequest(request);
    }


    void ArmNotificationRead(
        int notificationIndex,
        const TNotificationHandle& notificationHandle,
        intptr_t notificationUserData)
    {
        YT_VERIFY(notificationIndex >= 0);
        YT_VERIFY(notificationIndex < std::ssize(NotificationIov_));
        YT_VERIFY(notificationIndex < std::ssize(NotificationReadBuffer_));

        auto iov = &NotificationIov_[notificationIndex];
        *iov = {
            .iov_base = &NotificationReadBuffer_[notificationIndex],
            .iov_len = sizeof(NotificationReadBuffer_[notificationIndex])
        };

        auto* sqe = AllocateNonRequestSqe();
        io_uring_prep_readv(sqe, notificationHandle.GetFD(), iov, 1, 0);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(notificationUserData));
    }

    void ArmStopNotificationRead()
    {
        YT_LOG_TRACE("Arming stop notification read");
        ArmNotificationRead(StopNotificationIndex, StopNotificationHandle_, StopNotificationUserData);
    }

    void ArmRequestNotificationRead()
    {
        if (!RequestNotificationReadArmed_ &&
            !Stopping_ &&
            CanHandleMoreSubmissions())
        {
            RequestNotificationReadArmed_ = true;
            YT_LOG_TRACE("Arming request notification read");
            ArmNotificationRead(RequestNotificationIndex, ThreadPool_->GetNotificationHandle(), RequestNotificationUserData);
        }
    }

    TUringIovBuffer* AllocateIovBuffer()
    {
        YT_VERIFY(!FreeIovBuffers_.empty());
        auto* result = FreeIovBuffers_.back();
        FreeIovBuffers_.pop_back();
        return result;
    }

    void ReleaseIovBuffer(TUringIovBuffer* buffer)
    {
        if (buffer) {
            FreeIovBuffers_.push_back(buffer);
        }
    }

    io_uring_cqe* GetCqe(bool wait)
    {
        io_uring_cqe* cqe = wait ? Uring_.WaitCqe() : Uring_.PeekCqe();
        if (cqe == nullptr) {
            YT_VERIFY(!wait);
            return nullptr;
        }
        auto userData = reinterpret_cast<intptr_t>(io_uring_cqe_get_data(cqe));
        if (userData != StopNotificationUserData && userData != RequestNotificationUserData) {
            --PendingSubmissionsCount_;
        }
        YT_VERIFY(PendingSubmissionsCount_ >= 0);
        YT_LOG_TRACE("CQE received (PendingRequestCount: %v)",
            PendingSubmissionsCount_);

        Uring_.CqeSeen(cqe);
        return cqe;
    }

    io_uring_sqe* AllocateNonRequestSqe()
    {
        auto* sqe = Uring_.TryGetSqe();
        YT_VERIFY(sqe);
        return sqe;
    }

    io_uring_sqe* AllocateSqe()
    {
        auto* sqe = Uring_.TryGetSqe();
        YT_VERIFY(sqe);
        PendingSubmissionsCount_++;
        return sqe;
    }

    void SubmitSqes()
    {
        int count = 0;
        {
            TRequestStatsGuard statsGuard(Sensors_->IoSubmitSensors);
            count = Uring_.Submit();
        }
        if (count > 0) {
            YT_LOG_TRACE("SQEs submitted (SqeCount: %v, PendingRequestCount: %v)",
                count,
                PendingSubmissionsCount_);
        }
    }

    static void SetRequestUserData(io_uring_sqe* sqe, TUringRequest* request, int subrequestIndex = 0)
    {
        request->StartTimeTracker();
        auto userData = reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(request) |
            (static_cast<uintptr_t>(subrequestIndex) << 48));
        io_uring_sqe_set_data(sqe, userData);
    }

    template <typename TUringRequest>
    static std::tuple<TUringRequest*, int> GetRequestUserData(const io_uring_cqe* cqe)
    {
        constexpr ui64 requestMask = (1ULL << 48) - 1;
        auto userData = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
        return {
            reinterpret_cast<TUringRequest*>(userData & requestMask),
            userData >> 48
        };
    }

    void DisposeRequest(TUringRequest* request)
    {
        YT_LOG_TRACE("Request disposed (Request: %v)",
            request);
        delete request;
    }
};

class TUringThreadPool
    : public IUringThreadPool
{
public:
    TUringThreadPool(
        TString threadNamePrefix,
        TUringIOEngineConfigPtr config,
        TIOEngineSensorsPtr sensors)
        : Config_(std::move(config))
        , ThreadNamePrefix_(std::move(threadNamePrefix))
        , Sensors_(std::move(sensors))
        , Threads_(Config_->UringThreadCount)
    {
        StartThreads();
    }

    ~TUringThreadPool()
    {
        StopThreads();
    }

    virtual const TString& Name() const override
    {
        return ThreadNamePrefix_;
    }

    virtual TUringRequestPtr TryDequeueRequest() override
    {
        RequestNotificationHandleRaised_.store(false);

        TUringRequestPtr request;
        RequestQueue_.try_dequeue(request);

        return request;
    }

    virtual const TNotificationHandle& GetNotificationHandle() const override
    {
        return RequestNotificationHandle_;
    }

    void Configure(const TUringIOEngineConfigPtr& config)
    {
        Y_UNUSED(config);
    }

    void SubmitRequest(TUringRequestPtr request)
    {
        YT_LOG_TRACE("Request enqueued (Request: %v, Type: %v)",
            request.get(),
            request->Type);

        RequestQueue_.enqueue(std::move(request));

        if (!RequestNotificationHandleRaised_.exchange(true)) {
            RequestNotificationHandle_.Raise();
        }
    }

private:
    const TUringIOEngineConfigPtr Config_;
    const TString ThreadNamePrefix_;
    const TIOEngineSensorsPtr Sensors_;

    using TUringThreadPtr = TIntrusivePtr<TUringThread>;

    std::vector<TUringThreadPtr> Threads_;

    TNotificationHandle RequestNotificationHandle_{true};
    std::atomic<bool> RequestNotificationHandleRaised_ = false;
    moodycamel::ConcurrentQueue<TUringRequestPtr> RequestQueue_;


    void StartThreads()
    {
        for (int threadIndex = 0; threadIndex < Config_->UringThreadCount; ++threadIndex) {
            Threads_[threadIndex] = New<TUringThread>(this, threadIndex, Config_, Sensors_);
        }
    }

    void StopThreads()
    {
        for (const auto& thread : Threads_) {
            thread->Stop();
        }
    }
};

using TUringThreadPoolPtr = std::unique_ptr<TUringThreadPool>;

////////////////////////////////////////////////////////////////////////////////

class TUringIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TUringIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TUringIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , ThreadPool_(std::make_unique<TUringThreadPool>(
            Format("IOU:%v", LocationId_),
            StaticConfig_,
            Sensors_))
    { }

    ~TUringIOEngine()
    {
        GetFinalizerInvoker()->Invoke(BIND([threadPool = std::move(ThreadPool_)] () mutable {
            threadPool.reset();
        }));
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory /*category*/,
        TRefCountedTypeCookie tagCookie,
        TSessionId /*sessionId*/,
        bool useDedicatedAllocations) override
    {
        if (std::ssize(requests) > MaxSubrequestCount) {
            return MakeFuture<TReadResponse>(TError("Too many read requests: %v > %v",
                requests.size(),
                MaxSubrequestCount));
        }

        auto uringRequest = std::make_unique<TReadUringRequest>(useDedicatedAllocations);

        auto combinedRequests = uringRequest->ReadRequestCombiner->Combine(
            std::move(requests),
            StaticConfig_->DirectIOPageSize,
            tagCookie);

        uringRequest->Type = EUringRequestType::Read;
        uringRequest->Sensors = Sensors_->ReadSensors;
        uringRequest->ReadSubrequests.reserve(combinedRequests.size());
        uringRequest->ReadSubrequestStates.reserve(combinedRequests.size());
        uringRequest->PendingReadSubrequestIndexes.reserve(combinedRequests.size());

        for (int index = 0; index < std::ssize(combinedRequests); ++index) {
            const auto& ioRequest = combinedRequests[index].ReadRequest;
            uringRequest->PaddedBytes += GetPaddedSize(
                ioRequest.Offset,
                ioRequest.Size,
                ioRequest.Handle->IsOpenForDirectIO() ? StaticConfig_->DirectIOPageSize : DefaultPageSize);
            uringRequest->ReadSubrequests.push_back({
                .Handle = std::move(ioRequest.Handle),
                .Offset = ioRequest.Offset,
                .Size = ioRequest.Size
            });
            uringRequest->ReadSubrequestStates.push_back({
                .Buffer = std::move(combinedRequests[index].ResultBuffer)
            });
            uringRequest->PendingReadSubrequestIndexes.push_back(index);
        }

        return SubmitRequest<TReadResponse>(std::move(uringRequest));
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory /*category*/,
        TSessionId) override
    {
        auto uringRequest = std::make_unique<TWriteUringRequest>();
        uringRequest->Type = EUringRequestType::Write;
        uringRequest->Sensors = Sensors_->WriteSensors;
        uringRequest->WriteRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory /*category*/) override
    {
        auto uringRequest = std::make_unique<TFlushFileUringRequest>();
        uringRequest->Type = EUringRequestType::FlushFile;
        uringRequest->Sensors = Sensors_->SyncSensors;
        uringRequest->FlushFileRequest = std::move(request);
        return SubmitRequest<void>(std::move(uringRequest));
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest /*request*/,
        EWorkloadCategory /*category*/,
        TSessionId /*sessionId*/) override
    {
        // TODO (capone212): implement
        return VoidFuture;
    }

private:
    const TConfigPtr StaticConfig_;

    TUringThreadPoolPtr ThreadPool_;


    template <typename TUringResponse, typename TUringRequest>
    TFuture<TUringResponse> SubmitRequest(TUringRequest request)
    {
        auto future = request->Promise.ToFuture();
        ThreadPool_->SubmitRequest(std::move(request));
        return future;
    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);

        ThreadPool_->Configure(config);
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngineUring(
    NYTree::INodePtr ioConfig,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)

{
#ifdef _linux_

    return CreateIOEngine<TUringIOEngine>(
        std::move(ioConfig),
        std::move(locationId),
        std::move(profiler),
        std::move(logger));

#endif
    return {};
}

////////////////////////////////////////////////////////////////////////////////

bool IsUringIOEngineSupported()
{
#ifdef _linux_

    io_uring uring;
    auto result = io_uring_queue_init(1, &uring, /* flags */ 0);
    if (result < 0) {
        return false;
    }
    io_uring_queue_exit(&uring);
    return true;

#endif
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
