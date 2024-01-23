#include "driver.h"
#include "meters.h"
#include "pread.h"
#include "linuxaio.h"
#include "uring.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

class TDriverBase
    : public IDriver
{
public:
    explicit TDriverBase(TInstant start)
        : Statistics_(start)
    { }

    void Oneshot(TOperationGenerator generator) override
    {
        auto operation = generator();
        TLatencyMeter latency;
        DoOperation(operation);
        Statistics_.Update(operation, latency.Tick());
        Statistics_.Finish();
    }

    void Burst(TOperationGenerator generator) override
    {
        TLatencyMeter latency;
        TOperation operation;
        do {
            operation = generator();
            if (operation.BurstAction != EBurstAction::Continue) {
                continue;
            }
            latency.Tick();
            DoOperation(operation);
            Statistics_.Update(operation, latency.Tick());
        } while (operation);
        Statistics_.Finish();
    }

    TStatistics GetStatistics() override
    {
        return Statistics_;
    }

protected:
    TStatistics Statistics_;

    virtual void DoOperation(const TOperation& operation) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMemcpyDriver
    : public TDriverBase
{
public:
    explicit TMemcpyDriver(TMemcpyDriverOptions options)
        : TDriverBase(options.Start)
        , Options_(options)
        , Buffer_(options.MaxBlockSize / sizeof(ui64))
        , Data_(DataSize_ / sizeof(ui64))
    {
        for (ui64 i = 0; i < Data_.size(); ++i) {
            Data_[i] = i;
        }
        for (ui64 i = 0; i < Buffer_.size(); ++i) {
            Buffer_[i] = i;
        }
    }

private:
    TMemcpyDriverOptions Options_;
    std::vector<ui64> Buffer_;
    std::vector<ui64> Data_;

    static constexpr ui64 DataSize_ = 1_GB;

    void DoOperation(const TOperation& operation) override
    {
        auto offset = operation.Offset % DataSize_;
        if (offset + operation.Size > DataSize_) {
            offset = 0;
        }
        auto start = offset / sizeof(ui64);

        switch (operation.Type) {
            case EOperationType::Read:
                memcpy(&Buffer_[0], &Data_[start], operation.Size);
                break;

            case EOperationType::Write:
                memcpy(&Data_[start], &Buffer_[0], operation.Size);
                break;
        }

        if (Options_.Validate) {
            for (size_t i = 0; i < operation.Size / sizeof(ui64); ++i) {
                YT_VERIFY(Buffer_[i] == Data_[start + i]);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateMemcpyDriver(TMemcpyDriverOptions options)
{
    return New<TMemcpyDriver>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class TPrwDriver
    : public TDriverBase
{
public:
    explicit TPrwDriver(TPrwDriverOptions options)
        : TDriverBase(options.Start)
        , Options_(std::move(options))
        , Buffer_(Options_.MaxBlockSize / sizeof(ui64))
    {
        for (ui64 i = 0; i < Buffer_.size(); ++i) {
            Buffer_[i] = i;
        }
    }

private:
    TPrwDriverOptions Options_;
    std::vector<ui64> Buffer_;

    void DoOperation(const TOperation& operation) override
    {
        switch (operation.Type) {
            case EOperationType::Read:
                Options_.Files[operation.FileIndex].File.Pread(&Buffer_[0], operation.Size, operation.Offset);
                ValidateRead(operation);
                break;

            case EOperationType::Write:
                PrepareValidateWrite(operation);
                Options_.Files[operation.FileIndex].File.Pwrite(&Buffer_[0], operation.Size, operation.Offset);
                //Options_.Files[operation.FileIndex].File.FlushData();
                ValidateWrite(operation);
                break;
        }
    }

    void ValidateRead(const TOperation& operation)
    {
        if (Options_.Validate) {
            for (ui64 i = 0; i < operation.Size / sizeof(ui64); ++i) {
                YT_VERIFY(Buffer_[i] == (operation.Offset / sizeof(ui64)) + i);
            }
        }
    }

    void PrepareValidateWrite(const TOperation& operation)
    {
        if (Options_.Validate) {
            for (ui64 i = 0; i < operation.Size / sizeof(ui64); ++i) {
                YT_VERIFY(Buffer_[i] == (operation.Offset / sizeof(ui64)) + i);
            }
        }
    }

    void ValidateWrite(const TOperation& operation)
    {
        if (Options_.Validate) {
            Options_.Files[operation.FileIndex].File.Pread(&Buffer_[0], operation.Size, operation.Offset);
            ValidateRead(operation);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreatePrwDriver(TPrwDriverOptions options)
{
    return New<TPrwDriver>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class TPrwv2Driver
    : public TDriverBase
{
public:
    explicit TPrwv2Driver(TPrwv2DriverOptions options)
        : TDriverBase(options.Start)
        , Options_(std::move(options))
        , Flags_(ComputeFlags(Options_.Config))
        , Buffer_(Options_.MaxBlockSize / sizeof(ui64))
    {
        for (ui64 i = 0; i < Buffer_.size(); ++i) {
            Buffer_[i] = i;
        }

        Iovec_.iov_base = &Buffer_[0];
    }

private:
    TPrwv2DriverOptions Options_;
    EPreadv2Flags Flags_;
    std::vector<ui64> Buffer_;
    TIovec Iovec_;

    EPreadv2Flags ComputeFlags(const TPrwv2DriverConfigPtr& config)
    {
        EPreadv2Flags flags{};
        if (config->HighPriority) {
            flags |= EPreadv2Flags::HighPriority;
        }
        if (config->Sync) {
            flags |= EPreadv2Flags::Sync;
        }
        if (config->DataSync) {
            flags |= EPreadv2Flags::DataSync;
        }
        return flags;
    }

    void DoOperation(const TOperation& operation) override
    {
        switch (operation.Type) {
            case EOperationType::Read: {
                Iovec_.iov_len = operation.Size;
                auto err = Preadv2(Options_.Files[operation.FileIndex].File, &Iovec_, 1, operation.Offset, Flags_);
                VerifyPreadResult("preadv2", err);
                ValidateRead(operation);
                break;
            }

            case EOperationType::Write: {
                PrepareValidateWrite(operation);
                Iovec_.iov_len = operation.Size;
                auto err = Pwritev2(Options_.Files[operation.FileIndex].File, &Iovec_, 1, operation.Offset, Flags_);
                VerifyPreadResult("pwritev2", err);
                ValidateWrite(operation);
                break;
            }
        }
    }

    void ValidateRead(const TOperation& operation)
    {
        if (Options_.Validate) {
            for (ui64 i = 0; i < operation.Size / sizeof(ui64); ++i) {
                YT_VERIFY(Buffer_[i] == (operation.Offset / sizeof(ui64)) + i);
            }
        }
    }

    void PrepareValidateWrite(const TOperation& operation)
    {
        if (Options_.Validate) {
            for (ui64 i = 0; i < operation.Size / sizeof(ui64); ++i) {
                YT_VERIFY(Buffer_[i] == (operation.Offset / sizeof(ui64)) + i);
            }
        }
    }

    void ValidateWrite(const TOperation& operation)
    {
        if (Options_.Validate) {
            Options_.Files[operation.FileIndex].File.Pread(&Buffer_[0], operation.Size, operation.Offset);
            ValidateRead(operation);
        }
    }

    void VerifyPreadResult(TStringBuf stage, ssize_t err)
    {
        if (err < 0) {
            THROW_ERROR_EXCEPTION("%v failed: %Qv",
                stage,
                LastSystemErrorText())
                << TErrorAttribute("driver_config", Options_.Config);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreatePrwv2Driver(TPrwv2DriverOptions options)
{
    return New<TPrwv2Driver>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncDriver
    : public IDriver
{
public:
    explicit TAsyncDriver(TAsyncDriverOptions options)
        : Options_(std::move(options))
        , QueueSize_(Options_.Config->QueueSize)
        , BatchSize_(Options_.Config->BatchSize)
        , Statistics_(options.Start)
    {
        Requests_.resize(QueueSize_);
        ControlBlocks_.resize(QueueSize_);
        CompletedEvents_.resize(QueueSize_);
        ControlBlockPtrs_.reserve(QueueSize_);
        FreeRequests_.reserve(QueueSize_);

        for (int index = 0; index < QueueSize_; ++index) {
            auto& request = Requests_[index];
            auto& controlBlock = ControlBlocks_[index];
            request.Buffer = TSharedMutableRef::AllocatePageAligned(options.MaxBlockSize);
            controlBlock.aio_buf = reinterpret_cast<ui64>(request.Buffer.Begin());
            FreeRequests_.push_back(index);
        }

        Context_ = LinuxAioCreate(QueueSize_);
    }

    ~TAsyncDriver()
    {
        LinuxAioDestroy(Context_);
    }

    void Oneshot(TOperationGenerator generator) override
    {
        PrepareOperation(generator());

        int submitted = LinuxAioSubmit(Context_, ControlBlockPtrs_);
        YT_VERIFY(submitted == 1);

        for (int received = 0; received <= 0;) {
            received = LinuxAioGetEvents(Context_, 1, TDuration(), &CompletedEvents_);
        }

        ProcessReceived(1);
        Statistics_.Finish();
    }

    void Burst(TOperationGenerator generator) override
    {
        TOperation operation;

        while (operation || std::ssize(FreeRequests_) != QueueSize_) {
            if (operation && !FreeRequests_.empty()) {
                operation = generator();
                if (operation.BurstAction == EBurstAction::Continue) {
                    PrepareOperation(operation);
                    continue;
                }
            }

            if (!ControlBlockPtrs_.empty()) {
                int submitted = LinuxAioSubmit(Context_, ControlBlockPtrs_);
                ControlBlockPtrs_.erase(ControlBlockPtrs_.begin(), ControlBlockPtrs_.begin() + submitted);
            }

            if (operation.BurstAction == EBurstAction::Continue && std::ssize(ControlBlockPtrs_) > QueueSize_ - BatchSize_) {
                continue;
            }

            // TODO(savrus) use userspace getevents
            int active = QueueSize_ - FreeRequests_.size();
            if (active > 0) {
                int received = LinuxAioGetEvents(Context_, std::min(active, BatchSize_), TDuration(), &CompletedEvents_);
                ProcessReceived(received);
            }
        }

        Statistics_.Finish();
    }

    TStatistics GetStatistics() override
    {
        return Statistics_;
    }

private:
    struct TIORequest
    {
        TSharedMutableRef Buffer;
        TOperation Operation;
        TLatencyMeter Latency;
    };

    const TAsyncDriverOptions Options_;
    const int QueueSize_;
    const int BatchSize_;
    TStatistics Statistics_;
    TLinuxAioContext Context_;
    std::vector<TIORequest> Requests_;
    std::vector<TLinuxAioControlBlock> ControlBlocks_;
    std::vector<TLinuxAioControlBlock*> ControlBlockPtrs_;
    std::vector<TLinuxAioEvent> CompletedEvents_;
    std::vector<int> FreeRequests_;

    int GetLinuxAioOpcode(const TOperation& operation) {
        switch (operation.Type) {
            case EOperationType::Read:
                return IOCB_CMD_PREAD;

            case EOperationType::Write:
                return IOCB_CMD_PWRITE;

            default:
                YT_ABORT();
        }
    }

    void PrepareOperation(const TOperation& operation)
    {
        YT_VERIFY(!FreeRequests_.empty());
        int requestIndex = FreeRequests_.back();
        FreeRequests_.pop_back();

        auto& controlBlock = ControlBlocks_[requestIndex];
        controlBlock.aio_fildes = Options_.Files[operation.FileIndex].File.GetHandle();
        controlBlock.aio_offset = operation.Offset;
        controlBlock.aio_nbytes = operation.Size;
        controlBlock.aio_lio_opcode = GetLinuxAioOpcode(operation);
        ControlBlockPtrs_.push_back(&controlBlock);

        auto& request = Requests_[requestIndex];
        request.Operation = operation;
        PrepareValidate(requestIndex);
        request.Latency.Tick();
    }

    void ProcessReceived(int received)
    {
        for (int index = 0; index < received; ++index) {
            auto& event = CompletedEvents_[index];
            auto* controlBlockPtr = reinterpret_cast<TLinuxAioControlBlock*>(event.obj);
            int requestIndex = controlBlockPtr - &ControlBlocks_[0];
            auto& controlBlock = ControlBlocks_[requestIndex];
            auto& request = Requests_[requestIndex];
            YT_VERIFY(controlBlockPtr == &controlBlock);
            YT_VERIFY(event.res == static_cast<ssize_t>(controlBlock.aio_nbytes));
            YT_VERIFY(event.res == request.Operation.Size);
            Statistics_.Update(request.Operation, request.Latency.Tick());
            FreeRequests_.push_back(requestIndex);
            Validate(requestIndex);
        }
    }

    void PrepareValidate(int requestIndex)
    {
        if (!Options_.Validate) {
            return;
        }

        auto& operation = Requests_[requestIndex].Operation;
        auto& buffer = Requests_[requestIndex].Buffer;

        switch (operation.Type) {
            case EOperationType::Read:
                break;

            case EOperationType::Write:
                PrepareValidateWrite(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            default:
                YT_ABORT();
        }
    }

    void PrepareValidateWrite(const TOperation& operation, i64* buffer)
    {
        for (ssize_t i = 0; i < static_cast<ssize_t>(operation.Size / sizeof(ui64)); ++i) {
            YT_VERIFY(buffer[i] == static_cast<ssize_t>(operation.Offset / sizeof(ui64)) + i);
        }
    }

    void Validate(int requestIndex)
    {
        if (!Options_.Validate) {
            return;
        }

        auto& operation = Requests_[requestIndex].Operation;
        auto& buffer = Requests_[requestIndex].Buffer;

        switch (operation.Type) {
            case EOperationType::Read:
                ValidateRead(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            case EOperationType::Write:
                ValidateWrite(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            default:
                YT_ABORT();
        }
    }

    void ValidateRead(const TOperation& operation, i64* buffer)
    {
        for (ssize_t i = 0; i < static_cast<ssize_t>(operation.Size / sizeof(ui64)); ++i) {
            YT_VERIFY(buffer[i] == static_cast<ssize_t>(operation.Offset / sizeof(ui64)) + i);
        }
    }

    void ValidateWrite(const TOperation& operation, i64* buffer)
    {
        Options_.Files[operation.FileIndex].File.Pread(buffer, operation.Size, operation.Offset);
        ValidateRead(operation, buffer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateAsyncDriver(TAsyncDriverOptions options)
{
    return New<TAsyncDriver>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

class TUringDriver
    : public IDriver
{
public:
    explicit TUringDriver(TUringDriverOptions options)
        : Options_(std::move(options))
        , QueueSize_(Options_.Config->QueueSize)
        , BatchSize_(Options_.Config->BatchSize)
        , Statistics_(options.Start)
        , Uring_(&UringHolder_)
    {
        Requests_.resize(QueueSize_);
        FreeRequests_.reserve(QueueSize_);

        for (int index = 0; index < QueueSize_; ++index) {
            auto& request = Requests_[index];
            request.Buffer = TSharedMutableRef::AllocatePageAligned(options.MaxBlockSize);
            request.Iovec.iov_base = request.Buffer.Begin();
            request.Iovec.iov_len = options.MaxBlockSize;
            FreeRequests_.push_back(index);
        }

        unsigned flags = 0;
        if (Options_.Config->UseKernelSQThread) {
            flags |= IORING_SETUP_SQPOLL;
        }

        // TODO(savrus) Support SQ thread affinity.

        int err = UringQueueInit(QueueSize_, Uring_, flags);
        VerifyUringResult("init", err);

        if (Options_.Config->FixedBuffers) {
            std::vector<TIovec> iovecs;
            for (const auto& request : Requests_) {
                iovecs.push_back(request.Iovec);
            }
            err = UringRegisterBuffers(Uring_, iovecs);
            VerifyUringResult("buffers registration", err);
        }

        if (Options_.Config->FixedFiles) {
            std::vector<int> files;
            for (auto& file : Options_.Files) {
                files.push_back(file.File.GetHandle());
            }
            err = UringRegisterFiles(Uring_, files);
            VerifyUringResult("files registration", err);
        }
    }

    ~TUringDriver()
    {
        UringExit(Uring_);
    }

    void Oneshot(TOperationGenerator generator) override
    {
        PrepareOperation(generator());

        int err = UringSubmit(Uring_);
        VerifyUringResult("wait", err);

        WaitAndProcessReceived(1);
        Statistics_.Finish();
    }

    void Burst(TOperationGenerator generator) override
    {
        TOperation operation;

        while (operation || std::ssize(FreeRequests_) != QueueSize_) {
            if (operation && !FreeRequests_.empty()) {
                operation = generator();
                if (operation.BurstAction == EBurstAction::Continue) {
                    PrepareOperation(operation);
                    continue;
                }
            }

            UringSubmit(Uring_);

            int active = QueueSize_ - FreeRequests_.size();
            if (active > 0) {
                unsigned waitCount = operation.BurstAction == EBurstAction::Continue
                    ? std::min(active, BatchSize_)
                    : 0;
                WaitAndProcessReceived(waitCount);
            }
        }

        Statistics_.Finish();
    }

    TStatistics GetStatistics() override
    {
        return Statistics_;
    }

private:
    struct TIORequest
    {
        TSharedMutableRef Buffer;
        TIovec Iovec;
        TOperation Operation;
        TLatencyMeter Latency;
    };

    const TUringDriverOptions Options_;
    const int QueueSize_;
    const int BatchSize_;
    TStatistics Statistics_;
    TUring UringHolder_;
    TUring* Uring_;

    std::vector<TIORequest> Requests_;
    std::vector<int> FreeRequests_;

    void PrepareOperation(const TOperation& operation)
    {
        YT_VERIFY(!FreeRequests_.empty());

        TUringSqe* sqe = UringGetSqe(Uring_);
        if (!sqe) {
            return;
        }

        int requestIndex = FreeRequests_.back();
        FreeRequests_.pop_back();
        auto& request = Requests_[requestIndex];
        request.Iovec.iov_len = operation.Size;
        request.Operation = operation;

        int fd = Options_.Config->FixedFiles
            ? operation.FileIndex
            : Options_.Files[operation.FileIndex].File.GetHandle();

        if (Options_.Config->FixedBuffers) {
            switch (operation.Type) {
                case EOperationType::Read:
                    UringPrepareReadFixed(sqe, fd, reinterpret_cast<i64*>(request.Buffer.Begin()), operation.Size, operation.Offset, requestIndex);
                    break;
                case EOperationType::Write:
                    UringPrepareWriteFixed(sqe, fd, reinterpret_cast<i64*>(request.Buffer.Begin()), operation.Size, operation.Offset, requestIndex);
                    break;
                default:
                    YT_ABORT();
            }
        } else {
            switch (operation.Type) {
                case EOperationType::Read:
                    UringPrepareReadv(sqe, fd, &request.Iovec, 1, operation.Offset);
                    break;
                case EOperationType::Write:
                    UringPrepareWritev(sqe, fd, &request.Iovec, 1, operation.Offset);
                    break;
                default:
                    YT_ABORT();
            }
        }

        UringSqeSetData(sqe, reinterpret_cast<void*>(requestIndex));

        if (Options_.Config->FixedFiles) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        PrepareValidate(requestIndex);
        request.Latency.Tick();
    }


    void WaitAndProcessReceived(unsigned waitCount)
    {
        auto processed = ProcessReceived();
        if (processed >= static_cast<ssize_t>(waitCount)) {
            return;
        }

        TUringCqe* cqe;
        int err = UringWaitCqes(Uring_, &cqe, waitCount - processed, NULL, NULL);
        VerifyUringResult("wait", err);

        ProcessReceived();
    }

    int ProcessReceived()
    {
        int processed = 0;
        TUringCqe* cqe;

        while (true) {
            int err = UringPeekCqe(Uring_, &cqe);
            if (err) {
                break;
            }

            auto requestIndex = reinterpret_cast<i64>(UringCqeGetData(cqe));
            auto& request = Requests_[requestIndex];
            YT_VERIFY(cqe->res == request.Operation.Size);
            Statistics_.Update(request.Operation, request.Latency.Tick());
            UringCqeSeen(Uring_, cqe);
            FreeRequests_.push_back(requestIndex);
            Validate(requestIndex);
            ++processed;
        }
        return processed;
    }

    void PrepareValidate(int requestIndex)
    {
        if (!Options_.Validate) {
            return;
        }

        auto& operation = Requests_[requestIndex].Operation;
        auto& buffer = Requests_[requestIndex].Buffer;

        switch (operation.Type) {
            case EOperationType::Read:
                break;

            case EOperationType::Write:
                PrepareValidateWrite(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            default:
                YT_ABORT();
        }
    }

    void PrepareValidateWrite(const TOperation& operation, i64* buffer)
    {
        for (ssize_t i = 0; i < static_cast<ssize_t>(operation.Size / sizeof(ui64)); ++i) {
            YT_VERIFY(buffer[i] == static_cast<ssize_t>(operation.Offset / sizeof(ui64)) + i);
        }
    }

    void Validate(int requestIndex)
    {
        if (!Options_.Validate) {
            return;
        }

        auto& operation = Requests_[requestIndex].Operation;
        auto& buffer = Requests_[requestIndex].Buffer;

        switch (operation.Type) {
            case EOperationType::Read:
                ValidateRead(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            case EOperationType::Write:
                ValidateWrite(operation, reinterpret_cast<i64*>(buffer.Begin()));
                break;

            default:
                YT_ABORT();
        }
    }

    void ValidateRead(const TOperation& operation, i64* buffer)
    {
        for (ssize_t i = 0; i < static_cast<ssize_t>(operation.Size / sizeof(ui64)); ++i) {
            YT_VERIFY(buffer[i] == static_cast<ssize_t>(operation.Offset / sizeof(ui64)) + i);
        }
    }

    void ValidateWrite(const TOperation& operation, i64* buffer)
    {
        Options_.Files[operation.FileIndex].File.Pread(buffer, operation.Size, operation.Offset);
        ValidateRead(operation, buffer);
    }

    void VerifyUringResult(TStringBuf stage, int err)
    {
        if (err < 0) {
            THROW_ERROR_EXCEPTION("Uring %v failed: %Qv",
                stage,
                LastSystemErrorText(-err))
                << TErrorAttribute("driver_config", Options_.Config);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateUringDriver(TUringDriverOptions options)
{
    return New<TUringDriver>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
