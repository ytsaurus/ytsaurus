#include "connection.h"
#include "packet_connection.h"
#include "private.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/finally.h>

#include <yt/core/net/socket.h>

#include <errno.h>

namespace NYT::NNet {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFDConnectionImpl)

////////////////////////////////////////////////////////////////////////////////

struct TIOResult
{
    bool Retry;
    i64 ByteCount;

    TIOResult(bool retry, i64 byteCount)
        : Retry(retry)
        , ByteCount(byteCount)
    { }
};

struct IIOOperation
{
    virtual ~IIOOperation() = default;

    virtual TErrorOr<TIOResult> PerformIO(int fd) = 0;

    virtual void Abort(const TError& error) = 0;

    virtual void SetResult() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadOperation
    : public IIOOperation
{
public:
    explicit TReadOperation(const TSharedMutableRef& buffer)
        : Buffer_(buffer)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        size_t bytesRead = 0;
        while (true) {
            ssize_t size = HandleEintr(::read, fd, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);
            if (size == -1) {
                if (errno == EAGAIN) {
                    return TIOResult(Position_ == 0, bytesRead);
                }

                return TError("Read failed")
                    << TError::FromSystem();
            }

            bytesRead += size;
            Position_ += size;

            if (Position_ == Buffer_.Size() || size == 0) {
                return TIOResult(false, bytesRead);
            }
        }
    }

    virtual void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    virtual void SetResult() override
    {
        ResultPromise_.Set(Position_);
    }

    TFuture<size_t> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    TSharedMutableRef Buffer_;
    size_t Position_ = 0;

    TPromise<size_t> ResultPromise_ = NewPromise<size_t>();
};

////////////////////////////////////////////////////////////////////////////////

class TReceiveFromOperation
    : public IIOOperation
{
public:
    explicit TReceiveFromOperation(const TSharedMutableRef& buffer)
        : Buffer_(buffer)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        ssize_t size = HandleEintr(
            ::recvfrom,
            fd,
            Buffer_.Begin(),
            Buffer_.Size(),
            0, // flags
            RemoteAddress_.GetSockAddr(),
            RemoteAddress_.GetLengthPtr());

        if (size == -1) {
            if (errno == EAGAIN) {
                return TIOResult(true, 0);
            }

            return TError("Read failed")
                << TError::FromSystem();
        }

        Position_ += size;
        return TIOResult(false, size);
    }

    virtual void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    virtual void SetResult() override
    {
        ResultPromise_.Set(std::make_pair(Position_, RemoteAddress_));
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    TSharedMutableRef Buffer_;
    size_t Position_ = 0;
    TNetworkAddress RemoteAddress_;

    TPromise<std::pair<size_t, TNetworkAddress>> ResultPromise_ = NewPromise<std::pair<size_t, TNetworkAddress>>();
};

////////////////////////////////////////////////////////////////////////////////

class TWriteOperation
    : public IIOOperation
{
public:
    explicit TWriteOperation(const TSharedRef& buffer)
        : Buffer_(buffer)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        size_t bytesWritten = 0;
        while (true) {
            YT_VERIFY(Position_ < Buffer_.Size());
            ssize_t size = HandleEintr(::write, fd, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);

            if (size == -1) {
                if (errno == EAGAIN) {
                    return TIOResult(true, bytesWritten);
                }

                return TError("Write failed")
                    << TError::FromSystem();
            }

            YT_VERIFY(size > 0);
            bytesWritten += size;
            Position_ += size;

            if (Position_ == Buffer_.Size()) {
                return TIOResult(false, bytesWritten);
            }
        }
    }

    virtual void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    virtual void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    TSharedRef Buffer_;
    size_t Position_ = 0;

    TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

class TWriteVOperation
    : public IIOOperation
{
public:
    explicit TWriteVOperation(const TSharedRefArray& buffers)
        : Buffers_(buffers)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        size_t bytesWritten = 0;
        while (true) {
            constexpr int MaxEntries = 128;
            iovec ioVectors[MaxEntries];

            ioVectors[0].iov_base = reinterpret_cast<void*>(const_cast<char*>(Buffers_[Index_].Begin() + Position_));
            ioVectors[0].iov_len = Buffers_[Index_].Size() - Position_;

            size_t ioVectorsCount = 1;
            for (; ioVectorsCount < MaxEntries && ioVectorsCount + Index_ < Buffers_.Size(); ++ioVectorsCount) {
                const auto& ref = Buffers_[Index_ + ioVectorsCount];

                ioVectors[ioVectorsCount].iov_base = reinterpret_cast<void*>(const_cast<char*>(ref.Begin()));
                ioVectors[ioVectorsCount].iov_len = ref.Size();
            }

            ssize_t size = HandleEintr(::writev, fd, ioVectors, ioVectorsCount);

            if (size == -1) {
                if (errno == EAGAIN) {
                    return TIOResult(true, bytesWritten);
                }

                return TError("Write failed")
                    << TError::FromSystem();
            }

            YT_VERIFY(size > 0);
            bytesWritten += size;
            Position_ += size;

            while (Index_ != Buffers_.Size() && Position_ >= Buffers_[Index_].Size()) {
                Position_ -= Buffers_[Index_].Size();
                Index_++;
            }

            if (Index_ == Buffers_.Size()) {
                return TIOResult(false, bytesWritten);
            }
        }
    }

    virtual void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    virtual void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    TSharedRefArray Buffers_;
    size_t Index_ = 0;
    size_t Position_ = 0;

    TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

class TShutdownOperation
    : public IIOOperation
{
public:
    explicit TShutdownOperation(bool shutdownRead)
        : ShutdownRead_(shutdownRead)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        int res = HandleEintr(::shutdown, fd, ShutdownRead_ ? SHUT_RD : SHUT_WR);
        if (res == -1) {
            return TError("Shutdown failed")
                << TError::FromSystem();
        }
        return TIOResult(false, 0);
    }

    virtual void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    virtual void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    const bool ShutdownRead_;
    TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

class TFDConnectionImpl
    : public IPollable
{
public:
    static TFDConnectionImplPtr Create(
        int fd,
        const TString& filePath,
        const IPollerPtr& poller)
    {
        auto impl = New<TFDConnectionImpl>(fd, filePath, poller);
        impl->Init();
        return impl;
    }

    static TFDConnectionImplPtr Create(
        int fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        const IPollerPtr& poller)
    {
        auto impl = New<TFDConnectionImpl>(fd, localAddress, remoteAddress, poller);
        impl->Init();
        return impl;
    }

    virtual const TString& GetLoggingId() const override
    {
        return LoggingId_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        DoIO(&WriteDirection_, Any(control & EPollControl::Write));
        DoIO(&ReadDirection_, Any(control & EPollControl::Read));
    }

    virtual void OnShutdown() override
    {
        // Poller guarantees that OnShutdown is never executed concurrently with OnEvent()
        {
            auto guard = Guard(Lock_);

            YT_VERIFY(!ReadDirection_.Running);
            YT_VERIFY(!WriteDirection_.Running);

            if (Error_.IsOK()) {
                Error_ = TError("Connection is shut down");
            }

            ShutdownRequested_ = true;
            if (SynchronousIOCount_) {
                return;
            }
        }

        if (ReadDirection_.Operation) {
            ReadDirection_.Operation->Abort(Error_);
            ReadDirection_.Operation.reset();
        }
        if (WriteDirection_.Operation) {
            WriteDirection_.Operation->Abort(Error_);
            WriteDirection_.Operation.reset();
        }

        YT_VERIFY(TryClose(FD_, false));
        FD_ = -1;

        ShutdownPromise_.Set();

        TDelayedExecutor::CancelAndClear(WriteTimeoutCookie_);
        TDelayedExecutor::CancelAndClear(ReadTimeoutCookie_);
    }

    TFuture<size_t> Read(const TSharedMutableRef& data)
    {
        auto read = std::make_unique<TReadOperation>(data);
        auto future = read->ToFuture();
        StartIO(&ReadDirection_, std::move(read));
        return future;
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(const TSharedMutableRef& buffer)
    {
        auto receive = std::make_unique<TReceiveFromOperation>(buffer);
        auto future = receive->ToFuture();
        StartIO(&ReadDirection_, std::move(receive));
        return future;
    }

    void SendTo(const TSharedRef& buffer, const TNetworkAddress& address)
    {
        auto guard = TSynchronousIOGuard(this);
        auto res = HandleEintr(
            ::sendto,
            FD_,
            buffer.Begin(),
            buffer.Size(),
            0, // flags
            address.GetSockAddr(),
            address.GetLength());
        if (res == -1) {
            THROW_ERROR_EXCEPTION("Write failed")
                << TError::FromSystem();
        }
    }

    bool SetNoDelay()
    {
        auto guard = TSynchronousIOGuard(this);
        return TrySetSocketNoDelay(FD_);
    }

    bool SetKeepAlive()
    {
        auto guard = TSynchronousIOGuard(this);
        return TrySetSocketKeepAlive(FD_);
    }

    TFuture<void> Write(const TSharedRef& data)
    {
        auto write = std::make_unique<TWriteOperation>(data);
        auto future = write->ToFuture();
        StartIO(&WriteDirection_, std::move(write));
        return future;
    }

    TFuture<void> WriteV(const TSharedRefArray& data)
    {
        auto writeV = std::make_unique<TWriteVOperation>(data);
        auto future = writeV->ToFuture();
        StartIO(&WriteDirection_, std::move(writeV));
        return future;
    }

    TFuture<void> Close()
    {
        auto error = TError("Connection closed")
            << TErrorAttribute("connection", Name_);
        return AbortIO(error);
    }

    bool IsIdle()
    {
        auto guard = Guard(Lock_);
        return Error_.IsOK() && !WriteDirection_.Operation && !ReadDirection_.Operation && SynchronousIOCount_ == 0;
    }

    TFuture<void> Abort(const TError& error)
    {
        return AbortIO(error);
    }

    TFuture<void> CloseRead()
    {
        auto shutdownRead = std::make_unique<TShutdownOperation>(true);
        auto future = shutdownRead->ToFuture();
        StartIO(&ReadDirection_, std::move(shutdownRead));
        return future;
    }

    TFuture<void> CloseWrite()
    {
        auto shutdownWrite = std::make_unique<TShutdownOperation>(false);
        auto future = shutdownWrite->ToFuture();
        StartIO(&WriteDirection_, std::move(shutdownWrite));
        return future;
    }

    const TNetworkAddress& LocalAddress() const
    {
        return LocalAddress_;
    }

    const TNetworkAddress& RemoteAddress() const
    {
        return RemoteAddress_;
    }

    int GetHandle() const
    {
        return FD_;
    }

    i64 GetReadByteCount() const
    {
        return ReadDirection_.BytesTransferred;
    }

    i64 GetWriteByteCount() const
    {
        return WriteDirection_.BytesTransferred;
    }

    TConnectionStatistics GetReadStatistics() const
    {
        auto guard = Guard(Lock_);
        return ReadDirection_.GetStatistics();
    }

    TConnectionStatistics GetWriteStatistics() const
    {
        auto guard = Guard(Lock_);
        return WriteDirection_.GetStatistics();
    }

    void SetReadDeadline(TInstant deadline)
    {
        if (ReadTimeoutCookie_) {
            TDelayedExecutor::CancelAndClear(ReadTimeoutCookie_);
        }

        if (deadline) {
            ReadTimeoutCookie_ = TDelayedExecutor::Submit(AbortFromReadTimeout_, deadline);
        }
    }

    void SetWriteDeadline(TInstant deadline)
    {
        if (WriteTimeoutCookie_) {
            TDelayedExecutor::CancelAndClear(WriteTimeoutCookie_);
        }

        if (deadline) {
            WriteTimeoutCookie_ = TDelayedExecutor::Submit(AbortFromWriteTimeout_, deadline);
        }
    }

private:
    const TString Name_;
    const TString LoggingId_;
    const TNetworkAddress LocalAddress_;
    const TNetworkAddress RemoteAddress_;
    int FD_ = -1;

    TFDConnectionImpl(
        int fd,
        const TString& filePath,
        const IPollerPtr& poller)
        : Name_(Format("File{%v}", filePath))
        , FD_(fd)
        , Poller_(poller)
    { }

    TFDConnectionImpl(
        int fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        const IPollerPtr& poller)
        : Name_(Format("FD{%v<->%v}", localAddress, remoteAddress))
        , LoggingId_(Format("ConnectionId: %v", Name_))
        , LocalAddress_(localAddress)
        , RemoteAddress_(remoteAddress)
        , FD_(fd)
        , Poller_(poller)
    { }

    DECLARE_NEW_FRIEND();

    class TSynchronousIOGuard
    {
    public:
        TSynchronousIOGuard(TFDConnectionImplPtr owner)
            : Owner_(std::move(owner))
        {
            auto guard = Guard(Owner_->Lock_);
            Owner_->Error_.ThrowOnError();
            ++Owner_->SynchronousIOCount_;
        }

        ~TSynchronousIOGuard()
        {
            if (Owner_) {
                auto guard = Guard(Owner_->Lock_);
                YT_VERIFY(Owner_->SynchronousIOCount_);
                if (--Owner_->SynchronousIOCount_ == 0 &&
                    Owner_->ShutdownRequested_)
                {
                    guard.Release();
                    Owner_->OnShutdown();
                }
            }
        }

        TSynchronousIOGuard(const TSynchronousIOGuard&) = delete;
        TSynchronousIOGuard(TSynchronousIOGuard&&) = default;

        TSynchronousIOGuard& operator=(const TSynchronousIOGuard&) = delete;
        TSynchronousIOGuard& operator=(TSynchronousIOGuard&&) = default;

    private:
        TFDConnectionImplPtr Owner_;
    };

    struct TIODirection
    {
        std::unique_ptr<IIOOperation> Operation;
        std::atomic<i64> BytesTransferred = {0};
        TDuration IdleDuration;
        TDuration BusyDuration;
        TCpuInstant StartTime = GetCpuInstant();
        bool Pending = false;
        bool Running = false;

        void StartBusyTimer()
        {
            auto now = GetCpuInstant();
            IdleDuration += CpuDurationToDuration(now - StartTime);
            StartTime = now;
        }

        void StopBusyTimer()
        {
            auto now = GetCpuInstant();
            BusyDuration += CpuDurationToDuration(now - StartTime);
            StartTime = now;
        }

        TConnectionStatistics GetStatistics() const
        {
            TConnectionStatistics statistics{IdleDuration, BusyDuration};
            (Operation ? statistics.BusyDuration : statistics.IdleDuration) += CpuDurationToDuration(GetCpuInstant() - StartTime);
            return statistics;
        }
    };

    TSpinLock Lock_;
    TIODirection ReadDirection_;
    TIODirection WriteDirection_;
    bool ShutdownRequested_ = false;
    unsigned int SynchronousIOCount_ = 0;
    TError Error_;
    TPromise<void> ShutdownPromise_ = NewPromise<void>();

    IPollerPtr Poller_;

    TClosure AbortFromReadTimeout_;
    TClosure AbortFromWriteTimeout_;

    TDelayedExecutorCookie ReadTimeoutCookie_;
    TDelayedExecutorCookie WriteTimeoutCookie_;

    void Init()
    {
        AbortFromReadTimeout_ = BIND(&TFDConnectionImpl::AbortFromReadTimeout, MakeWeak(this));
        AbortFromWriteTimeout_ = BIND(&TFDConnectionImpl::AbortFromWriteTimeout, MakeWeak(this));

        Poller_->Register(this);
        Poller_->Arm(FD_, this, EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered);
    }

    void StartIO(TIODirection* direction, std::unique_ptr<IIOOperation> operation)
    {
        TError error;
        bool needRetry = false;

        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                if (direction->Operation) {
                    THROW_ERROR_EXCEPTION("Another IO operation is in progress")
                        << TErrorAttribute("connection", Name_);
                }

                YT_VERIFY(!direction->Running);
                direction->Operation = std::move(operation);
                direction->StartBusyTimer();
                // Start operation only if this direction already has pending
                // event otherwise reading from FIFO before opening by writer
                // will return EOF immediately.
                needRetry = direction->Pending;
            } else {
                error = Error_;
            }
        }

        if (!error.IsOK()) {
            operation->Abort(error);
            return;
        }

        if (needRetry) {
            Poller_->Retry(this);
        }
    }

    void DoIO(TIODirection* direction, bool event)
    {
        {
            auto guard = Guard(Lock_);

            if (!event && !direction->Pending) {
                return;
            }

            if (!Error_.IsOK()) {
                return;
            }

            if (!direction->Operation || direction->Running) {
                direction->Pending |= event;
                return;
            }

            direction->Pending = false;
            direction->Running = true;
        }

        auto result = direction->Operation->PerformIO(FD_);
        if (result.IsOK()) {
            direction->BytesTransferred += result.Value().ByteCount;
        } else {
            result = result << TErrorAttribute("connection", Name_);
        }

        bool needUnregister = false;
        bool needRetry = false;
        std::unique_ptr<IIOOperation> operation;
        {
            auto guard = Guard(Lock_);
            direction->Running = false;
            if (!result.IsOK()) {
                // IO finished with error.
                operation = std::move(direction->Operation);
                if (Error_.IsOK()) {
                    Error_ = result;
                    Poller_->Unarm(FD_);
                    needUnregister = true;
                }
                direction->StopBusyTimer();
            } else if (!Error_.IsOK()) {
                // IO was aborted.
                operation = std::move(direction->Operation);
                // Avoid aborting completed IO.
                if (result.Value().Retry) {
                    result = Error_;
                }
                direction->Pending = true;
                direction->StopBusyTimer();
            } else if (result.Value().Retry) {
                // IO not completed. Retry if have pending backlog.
                needRetry = direction->Pending;
            } else {
                // IO finished successfully.
                operation = std::move(direction->Operation);
                // TODO not set pending if no backlog after short read/write
                direction->Pending = true;
                direction->StopBusyTimer();
            }
        }

        if (!result.IsOK()) {
            operation->Abort(result);
        } else if (!result.Value().Retry) {
            operation->SetResult();
        } else if (needRetry) {
            Poller_->Retry(this, false);
        }

        if (needUnregister) {
            Poller_->Unregister(this);
        }
    }

    TFuture<void> AbortIO(const TError& error)
    {
        auto guard = Guard(Lock_);
        if (Error_.IsOK()) {
            Error_ = error;
            Poller_->Unarm(FD_);
            guard.Release();
            Poller_->Unregister(this);
        }
        return ShutdownPromise_.ToFuture();
    }

    void AbortFromReadTimeout()
    {
        Abort(TError("Read timeout"));
    }

    void AbortFromWriteTimeout()
    {
        Abort(TError("Write timeout"));
    }
};

DEFINE_REFCOUNTED_TYPE(TFDConnectionImpl)

////////////////////////////////////////////////////////////////////////////////

// The sole purpose of this class is to call Abort on Impl in dtor.
class TFDConnection
    : public IConnection
{
public:
    TFDConnection(
        int fd,
        const TString& pipePath,
        const IPollerPtr& poller,
        TIntrusivePtr<TRefCounted> pipeHolder = nullptr)
        : Impl_(TFDConnectionImpl::Create(fd, pipePath, poller))
        , PipeHolder_(std::move(pipeHolder))
    { }

    TFDConnection(
        int fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        const IPollerPtr& poller)
        : Impl_(TFDConnectionImpl::Create(fd, localAddress, remoteAddress, poller))
    { }

    ~TFDConnection()
    {
        Impl_->Abort(TError("Connection is abandoned"));
    }

    virtual const TNetworkAddress& LocalAddress() const override
    {
        return Impl_->LocalAddress();
    }

    virtual const TNetworkAddress& RemoteAddress() const override
    {
        return Impl_->RemoteAddress();
    }

    virtual int GetHandle() const override
    {
        return Impl_->GetHandle();
    }

    virtual TFuture<size_t> Read(const TSharedMutableRef& data) override
    {
        return Impl_->Read(data);
    }

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        return Impl_->Write(data);
    }

    virtual TFuture<void> WriteV(const TSharedRefArray& data) override
    {
        return Impl_->WriteV(data);
    }

    virtual TFuture<void> Close() override
    {
        return Impl_->Close();
    }

    virtual bool IsIdle() const override
    {
        return Impl_->IsIdle();
    }

    virtual TFuture<void> Abort() override
    {
        return Impl_->Abort(TError(EErrorCode::Aborted, "Connection aborted"));
    }

    virtual TFuture<void> CloseRead() override
    {
        return Impl_->CloseRead();
    }

    virtual TFuture<void> CloseWrite() override
    {
        return Impl_->CloseWrite();
    }

    virtual i64 GetReadByteCount() const override
    {
        return Impl_->GetReadByteCount();
    }

    virtual i64 GetWriteByteCount() const override
    {
        return Impl_->GetWriteByteCount();
    }

    virtual TConnectionStatistics GetReadStatistics() const override
    {
        return Impl_->GetReadStatistics();
    }

    virtual TConnectionStatistics GetWriteStatistics() const override
    {
        return Impl_->GetWriteStatistics();
    }

    virtual void SetReadDeadline(TInstant deadline) override
    {
        Impl_->SetReadDeadline(deadline);
    }

    virtual void SetWriteDeadline(TInstant deadline) override
    {
        Impl_->SetWriteDeadline(deadline);
    }

    virtual bool SetNoDelay() override
    {
        return Impl_->SetNoDelay();
    }

    virtual bool SetKeepAlive() override
    {
        return Impl_->SetKeepAlive();
    }

private:
    const TFDConnectionImplPtr Impl_;
    TIntrusivePtr<TRefCounted> PipeHolder_;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<IConnectionPtr, IConnectionPtr> CreateConnectionPair(const IPollerPtr& poller)
{
    int flags = SOCK_STREAM;
#ifdef _linux_
    flags |= SOCK_NONBLOCK | SOCK_CLOEXEC;
#endif

    int fds[2];
    if (HandleEintr(::socketpair, AF_LOCAL, flags, 0, fds) == -1) {
        THROW_ERROR_EXCEPTION("Failed to create socket pair")
            << TError::FromSystem();
    }

    try {
        auto address0 = GetSocketName(fds[0]);
        auto address1 = GetSocketName(fds[1]);

        auto first = New<TFDConnection>(fds[0], address0, address1, poller);
        auto second = New<TFDConnection>(fds[1], address1, address0, poller);
        return std::make_pair(std::move(first), std::move(second));
    } catch (...) {
        YT_VERIFY(TryClose(fds[0], false));
        YT_VERIFY(TryClose(fds[1], false));
        throw;
    }
}

IConnectionPtr CreateConnectionFromFD(
    int fd,
    const TNetworkAddress& localAddress,
    const TNetworkAddress& remoteAddress,
    const IPollerPtr& poller)
{
    return New<TFDConnection>(fd, localAddress, remoteAddress, poller);
}

IConnectionReaderPtr CreateInputConnectionFromFD(
    int fd,
    const TString& pipePath,
    const IPollerPtr& poller,
    const TIntrusivePtr<TRefCounted>& pipeHolder)
{
    return New<TFDConnection>(fd, pipePath, poller, pipeHolder);
}

IConnectionReaderPtr CreateInputConnectionFromPath(
    const TString& pipePath,
    const IPollerPtr& poller,
    const TIntrusivePtr<TRefCounted>& pipeHolder)
{
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    int fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd == -1) {
        THROW_ERROR_EXCEPTION("Failed to open named pipe")
            << TError::FromSystem()
            << TErrorAttribute("path", pipePath);
    }

    return New<TFDConnection>(fd, pipePath, poller, pipeHolder);
}

IConnectionWriterPtr CreateOutputConnectionFromPath(
    const TString& pipePath,
    const IPollerPtr& poller,
    const TIntrusivePtr<TRefCounted>& pipeHolder)
{
    int flags = O_WRONLY | O_CLOEXEC;
    int fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd == -1) {
        THROW_ERROR_EXCEPTION("Failed to open named pipe")
            << TError::FromSystem()
            << TErrorAttribute("path", pipePath);
    }

    try {
        SafeMakeNonblocking(fd);
    } catch (...) {
        SafeClose(fd, false);
        throw;
    }
    return New<TFDConnection>(fd, pipePath, poller, pipeHolder);
}

////////////////////////////////////////////////////////////////////////////////

class TPacketConnection
    : public IPacketConnection
{
public:
    TPacketConnection(
        int fd,
        const TNetworkAddress& localAddress,
        const IPollerPtr& poller)
        : Impl_(TFDConnectionImpl::Create(fd, localAddress, TNetworkAddress{}, poller))
    { }

    ~TPacketConnection()
    {
        Abort();
    }

    virtual TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(
        const TSharedMutableRef& buffer) override
    {
        return Impl_->ReceiveFrom(buffer);
    }

    virtual void SendTo(const TSharedRef& buffer, const TNetworkAddress& address) override
    {
        Impl_->SendTo(buffer, address);
    }

    virtual TFuture<void> Abort() override
    {
        return Impl_->Abort(TError("Connection is abandoned"));
    }

private:
    TFDConnectionImplPtr Impl_;
};

IPacketConnectionPtr CreatePacketConnection(
    const TNetworkAddress& at,
    const NConcurrency::IPollerPtr& poller)
{
    auto fd = CreateUdpSocket();
    try {
        SetReuseAddrFlag(fd);
        BindSocket(fd, at);
    } catch (...) {
        SafeClose(fd, false);
        throw;
    }

    return New<TPacketConnection>(fd, at, poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
