#include "connection.h"
#include "packet_connection.h"
#include "private.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/proc.h>
#include <yt/core/net/socket.h>
#include <yt/core/misc/finally.h>

#include <errno.h>

namespace NYT::NNet {

using namespace NConcurrency;
using namespace NProfiling;

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
    TReadOperation(const TSharedMutableRef& buffer, bool delayFirstRead)
        : Buffer_(buffer)
        , DelayFirstRead_(delayFirstRead)
    { }

    virtual TErrorOr<TIOResult> PerformIO(int fd) override
    {
        if (DelayFirstRead_) {
            DelayFirstRead_ = false;
            return TIOResult(true, 0);
        }

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
    bool DelayFirstRead_ = false;

    TPromise<size_t> ResultPromise_ = NewPromise<size_t>();
};

////////////////////////////////////////////////////////////////////////////////

class TReceiveFromOperation
    : public IIOOperation
{
public:
    TReceiveFromOperation(const TSharedMutableRef& buffer)
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
            YCHECK(Position_ < Buffer_.Size());
            ssize_t size = HandleEintr(::write, fd, Buffer_.Begin() + Position_, Buffer_.Size() - Position_);

            if (size == -1) {
                if (errno == EAGAIN) {
                    return TIOResult(true, bytesWritten);
                }

                return TError("Write failed")
                    << TError::FromSystem();
            }

            YCHECK(size > 0);
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

            YCHECK(size > 0);
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

TError SyncSendTo(int fd, const TSharedRef& buffer, const TNetworkAddress& address) noexcept
{
    auto res = HandleEintr(
        ::sendto,
        fd,
        buffer.Begin(),
        buffer.Size(),
        0, // flags
        address.GetSockAddr(),
        address.GetLength());

    if (res == -1) {
        return TError("Write failed")
            << TError::FromSystem();
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TFDConnectionImpl
    : public IPollable
{
public:
    TFDConnectionImpl(
        int fd,
        const TString& filePath,
        const IPollerPtr& poller,
        bool delayFirstRead)
        : Name_(Format("FDConnection{file://%v}", filePath))
        , FD_(fd)
        , Poller_(poller)
        , DelayFirstRead_(delayFirstRead)
    {
        Init();
    }

    TFDConnectionImpl(
        int fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        const IPollerPtr& poller)
        : Name_(Format("FDConnection{%v<->%v}", localAddress, remoteAddress))
        , LocalAddress_(localAddress)
        , RemoteAddress_(remoteAddress)
        , FD_(fd)
        , Poller_(poller)
    {
        Init();
    }

    virtual const TString& GetLoggingId() const override
    {
        return Name_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        if (Any(control & EPollControl::Write)) {
            DoIO(&WriteDirection_, true);
        }

        if (Any(control & EPollControl::Read)) {
            DoIO(&ReadDirection_, true);
        }
    }

    virtual void OnShutdown() override
    {
        // Poller gurantees that OnShutdown is never executed concurrently with OnEvent()
        // but it may execute concurrently with callback what was posted directly to
        // the poller invoker. In that case we postpone closing the descriptor until
        // callback finished executing.
        {
            auto guard = Guard(Lock_);

            // IO callback is responsible for closing file descriptor and setting future.
            if (ReadDirection_.Starting || WriteDirection_.Starting || SynchronousIOCount_) {
                AbortDelayed_ = true;
                return;
            }

            YCHECK(!Error_.IsOK());
        }

        // At this point Error_, ReadDirection_, WriteDirection_ can't change.
        // We access them directly, without spinlock.
        if (ReadDirection_.Operation) {
            ReadDirection_.Operation->Abort(Error_);
            ReadDirection_.Operation.reset();
        }
        if (WriteDirection_.Operation) {
            WriteDirection_.Operation->Abort(Error_);
            WriteDirection_.Operation.reset();
        }
        FinishShutdown();
    }

    TFuture<size_t> Read(const TSharedMutableRef& data)
    {
        auto read = std::make_unique<TReadOperation>(data, DelayFirstRead_);
        DelayFirstRead_ = false;
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
        EnterSychronousIO();
        auto error = SyncSendTo(FD_, buffer, address);
        ExitSynchronousIO();
        error.ThrowOnError();
    }

    bool SetNoDelay()
    {
        EnterSychronousIO();
        bool ok = TrySetSocketNoDelay(FD_);
        ExitSynchronousIO();
        return ok;
    }

    bool SetKeepAlive()
    {
        EnterSychronousIO();
        bool ok = TrySetSocketKeepAlive(FD_);
        ExitSynchronousIO();
        return ok;
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
        if (ReadTimeout_) {
            TDelayedExecutor::CancelAndClear(ReadTimeout_);
        }

        if (deadline) {
            ReadTimeout_ = TDelayedExecutor::Submit(AbortFromReadTimeout_, deadline);
        }
    }

    void SetWriteDeadline(TInstant deadline)
    {
        if (WriteTimeout_) {
            TDelayedExecutor::CancelAndClear(WriteTimeout_);
        }

        if (deadline) {
            WriteTimeout_ = TDelayedExecutor::Submit(AbortFromWriteTimeout_, deadline);
        }        
    }

private:
    const TString Name_;
    const TNetworkAddress LocalAddress_;
    const TNetworkAddress RemoteAddress_;
    int FD_ = -1;

    struct TIODirection
    {
        std::unique_ptr<IIOOperation> Operation;
        bool Starting = false;
        std::atomic<i64> BytesTransferred = {0};
        TDuration IdleDuration_;
        TDuration BusyDuration_;
        TCpuInstant StartTime_ = GetCpuInstant();

        TClosure DoIO;
        EPollControl PollFlag;

        void StartBusyTimer()
        {
            auto now = GetCpuInstant();
            IdleDuration_ += CpuDurationToDuration(now - StartTime_);
            StartTime_ = now;
        }

        void StopBusyTimer()
        {
            auto now = GetCpuInstant();
            BusyDuration_ += CpuDurationToDuration(now - StartTime_);
            StartTime_ = now;
        }

        TConnectionStatistics GetStatistics() const
        {
            TConnectionStatistics statistics{IdleDuration_, BusyDuration_};
            (Operation ? statistics.BusyDuration : statistics.IdleDuration) += CpuDurationToDuration(GetCpuInstant() - StartTime_);
            return statistics;
        }
    };

    TSpinLock Lock_;
    TIODirection ReadDirection_;
    TIODirection WriteDirection_;
    bool AbortDelayed_ = false;

    int SynchronousIOCount_ = 0;
    
    TError Error_;
    TPromise<void> ShutdownPromise_ = NewPromise<void>();

    EPollControl Control_ = EPollControl::None;
    IPollerPtr Poller_;
    bool DelayFirstRead_ = false;

    TClosure AbortFromReadTimeout_, AbortFromWriteTimeout_;
    TDelayedExecutorCookie ReadTimeout_, WriteTimeout_;

    void Init()
    {
        AbortFromReadTimeout_ = BIND(&TFDConnectionImpl::AbortFromReadTimeout, MakeWeak(this));
        AbortFromWriteTimeout_ = BIND(&TFDConnectionImpl::AbortFromWriteTimeout, MakeWeak(this));
    
        ReadDirection_.DoIO = BIND(&TFDConnectionImpl::DoIO, MakeStrong(this), &ReadDirection_, false);
        ReadDirection_.PollFlag = EPollControl::Read;
        WriteDirection_.DoIO = BIND(&TFDConnectionImpl::DoIO, MakeStrong(this), &WriteDirection_, false);
        WriteDirection_.PollFlag = EPollControl::Write;
        Poller_->Register(this);
    }

    void EnterSychronousIO()
    {
        auto guard = Guard(Lock_);
        Error_.ThrowOnError();
        SynchronousIOCount_++;
    }

    void ExitSynchronousIO()
    {
        bool needClose = false;
        {
            auto guard = Guard(Lock_);
            SynchronousIOCount_--;
            if (AbortDelayed_ && !WriteDirection_.Starting && !ReadDirection_.Starting && !SynchronousIOCount_) {
                needClose = true;
            }
        }

        if (needClose) {
            FinishShutdown();
        }
    }

    void StartIO(TIODirection* direction, std::unique_ptr<IIOOperation> operation)
    {
        TError error;

        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                if (direction->Operation) {
                    THROW_ERROR_EXCEPTION("Another IO operation is in progress")
                        << TErrorAttribute("connection", Name_);
                }
            
                YCHECK(!direction->Operation);
                direction->Operation = std::move(operation);
                direction->StartBusyTimer();

                YCHECK(!direction->Starting);
                direction->Starting = true;
            } else {
                error = Error_;
            }
        }

        if (!error.IsOK()) {
            operation->Abort(error);
            return;
        }

        Poller_->GetInvoker()->Invoke(direction->DoIO);
    }

    void DoIO(TIODirection* direction, bool filterSpuriousEvent)
    {
        {
            auto guard = Guard(Lock_);
            // We use Poller in a way that generates spurious
            // notifications. Do nothing if we are not interested in
            // this event.
            if (filterSpuriousEvent && None(Control_ & direction->PollFlag)) {
                return;
            }

            if (filterSpuriousEvent) {
                Control_ ^= direction->PollFlag;
            }
        }

        YCHECK(direction->Operation);
        auto result = direction->Operation->PerformIO(FD_);
        if (result.IsOK()) {
            direction->BytesTransferred += result.Value().ByteCount;
        } else {
            result << TErrorAttribute("connection", Name_);
        }

        bool needClose = false;
        std::unique_ptr<IIOOperation> operation;
        {
            auto guard = Guard(Lock_);
            if (!result.IsOK()) {
                // IO finished with error.
                operation = std::move(direction->Operation);
                if (Error_.IsOK()) {
                    Error_ = result;
                    UnregisterFromPoller();
                }
                direction->StopBusyTimer();
            } else if (!Error_.IsOK()) {
                // IO was aborted.
                operation = std::move(direction->Operation);
                // Avoid aborting completed IO.
                if (result.Value().Retry) {
                    result = Error_;
                }
                direction->StopBusyTimer();
            } else if (result.Value().Retry) {
                // IO not completed.
                Control_ |= direction->PollFlag;
            } else {
                // IO finished successfully.
                operation = std::move(direction->Operation);
                direction->StopBusyTimer();
              }

            direction->Starting = false;
            if (AbortDelayed_ && !WriteDirection_.Starting && !ReadDirection_.Starting && !SynchronousIOCount_) {
                // Object is in the middle of shutdown. We are responsible for closing FD_.
                needClose = true;
            }

            MaybeRearm();
        }

        if (!result.IsOK()) {
            operation->Abort(result);
        } else if (!result.Value().Retry) {
            operation->SetResult();
        }

        if (needClose) {
            FinishShutdown();
        }
    }

    TFuture<void> AbortIO(const TError& error)
    {
        auto guard = Guard(Lock_);
        if (Error_.IsOK()) {
            Error_ = error;
            UnregisterFromPoller();
        }
        return ShutdownPromise_.ToFuture();
    }

    void UnregisterFromPoller()
    {
        Poller_->Unarm(FD_);
        Poller_->Unregister(this);
    }

    void MaybeRearm()
    {
        if (Control_ != EPollControl::None) {
            Poller_->Arm(FD_, this, Control_);
        }
    }

    void FinishShutdown()
    {
        YCHECK(TryClose(FD_, false));
        FD_ = -1;

        ReadDirection_.DoIO.Reset();
        WriteDirection_.DoIO.Reset();
        ShutdownPromise_.Set();

        if (WriteTimeout_) {
            TDelayedExecutor::CancelAndClear(WriteTimeout_);
        }

        if (ReadTimeout_) {
            TDelayedExecutor::CancelAndClear(ReadTimeout_);
        }
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

DECLARE_REFCOUNTED_CLASS(TFDConnectionImpl)
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
        TIntrusivePtr<TRefCounted> pipeHolder = nullptr,
        bool delayFirstRead = true)
        : Impl_(New<TFDConnectionImpl>(fd, pipePath, poller, delayFirstRead))
        , PipeHolder_(std::move(pipeHolder))
    { }

    TFDConnection(
        int fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        const IPollerPtr& poller)
        : Impl_(New<TFDConnectionImpl>(fd, localAddress, remoteAddress, poller))
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
        YCHECK(TryClose(fds[0], false));
        YCHECK(TryClose(fds[1], false));
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

    return New<TFDConnection>(fd, pipePath, poller, pipeHolder, true);
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
    return New<TFDConnection>(fd, pipePath, poller, pipeHolder, false);
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
        : Impl_(New<TFDConnectionImpl>(fd, localAddress, TNetworkAddress{}, poller))
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
