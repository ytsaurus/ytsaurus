#include "stdafx.h"
#include "tcp_connection.h"
#include "tcp_dispatcher_impl.h"
#include "server.h"
#include "config.h"

#include <core/misc/string.h>

#include <core/rpc/public.h>

#include <core/ytree/convert.h>

#include <core/profiling/profile_manager.h>

#include <util/system/error.h>

#include <errno.h>

#ifndef _win_
    #include <netinet/tcp.h>
#endif

namespace NYT {
namespace NBus {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const size_t MinBatchReadSize = 16 * 1024;
static const size_t MaxBatchReadSize = 64 * 1024;

static const size_t MaxFragmentsPerWrite = 256;
static const size_t MaxBatchWriteSize    = 64 * 1024;
static const size_t MaxWriteCoalesceSize = 4 * 1024;

static NProfiling::TAggregateCounter ReceiveTime("/receive_time");
static NProfiling::TAggregateCounter ReceiveSize("/receive_size");
static NProfiling::TAggregateCounter InHandlerTime("/in_handler_time");
static NProfiling::TRateCounter InThroughputCounter("/in_throughput");
static NProfiling::TRateCounter InCounter("/in_rate");

static NProfiling::TAggregateCounter SendTime("/send_time");
static NProfiling::TAggregateCounter SendSize("/send_size");
static NProfiling::TAggregateCounter OutHandlerTime("/out_handler_time");
static NProfiling::TRateCounter OutThroughputCounter("/out_throughput");
static NProfiling::TRateCounter OutCounter("/out_rate");
static NProfiling::TAggregateCounter PendingOutCounter("/pending_out_count");
static NProfiling::TAggregateCounter PendingOutSize("/pending_out_size");

////////////////////////////////////////////////////////////////////////////////

struct TTcpConnectionTag { };

TTcpConnection::TTcpConnection(
    TTcpBusConfigPtr config,
    TTcpDispatcherThreadPtr dispatcherThread,
    EConnectionType connectionType,
    ETcpInterfaceType interfaceType,
    const TConnectionId& id,
    int socket,
    const Stroka& address,
    int priority,
    IMessageHandlerPtr handler)
    : Config_(std::move(config))
    , DispatcherThread_(std::move(dispatcherThread))
    , ConnectionType_(connectionType)
    , InterfaceType_(interfaceType)
    , Id_(id)
    , Socket_(socket)
    , Fd_(INVALID_SOCKET)
    , Address_(address)
#ifdef _linux_
    , Priority_(priority)
#endif
    , Handler_(handler)
    , Logger(BusLogger)
    , Profiler(BusProfiler)
    , MessageEnqueuedCallback_(BIND(&TTcpConnection::OnMessageEnqueuedThunk, MakeWeak(this)))
    , ReadBuffer_(TTcpConnectionTag(), MinBatchReadSize)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(handler);

    Logger.AddTag("ConnectionId: %v, Address: %v",
        id,
        Address_);

    auto tagId = NProfiling::TProfileManager::Get()->RegisterTag("interface", FormatEnum(InterfaceType_));
    Profiler.TagIds().push_back(tagId);

    switch (ConnectionType_) {
        case EConnectionType::Client:
            YCHECK(Socket_ == INVALID_SOCKET);
            SetState(ETcpConnectionState::Resolving);
            break;

        case EConnectionType::Server:
            YCHECK(Socket_ != INVALID_SOCKET);
            SetState(ETcpConnectionState::Opening);
            break;

        default:
            YUNREACHABLE();
    }

    MessageEnqueuedCallbackPending_.store(false);

    WriteBuffers_.push_back(std::make_unique<TBlob>(TTcpConnectionTag()));
    WriteBuffers_[0]->Reserve(MaxBatchWriteSize);

    UpdateConnectionCount(+1);
}

TTcpConnection::~TTcpConnection()
{
    CloseSocket();
    Cleanup();
}

ETcpConnectionState TTcpConnection::GetState() const
{
    return ETcpConnectionState(State_.load());
}

void TTcpConnection::SetState(ETcpConnectionState state)
{
    State_.store(static_cast<int>(state));
}

void TTcpConnection::Cleanup()
{
    DiscardOutcomingMessages(TError(
        NRpc::EErrorCode::TransportError,
        "Connection closed"));

    while (!QueuedPackets_.empty()) {
        auto* packet = QueuedPackets_.front();
        UpdatePendingOut(-1, -packet->Size);
        delete packet;
        QueuedPackets_.pop();
    }

    while (!EncodedPackets_.empty()) {
        auto* packet = EncodedPackets_.front();
        UpdatePendingOut(-1, -packet->Size);
        delete packet;
        EncodedPackets_.pop();
    }

    EncodedFragments_.clear();
}

void TTcpConnection::SyncInitialize()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    switch (ConnectionType_) {
        case EConnectionType::Client:
            SyncResolve();
            break;

        case EConnectionType::Server:
            InitFd();
            InitSocketWatcher();
            SyncOpen();
            break;

        default:
            YUNREACHABLE();
    }
}

void TTcpConnection::SyncFinalize()
{
    SyncClose(TError(NRpc::EErrorCode::TransportError, "Bus terminated"));
}

Stroka TTcpConnection::GetLoggingId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Format("ConnectionId: %v", Id_);
}

TTcpDispatcherStatistics& TTcpConnection::Statistics()
{
    return DispatcherThread_->Statistics(InterfaceType_);
}

void TTcpConnection::UpdateConnectionCount(int delta)
{
    switch (ConnectionType_) {
        case EConnectionType::Client: {
            int value = (Statistics().ClientConnectionCount += delta);
            Profiler.Enqueue("/client_connection_count", value);
            break;
        }

        case EConnectionType::Server: {
            int value = (Statistics().ServerConnectionCount += delta);
            Profiler.Enqueue("/server_connection_count", value);
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TTcpConnection::UpdatePendingOut(int countDelta, i64 sizeDelta)
{
    {
        int value = (Statistics().PendingOutCount += countDelta);
        Profiler.Aggregate(PendingOutCounter, value);
    }
    {
        size_t value = (Statistics().PendingOutSize += sizeDelta);
        Profiler.Aggregate(PendingOutSize, value);
    }
}

const TConnectionId& TTcpConnection::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

void TTcpConnection::SyncOpen()
{
    SetState(ETcpConnectionState::Open);

    LOG_DEBUG("Connection established");

    // Flush messages that were enqueued when the connection was still opening.
    ProcessOutcomingMessages();
    
    // Simulate read-write notification.
    OnSocket(*SocketWatcher_, ev::READ|ev::WRITE);
}

void TTcpConnection::SyncResolve()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TStringBuf hostName;
    try {
        ParseServiceAddress(Address_, &hostName, &Port_);
    } catch (const std::exception& ex) {
        SyncClose(TError(ex).SetCode(NRpc::EErrorCode::TransportError));
        return;
    }

    if (InterfaceType_ == ETcpInterfaceType::Local) {
        LOG_DEBUG("Address resolved as local, connecting");

        auto netAddress = GetLocalBusAddress(Port_);
        OnAddressResolved(netAddress);
    } else {
        TAddressResolver::Get()->Resolve(Stroka(hostName)).Subscribe(
            BIND(&TTcpConnection::OnAddressResolutionFinished, MakeStrong(this))
                .Via(DispatcherThread_->GetInvoker()));
    }
}

void TTcpConnection::OnAddressResolutionFinished(TErrorOr<TNetworkAddress> result)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    if (!result.IsOK()) {
        SyncClose(result);
        return;
    }

    LOG_DEBUG("Address resolved, connecting");

    TNetworkAddress netAddress(result.Value(), Port_);
    OnAddressResolved(netAddress);
}

void TTcpConnection::OnAddressResolved(const TNetworkAddress& netAddress)
{
    try {
        ConnectSocket(netAddress);
    } catch (const std::exception& ex) {
        SyncClose(TError(ex).SetCode(NRpc::EErrorCode::TransportError));
        return;
    }

    InitSocketWatcher();

    SetState(ETcpConnectionState::Opening);
}

void TTcpConnection::SyncClose(const TError& error)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(!error.IsOK());

    // Check for second close attempt.
    if (GetState() == ETcpConnectionState::Closed) {
        return;
    }

    SetState(ETcpConnectionState::Closed);

    // Stop all watchers.
    SocketWatcher_.reset();

    // Close the socket.
    CloseSocket();

    // Mark all unacked messages as failed.
    DiscardUnackedMessages(error);

    // Mark all queued messages as failed.
    DiscardOutcomingMessages(error);

    // Release memory.
    Cleanup();

    // Construct a detailed error.
    auto detailedError = error
        << TErrorAttribute("connection_id", Id_)
        << TErrorAttribute("address", Address_);

    LOG_DEBUG(detailedError, "Connection closed");

    // Invoke user callback.
    PROFILE_TIMING ("/terminate_handler_time") {
        TerminatedPromise_.Set(detailedError);
    }

    UpdateConnectionCount(-1);

    DispatcherThread_->AsyncUnregister(this);
}

void TTcpConnection::InitFd()
{
#ifdef _win_
    Fd_ = _open_osfhandle(Socket_, 0);
#else
    Fd_ = Socket_;
#endif
}

void TTcpConnection::InitSocketWatcher()
{
    SocketWatcher_.reset(new ev::io(DispatcherThread_->GetEventLoop()));
    SocketWatcher_->set<TTcpConnection, &TTcpConnection::OnSocket>(this);
    SocketWatcher_->start(Fd_, ev::READ|ev::WRITE);
}

void TTcpConnection::CloseSocket()
{
    if (Fd_ != INVALID_SOCKET) {
        close(Fd_);
        Socket_ = INVALID_SOCKET;
        Fd_ = INVALID_SOCKET;
    }
}

void TTcpConnection::ConnectSocket(const TNetworkAddress& netAddress)
{
    int family = netAddress.GetSockAddr()->sa_family;
    int protocol = family == AF_UNIX ? 0 : IPPROTO_TCP;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
#endif

    Socket_ = socket(family, type, protocol);
    if (Socket_ == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION("Failed to create client socket")
            << TError::FromSystem();
    }

    InitFd();

    if (family == AF_INET6) {
        int value = 0;
        if (setsockopt(Socket_, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &value, sizeof(value)) != 0) {
            THROW_ERROR_EXCEPTION("Failed to configure IPv6 protocol")
                << TError::FromSystem();
        }
    }

    if (Config_->EnableNoDelay && family != AF_UNIX) {
        if (Config_->EnableNoDelay) {
            int value = 1;
            if (setsockopt(Socket_, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to enable socket NODELAY mode")
                    << TError::FromSystem();
            }
        }
#ifdef _linux_
        {
            if (setsockopt(Socket_, SOL_SOCKET, SO_PRIORITY, (const char*) &Priority_, sizeof(Priority_)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to set socket priority")
                    << TError::FromSystem();
            }
        }
#endif
        {
            int value = 1;
            if (setsockopt(Socket_, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to enable keep alive")
                    << TError::FromSystem();
            }
        }
    }

    {
#ifdef _win_
        unsigned long value = 1;
        int result = ioctlsocket(Socket_, FIONBIO, &value);
#else
        int result = fcntl(Socket_, F_SETFL, O_NONBLOCK);
#endif
        if (result != 0) {
            THROW_ERROR_EXCEPTION("Failed to enable nonblocking mode")
                << TError::FromSystem();
        }
    }

    {
        int result;
        PROFILE_TIMING ("/connect_time") {
            do {
                result = connect(Socket_, netAddress.GetSockAddr(), netAddress.GetLength());
            } while (result < 0 && errno == EINTR);
        }

        if (result != 0) {
            int error = LastSystemError();
            if (IsSocketError(error)) {
                THROW_ERROR_EXCEPTION("Error connecting to %v", Address_)
                    << TError::FromSystem(error);
            }
        }
    }
}

TYsonString TTcpConnection::GetEndpointDescription() const
{
    return ConvertToYsonString(Address_);
}

TFuture<void> TTcpConnection::Send(TSharedRefArray message, EDeliveryTrackingLevel level)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueuedMessage queuedMessage(std::move(message), level);

    // NB: Log first to avoid producing weird traces.
    LOG_DEBUG("Outcoming message enqueued (PacketId: %v)",
        queuedMessage.PacketId);

    QueuedMessages_.Enqueue(queuedMessage);

    if (!MessageEnqueuedCallbackPending_.load(std::memory_order_relaxed)) {
        bool expected = false;
        if (MessageEnqueuedCallbackPending_.compare_exchange_strong(expected, true)) {
            DispatcherThread_->GetInvoker()->Invoke(MessageEnqueuedCallback_);
        }
    }

    return queuedMessage.Promise;
}

void TTcpConnection::Terminate(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!error.IsOK());

    {
        // Check if another termination request is already in progress.
        TGuard<TSpinLock> guard(TerminationSpinLock_);
        if (!TerminationError_.IsOK()) {
            return;
        }
        TerminationError_ = error;
    }

    LOG_DEBUG("Bus termination requested");

    DispatcherThread_->GetInvoker()->Invoke(BIND(
        &TTcpConnection::OnTerminated,
        MakeStrong(this)));
}

void TTcpConnection::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    TerminatedPromise_.ToFuture().Subscribe(callback);
}

void TTcpConnection::UnsubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/)
{
    YUNREACHABLE();
}

void TTcpConnection::OnSocket(ev::io&, int revents)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YASSERT(GetState() != ETcpConnectionState::Closed);

    if (revents & ev::ERROR) {
        SyncClose(TError(NRpc::EErrorCode::TransportError, "Socket failed"));
        return;
    }

    // NB: Try to read from the socket before writing into it to avoid
    // getting SIGPIPE when other party closes the connection.

    if (revents & ev::READ) {
        OnSocketRead();
    }

    if (revents & ev::WRITE) {
        OnSocketWrite();
    }

    UpdateSocketWatcher();
}

void TTcpConnection::OnSocketRead()
{
    if (GetState() == ETcpConnectionState::Closed) {
        return;
    }

    LOG_TRACE("Started serving read request");
    size_t bytesReadTotal = 0;

    while (true) {
        // Check if the decoder is expecting a chunk of large enough size.
        auto decoderChunk = Decoder_.GetFragment();
        size_t decoderChunkSize = decoderChunk.Size();
        LOG_TRACE("Decoder needs %v bytes", decoderChunkSize);

        if (decoderChunkSize >= MinBatchReadSize) {
            // Read directly into the decoder buffer.
            size_t bytesToRead = std::min(decoderChunkSize, MaxBatchReadSize);
            LOG_TRACE("Reading %v bytes into decoder", bytesToRead);
            size_t bytesRead;
            if (!ReadSocket(decoderChunk.Begin(), bytesToRead, &bytesRead)) {
                break;
            }
            bytesReadTotal += bytesRead;

            if (!AdvanceDecoder(bytesRead)) {
                return;
            }
        } else {
            // Read a chunk into the read buffer.
            LOG_TRACE("Reading %v bytes into buffer", ReadBuffer_.Size());
            size_t bytesRead;
            if (!ReadSocket(ReadBuffer_.Begin(), ReadBuffer_.Size(), &bytesRead)) {
                break;
            }
            bytesReadTotal += bytesRead;

            // Feed the read buffer to the decoder.
            const char* recvBegin = ReadBuffer_.Begin();
            size_t recvRemaining = bytesRead;
            while (recvRemaining != 0) {
                decoderChunk = Decoder_.GetFragment();
                decoderChunkSize = decoderChunk.Size();
                size_t bytesToCopy = std::min(recvRemaining, decoderChunkSize);
                LOG_TRACE("Decoder chunk size is %v bytes, copying %v bytes",
                    decoderChunkSize,
                    bytesToCopy);
                std::copy(recvBegin, recvBegin + bytesToCopy, decoderChunk.Begin());
                if (!AdvanceDecoder(bytesToCopy)) {
                    return;
                }
                recvBegin += bytesToCopy;
                recvRemaining -= bytesToCopy;
            }
            LOG_TRACE("Buffer exhausted");
        }
    }

    LOG_TRACE("Finished serving read request, %v bytes read total", bytesReadTotal);
}

bool TTcpConnection::ReadSocket(char* buffer, size_t size, size_t* bytesRead)
{
    ssize_t result;
    PROFILE_AGGREGATED_TIMING (ReceiveTime) {
        do {
            result = recv(Socket_, buffer, size, 0);
        } while (result < 0 && errno == EINTR);
    }

    if (!CheckReadError(result)) {
        *bytesRead = 0;
        return false;
    }

    *bytesRead = result;

    Profiler.Increment(InThroughputCounter, *bytesRead);
    Profiler.Aggregate(ReceiveSize, *bytesRead);

    LOG_TRACE("%v bytes read", *bytesRead);

#if !defined(_win_) && !defined(__APPLE__)
    if (Config_->EnableQuickAck) {
        int value = 1;
        setsockopt(Socket_, IPPROTO_TCP, TCP_QUICKACK, (const char*) &value, sizeof(value));
    }
#endif

    return true;
}

bool TTcpConnection::CheckReadError(ssize_t result)
{
    if (result == 0) {
        SyncClose(TError(NRpc::EErrorCode::TransportError, "Socket was closed"));
        return false;
    }

    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            auto wrappedError = TError(
                NRpc::EErrorCode::TransportError,
                "Socket read error")
                << TError::FromSystem(error);
            LOG_DEBUG(wrappedError);
            SyncClose(wrappedError);
        }
        return false;
    }

    return true;
}

bool TTcpConnection::AdvanceDecoder(size_t size)
{
    if (!Decoder_.Advance(size)) {
        SyncClose(TError(NRpc::EErrorCode::TransportError, "Error decoding incoming packet"));
        return false;
    }

    if (Decoder_.IsFinished()) {
        bool result = OnPacketReceived();
        Decoder_.Restart();
        return result;
    }

    return true;
}

bool TTcpConnection::OnPacketReceived()
{
    Profiler.Increment(InCounter);
    switch (Decoder_.GetPacketType()) {
        case EPacketType::Ack:
            return OnAckPacketReceived();
        case EPacketType::Message:
            return OnMessagePacketReceived();
        default:
            YUNREACHABLE();
    }
}

bool TTcpConnection::OnAckPacketReceived()
{
    if (UnackedMessages_.empty()) {
        LOG_ERROR("Unexpected ack received");
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Unexpected ack received"));
        return false;
    }

    auto& unackedMessage = UnackedMessages_.front();

    if (Decoder_.GetPacketId() != unackedMessage.PacketId) {
        LOG_ERROR("Ack for invalid packet ID received: expected %v, found %v",
            unackedMessage.PacketId,
            Decoder_.GetPacketId());
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Ack for invalid packet ID received"));
        return false;
    }

    LOG_DEBUG("Ack received (PacketId: %v)", Decoder_.GetPacketId());

    PROFILE_AGGREGATED_TIMING (OutHandlerTime) {
        if (unackedMessage.Promise) {
            unackedMessage.Promise.Set(TError());
        }
    }

    UnackedMessages_.pop();

    return true;
}

bool TTcpConnection::OnMessagePacketReceived()
{
    LOG_DEBUG("Incoming message received (PacketId: %v, PacketSize: %v)",
        Decoder_.GetPacketId(),
        Decoder_.GetPacketSize());

    if (Any(Decoder_.GetPacketFlags() & EPacketFlags::RequestAck)) {
        EnqueuePacket(EPacketType::Ack, EPacketFlags::None, Decoder_.GetPacketId());
    }

    auto message = Decoder_.GetMessage();
    PROFILE_AGGREGATED_TIMING (InHandlerTime) {
        Handler_->OnMessage(std::move(message), this);
    }

    return true;
}

void TTcpConnection::EnqueuePacket(
    EPacketType type,
    EPacketFlags flags,
    const TPacketId& packetId,
    TSharedRefArray message)
{
    i64 size = TPacketEncoder::GetPacketSize(type, message);
    QueuedPackets_.push(new TPacket(type, flags, packetId, message, size));
    UpdatePendingOut(+1, +size);
}

void TTcpConnection::OnSocketWrite()
{
    if (GetState() == ETcpConnectionState::Closed) {
        return;
    }

    // For client sockets the first write notification means that
    // connection was established (either successfully or not).
    if (ConnectionType_ == EConnectionType::Client && GetState() == ETcpConnectionState::Opening) {
        // Check if connection was established successfully.
        int error = GetSocketError();
        if (error != 0) {
            auto wrappedErrror = TError(
                NRpc::EErrorCode::TransportError,
                "Failed to connect to %v",
                Address_)
                << TError::FromSystem(error);
            LOG_ERROR(wrappedErrror);

            // We're currently in event loop context, so calling |SyncClose| is safe.
            SyncClose(wrappedErrror);

            return;
        }
        SyncOpen();
    }

    LOG_TRACE("Started serving write request");

    size_t bytesWrittenTotal = 0;
    while (HasUnsentData()) {
        if (!MaybeEncodeFragments()) {
            break;
        }

        size_t bytesWritten;
        bool success = WriteFragments(&bytesWritten);
        bytesWrittenTotal += bytesWritten;
        
        FlushWrittenFragments(bytesWritten);
        FlushWrittenPackets(bytesWritten);

        if (!success) {
            break;
        }
    }

    LOG_TRACE("Finished serving write request, %v bytes written total", bytesWrittenTotal);
}

bool TTcpConnection::HasUnsentData() const
{
    return !EncodedFragments_.empty() || !QueuedPackets_.empty();
}

bool TTcpConnection::WriteFragments(size_t* bytesWritten)
{
    LOG_TRACE("Writing fragments, %v encoded", EncodedFragments_.size());

    auto fragmentIt = EncodedFragments_.begin();
    auto fragmentEnd = EncodedFragments_.end();

    SendVector_.clear();
    size_t bytesAvailable = MaxBatchWriteSize;

    while (fragmentIt != fragmentEnd &&
           SendVector_.size() < MaxFragmentsPerWrite &&
           bytesAvailable > 0)
    {
        const auto& fragment = *fragmentIt;
        size_t size = std::min(fragment.Size(), bytesAvailable);
#ifdef _win_
        WSABUF item;
        item.buf = fragment.Begin();
        item.len = static_cast<ULONG>(size);
        SendVector_.push_back(item);
#else
        struct iovec item;
        item.iov_base = fragment.Begin();
        item.iov_len = size;
        SendVector_.push_back(item);
#endif
        EncodedFragments_.move_forward(fragmentIt);
        bytesAvailable -= size;
    }

    ssize_t result;
#ifdef _win_
    DWORD bytesWritten_ = 0;
    PROFILE_AGGREGATED_TIMING (SendTime) {
        result = WSASend(Socket_, SendVector_.data(), SendVector_.size(), &bytesWritten_, 0, NULL, NULL);
    }
    *bytesWritten = static_cast<size_t>(bytesWritten_);
#else
    PROFILE_AGGREGATED_TIMING (SendTime) {
        do {
            result = writev(Socket_, SendVector_.data(), SendVector_.size());
        } while (result < 0 && errno == EINTR);
    }
    *bytesWritten = result >= 0 ? result : 0;
#endif

    Profiler.Increment(OutThroughputCounter, *bytesWritten);
    Profiler.Aggregate(SendSize, *bytesWritten);

    LOG_TRACE("%v bytes written", *bytesWritten);

    return CheckWriteError(result);
}

void TTcpConnection::FlushWrittenFragments(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    LOG_TRACE("Flushing fragments, %v bytes written", bytesWritten);

    while (bytesToFlush != 0) {
        YASSERT(!EncodedFragments_.empty());
        auto& fragment = EncodedFragments_.front();

        if (fragment.Size() > bytesToFlush) {
            size_t bytesRemaining = fragment.Size() - bytesToFlush;
            LOG_TRACE("Partial write (Size: %v, RemainingSize: %v)",
                fragment.Size(),
                bytesRemaining);
            fragment = TRef(fragment.End() - bytesRemaining, bytesRemaining);
            break;
        }

        LOG_TRACE("Full write (Size: %v)", fragment.Size());

        bytesToFlush -= fragment.Size();
        EncodedFragments_.pop();
    }
}

void TTcpConnection::FlushWrittenPackets(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    LOG_TRACE("Flushing packets, %v bytes written", bytesWritten);

    while (bytesToFlush != 0) {
        YASSERT(!EncodedPacketSizes_.empty());
        auto& packetSize = EncodedPacketSizes_.front();

        if (packetSize > bytesToFlush) {
            size_t bytesRemaining = packetSize - bytesToFlush;
            LOG_TRACE("Partial write (Size: %v, RemainingSize: %v)",
                packetSize,
                bytesRemaining);
            packetSize = bytesRemaining;
            break;
        }

        LOG_TRACE("Full write (Size: %v)", packetSize);

        bytesToFlush -= packetSize;
        OnPacketSent();
        EncodedPacketSizes_.pop();
    }
}

bool TTcpConnection::MaybeEncodeFragments()
{
    if (!EncodedFragments_.empty() || QueuedPackets_.empty()) {
        return true;
    }

    // Discard all buffer except for a single one.
    WriteBuffers_.resize(1);
    auto* buffer = WriteBuffers_.back().get();
    buffer->Clear();

    size_t encodedSize = 0;
    size_t coalescedSize = 0;

    auto flushCoalesced = [&] () {
        if (coalescedSize > 0) {
            EncodedFragments_.push(TRef(buffer->End() - coalescedSize, coalescedSize));
            coalescedSize = 0;
        }
    };

    auto coalesce = [&] (const TRef& fragment) {
        if (buffer->Size() + fragment.Size() > buffer->Capacity()) {
            // Make sure we never reallocate.
            flushCoalesced();
            WriteBuffers_.push_back(std::make_unique<TBlob>(TTcpConnectionTag()));
            buffer = WriteBuffers_.back().get();
            buffer->Reserve(std::max(MaxBatchWriteSize, fragment.Size()));
        }
        buffer->Append(fragment);
        coalescedSize += fragment.Size();
    };

    while (EncodedFragments_.size() < MaxFragmentsPerWrite &&
           encodedSize <= MaxBatchWriteSize &&
           !QueuedPackets_.empty())
    {
        // Move the packet from queued to encoded.
        auto* packet = QueuedPackets_.front();
        QueuedPackets_.pop();
        EncodedPackets_.push(packet);

        // Encode the packet.
        LOG_TRACE("Starting encoding packet (PacketId: %v)", packet->PacketId);

        bool encodeResult = Encoder_.Start(
            packet->Type,
            packet->Flags,
            packet->PacketId,
            packet->Message);
        if (!encodeResult) {
            SyncClose(TError(NRpc::EErrorCode::TransportError, "Error encoding outcoming packet"));
            return false;
        }

        do {
            auto fragment = Encoder_.GetFragment();
            if (!Encoder_.IsFragmentOwned() || fragment.Size() <= MaxWriteCoalesceSize) {
                coalesce(fragment);
            } else {
                flushCoalesced();
                EncodedFragments_.push(fragment);
            }
            LOG_TRACE("Fragment encoded (Size: %v)", fragment.Size());
            Encoder_.NextFragment();
        } while (!Encoder_.IsFinished());

        EncodedPacketSizes_.push(packet->Size);
        encodedSize += packet->Size;

        LOG_TRACE("Finished encoding packet (PacketId: %v)", packet->PacketId);
    }
    
    flushCoalesced();

    return true;
}

bool TTcpConnection::CheckWriteError(ssize_t result)
{
    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            auto wrappedError = TError(
                NRpc::EErrorCode::TransportError,
                "Socket write error")
                << TError::FromSystem(error);
            LOG_WARNING(wrappedError);
            SyncClose(wrappedError);
        }
        return false;
    }

    return true;
}

void TTcpConnection::OnPacketSent()
{
    const auto* packet = EncodedPackets_.front();
    switch (packet->Type) {
        case EPacketType::Ack:
            OnAckPacketSent(*packet);
            break;

        case EPacketType::Message:
            OnMessagePacketSent(*packet);
            break;

        default:
            YUNREACHABLE();
    }


    UpdatePendingOut(-1, -packet->Size);
    Profiler.Increment(OutCounter);

    delete packet;
    EncodedPackets_.pop();
}

void  TTcpConnection::OnAckPacketSent(const TPacket& packet)
{
    LOG_DEBUG("Ack sent (PacketId: %v)",
        packet.PacketId);
}

void TTcpConnection::OnMessagePacketSent(const TPacket& packet)
{
    LOG_DEBUG("Outcoming message sent (PacketId: %v, PacketSize: %v)",
        packet.PacketId,
        packet.Size);
}

void TTcpConnection::OnMessageEnqueuedThunk(const TWeakPtr<TTcpConnection>& weakConnection)
{
    auto strongConnection = weakConnection.Lock();
    if (strongConnection) {
        strongConnection->OnMessageEnqueued();
    }
}

void TTcpConnection::OnMessageEnqueued()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    MessageEnqueuedCallbackPending_.store(false);

    switch (GetState()) {
        case ETcpConnectionState::Resolving:
        case ETcpConnectionState::Opening:
            // Do nothing.
            break;

        case ETcpConnectionState::Open:
            ProcessOutcomingMessages();
            UpdateSocketWatcher();
            break;

        case ETcpConnectionState::Closed:
            DiscardOutcomingMessages(TError(
                NRpc::EErrorCode::TransportError,
                "Connection closed"));
            break;

        default:
            YUNREACHABLE();
    }
}

void TTcpConnection::ProcessOutcomingMessages()
{
    auto messages = QueuedMessages_.DequeueAll();

    for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
        auto& queuedMessage = *it;

        auto flags = queuedMessage.Level == EDeliveryTrackingLevel::Full
            ? EPacketFlags::RequestAck
            : EPacketFlags::None;

        LOG_DEBUG("Outcoming message dequeued (PacketId: %v, Flags: %v)",
            queuedMessage.PacketId,
            flags);

        EnqueuePacket(
            EPacketType::Message,
            flags,
            queuedMessage.PacketId,
            queuedMessage.Message);

        if (Any(flags & EPacketFlags::RequestAck)) {
            TUnackedMessage unackedMessage(queuedMessage.PacketId, std::move(queuedMessage.Promise));
            UnackedMessages_.push(unackedMessage);            
        } else if (queuedMessage.Promise) {
            queuedMessage.Promise.Set();
        }
    }
}

void TTcpConnection::DiscardOutcomingMessages(const TError& error)
{
    TQueuedMessage queuedMessage;
    while (QueuedMessages_.Dequeue(&queuedMessage)) {
        LOG_DEBUG("Outcoming message dequeued (PacketId: %v)",
            queuedMessage.PacketId);
        if (queuedMessage.Promise) {
            queuedMessage.Promise.Set(error);
        }
    }
}

void TTcpConnection::DiscardUnackedMessages(const TError& error)
{
    while (!UnackedMessages_.empty()) {
        auto& message = UnackedMessages_.front();
        if (message.Promise) {
            message.Promise.Set(error);
        }
        UnackedMessages_.pop();
    }
}

void TTcpConnection::UpdateSocketWatcher()
{
    if (GetState() == ETcpConnectionState::Open) {
        SocketWatcher_->set(HasUnsentData() ? ev::READ|ev::WRITE : ev::READ);
    }
}

void TTcpConnection::OnTerminated()
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    
    if (GetState() == ETcpConnectionState::Closed) {
        return;
    }

    TError error;
    {
        TGuard<TSpinLock> guard(TerminationSpinLock_);
        error = TerminationError_;
    }

    SyncClose(error);
}

int TTcpConnection::GetSocketError() const
{
    int error;
    socklen_t errorLen = sizeof (error);
    getsockopt(Socket_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &errorLen);
    return error;
}

bool TTcpConnection::IsSocketError(ssize_t result)
{
#ifdef _win_
    return
        result != WSAEWOULDBLOCK &&
        result != WSAEINPROGRESS;
#else
    return
        result != EWOULDBLOCK &&
        result != EAGAIN &&
        result != EINPROGRESS;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

