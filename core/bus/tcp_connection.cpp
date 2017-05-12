#include "tcp_connection.h"
#include "config.h"
#include "server.h"
#include "tcp_dispatcher_impl.h"

#include <yt/core/misc/enum.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/profiling/timing.h>

#include <util/system/error.h>

#include <errno.h>

#ifdef _unix_
    #include <netinet/tcp.h>
#endif

namespace NYT {
namespace NBus {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const size_t MinBatchReadSize = 16 * 1024;
static const size_t MaxBatchReadSize = 64 * 1024;

static const size_t MaxFragmentsPerWrite = 256;
static const size_t MaxBatchWriteSize    = 64 * 1024;
static const size_t MaxWriteCoalesceSize = 4 * 1024;

////////////////////////////////////////////////////////////////////////////////

struct TTcpConnectionReadBufferTag { };
struct TTcpConnectionWriteBufferTag { };

////////////////////////////////////////////////////////////////////////////////

TTcpConnection::TTcpConnection(
    TTcpBusConfigPtr config,
    TTcpDispatcherThreadPtr dispatcherThread,
    EConnectionType connectionType,
    TNullable<ETcpInterfaceType> interfaceType,
    const TConnectionId& id,
    int socket,
    const TString& endpointDescription,
    const IAttributeDictionary& endpointAttributes,
    const TNullable<TString>& address,
    const TNullable<TString>& unixDomainName,
    int priority,
    IMessageHandlerPtr handler)
    : Config_(std::move(config))
    , DispatcherThread_(std::move(dispatcherThread))
    , ConnectionType_(connectionType)
    , Id_(id)
    , Socket_(socket)
    , EndpointDescription_(endpointDescription)
    , EndpointAttributes_(endpointAttributes.Clone())
    , Address_(address)
    , UnixDomainName_(unixDomainName)
#ifdef _linux_
    , Priority_(priority)
#endif
    , Handler_(std::move(handler))
    , Logger(NLogging::TLogger(BusLogger)
        .AddTag("ConnectionId: %v, RemoteAddress: %v",
            Id_,
            EndpointDescription_))
    , InterfaceType_(interfaceType)
    , EnableChecksums_(Config_->CalculateChecksum)
    , MessageEnqueuedCallback_(BIND(&TTcpConnection::OnMessageEnqueuedThunk, MakeWeak(this)))
    , Decoder_(Logger, Config_->VerifyChecksum)
    , ReadStallTimeout_(NProfiling::DurationToCpuDuration(Config_->ReadStallTimeout))
    , Encoder_(Logger)
    , WriteStallTimeout_(NProfiling::DurationToCpuDuration(Config_->WriteStallTimeout))
{
    VERIFY_THREAD_AFFINITY_ANY();
    Y_ASSERT(Handler_);
    Y_ASSERT(DispatcherThread_);

    switch (ConnectionType_) {
        case EConnectionType::Client:
            YCHECK(Socket_ == INVALID_SOCKET);
            State_ = EState::Resolving;
            break;

        case EConnectionType::Server:
            YCHECK(interfaceType);
            YCHECK(Socket_ != INVALID_SOCKET);
            State_ = EState::Opening;
            OnInterfaceTypeEstablished(*interfaceType);
            break;

        default:
            Y_UNREACHABLE();
    }
}

TTcpConnection::~TTcpConnection()
{
    CloseSocket();
    Cleanup();
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

    InitBuffers();

    switch (ConnectionType_) {
        case EConnectionType::Client:
            SyncResolve();
            break;

        case EConnectionType::Server:
            InitFD();
            InitSocketWatcher();
            SyncOpen();
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TTcpConnection::SyncFinalize()
{
    SyncClose(TError(NRpc::EErrorCode::TransportError, "Bus terminated"));
}

void TTcpConnection::SyncCheck()
{
    if (State_ != EState::Open) {
        return;
    }

    auto now = NProfiling::GetCpuInstant();

    if (HasUnsentData() && LastWriteScheduleTime_ < now - WriteStallTimeout_) {
        ++Statistics_->StalledWrites;
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Socket write stalled")
            << TErrorAttribute("timeout", Config_->WriteStallTimeout));
        return;
    }

    if (HasUnreadData() && LastReadTime_ < now - ReadStallTimeout_) {
        ++Statistics_->StalledReads;
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Socket read stalled")
            << TErrorAttribute("timeout", Config_->ReadStallTimeout));
        return;
    }
}

TString TTcpConnection::GetLoggingId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Format("ConnectionId: %v", Id_);
}

void TTcpConnection::UpdateConnectionCount(int delta)
{
    switch (ConnectionType_) {
        case EConnectionType::Client:
            Statistics_->ClientConnections += delta;
            break;

        case EConnectionType::Server:
            Statistics_->ServerConnections += delta;
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TTcpConnection::UpdatePendingOut(int countDelta, i64 sizeDelta)
{
    Statistics_->PendingOutPackets += countDelta;
    Statistics_->PendingOutBytes += sizeDelta;
}

const TConnectionId& TTcpConnection::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

void TTcpConnection::SyncOpen()
{
    State_ = EState::Open;

    int port = GetSocketPort();
    if (port >= 0) {
        Logger.AddTag("LocalPort: %v", GetSocketPort());
    }
    
    LOG_DEBUG("Connection established");

    // Flush messages that were enqueued when the connection was still opening.
    ProcessOutcomingMessages();

    // Simulate read-write notification.
    OnSocket(*SocketWatcher_, ev::READ|ev::WRITE);
}

void TTcpConnection::SyncResolve()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    if (UnixDomainName_) {
        if (!IsLocalBusTransportEnabled()) {
            SyncClose(TError(NRpc::EErrorCode::TransportError, "Local bus transport is not available"));
            return;
        }
        OnInterfaceTypeEstablished(ETcpInterfaceType::Local);
        OnAddressResolved(GetUnixDomainAddress(*UnixDomainName_));
        return;
    }

    TStringBuf hostName;
    try {
        ParseServiceAddress(*Address_, &hostName, &Port_);
    } catch (const std::exception& ex) {
        SyncClose(TError(ex).SetCode(NRpc::EErrorCode::TransportError));
        return;
    }

    TAddressResolver::Get()->Resolve(TString(hostName)).Subscribe(
        BIND(&TTcpConnection::OnAddressResolutionFinished, MakeStrong(this))
            .Via(DispatcherThread_->GetInvoker()));
}

void TTcpConnection::OnAddressResolutionFinished(const TErrorOr<TNetworkAddress>& result)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    if (!result.IsOK()) {
        SyncClose(result);
        return;
    }

    TNetworkAddress address(result.Value(), Port_);

    LOG_DEBUG("Connection network address resolved (Address: %v)",
        address);

    if (!InterfaceType_ && IsLocalBusTransportEnabled() && TAddressResolver::Get()->IsLocalAddress(address)) {
        address = GetLocalBusAddress(Port_);
        OnInterfaceTypeEstablished(ETcpInterfaceType::Local);
    } else {
        OnInterfaceTypeEstablished(ETcpInterfaceType::Remote);
    }

    OnAddressResolved(address);
}

void TTcpConnection::OnAddressResolved(TNetworkAddress address)
{
    try {
        ConnectSocket(address);
    } catch (const std::exception& ex) {
        SyncClose(TError(ex).SetCode(NRpc::EErrorCode::TransportError));
        return;
    }

    InitSocketWatcher();

    State_ = EState::Opening;
}

void TTcpConnection::OnInterfaceTypeEstablished(ETcpInterfaceType interfaceType)
{
    YCHECK(!InterfaceType_ || *InterfaceType_ == interfaceType);
    YCHECK(!Statistics_);

    LOG_DEBUG("Using %Qlv interface", interfaceType);

    Statistics_ = DispatcherThread_->GetStatistics(interfaceType);

    if (EnableChecksums_) {
        EnableChecksums_ = (interfaceType == ETcpInterfaceType::Remote);
    }

    UpdateConnectionCount(+1);
    ConnectionCounterUpdated_ = true;
}

void TTcpConnection::SyncClose(const TError& error)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(!error.IsOK());

    // Check for second close attempt.
    if (State_ == EState::Closed) {
        return;
    }

    State_ = EState::Closed;

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
        << *EndpointAttributes_;

    LOG_DEBUG(detailedError, "Connection closed");

    // Invoke user callbacks.
    Terminated_.Fire(detailedError);

    if (ConnectionCounterUpdated_) {
        UpdateConnectionCount(-1);
    }

    DispatcherThread_->AsyncUnregister(this);
}

void TTcpConnection::InitBuffers()
{
    ReadBuffer_ = TBlob(TTcpConnectionReadBufferTag(), MinBatchReadSize, false);

    WriteBuffers_.push_back(std::make_unique<TBlob>(TTcpConnectionWriteBufferTag()));
    WriteBuffers_[0]->Reserve(MaxBatchWriteSize);
}

void TTcpConnection::InitFD()
{
#ifdef _win_
    FD_ = _open_osfhandle(Socket_, 0);
#else
    FD_ = Socket_;
#endif
}

int TTcpConnection::GetSocketPort()
{
    TNetworkAddress address;
    auto* sockAddr = address.GetSockAddr();
    socklen_t sockAddrLen = address.GetLength();
    int result = getsockname(FD_, sockAddr, &sockAddrLen);
    if (result < 0) {
        return -1;
    }

    switch (sockAddr->sa_family) {
        case AF_INET:
            return ntohs(reinterpret_cast<sockaddr_in*>(sockAddr)->sin_port);

        case AF_INET6:
            return ntohs(reinterpret_cast<sockaddr_in6*>(sockAddr)->sin6_port);

        default:
            return -1;
    }
}

void TTcpConnection::InitSocketWatcher()
{
    SocketWatcher_.reset(new ev::io(DispatcherThread_->GetEventLoop()));
    SocketWatcher_->set<TTcpConnection, &TTcpConnection::OnSocket>(this);
    SocketWatcher_->start(FD_, ev::READ|ev::WRITE);
}

void TTcpConnection::CloseSocket()
{
    if (FD_ != INVALID_SOCKET) {
        close(FD_);
        Socket_ = INVALID_SOCKET;
        FD_ = INVALID_SOCKET;
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
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create client socket")
            << TError::FromSystem();
    }

    InitFD();

    if (family == AF_INET6) {
        int value = 0;
        if (setsockopt(Socket_, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &value, sizeof(value)) != 0) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to configure IPv6 protocol")
                << TError::FromSystem();
        }
    }

    if (Config_->EnableNoDelay && family != AF_UNIX) {
        if (Config_->EnableNoDelay) {
            int value = 1;
            if (setsockopt(Socket_, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::TransportError,
                    "Failed to enable socket NODELAY mode")
                    << TError::FromSystem();
            }
        }
#ifdef _linux_
        {
            if (setsockopt(Socket_, SOL_SOCKET, SO_PRIORITY, (const char*) &Priority_, sizeof(Priority_)) != 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::TransportError,
                    "Failed to set socket priority")
                    << TError::FromSystem();
            }
        }
#endif
        {
            int value = 1;
            if (setsockopt(Socket_, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::TransportError,
                    "Failed to enable keep alive")
                    << TError::FromSystem();
            }
        }
    }

    {
#ifdef _win_
        unsigned long value = 1;
        int result = ioctlsocket(Socket_, FIONBIO, &value);
#else
        int flags = fcntl(Socket_, F_GETFL);
        int result = fcntl(Socket_, F_SETFL, flags | O_NONBLOCK);
#endif
        if (result != 0) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem();
        }
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(Socket_, F_GETFD);
        int result = fcntl(Socket_, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                << TError::FromSystem();
        }
    }
#endif

    int result = HandleEintr(connect, Socket_, netAddress.GetSockAddr(), netAddress.GetLength());

    if (result != 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Error connecting to %v",
                EndpointDescription_)
                << TError::FromSystem(error);
        }
    }
}

const TString& TTcpConnection::GetEndpointDescription() const
{
    return EndpointDescription_;
}

const IAttributeDictionary& TTcpConnection::GetEndpointAttributes() const
{
    return *EndpointAttributes_;
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

    DispatcherThread_->GetInvoker()->Invoke(BIND(
        &TTcpConnection::OnTerminated,
        MakeStrong(this),
        error));
}

void TTcpConnection::OnTerminated(const TError& error)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    SyncClose(error);
}

void TTcpConnection::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Subscribe(callback);
}

void TTcpConnection::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Unsubscribe(callback);
}

void TTcpConnection::OnSocket(ev::io&, int revents)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    Y_ASSERT(State_ != EState::Closed);

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
    if (State_ == EState::Closed) {
        return;
    }

    LastReadTime_ = NProfiling::GetCpuInstant();

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

bool TTcpConnection::HasUnreadData() const
{
    return Decoder_.IsInProgress();
}

bool TTcpConnection::ReadSocket(char* buffer, size_t size, size_t* bytesRead)
{
    ssize_t result = HandleEintr(recv, Socket_, buffer, size, 0);

    if (!CheckReadError(result)) {
        *bytesRead = 0;
        return false;
    }

    *bytesRead = result;
    Statistics_->InBytes += result;

    LOG_TRACE("%v bytes read", *bytesRead);

#ifdef _linux_
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
            ++Statistics_->ReadErrors;
            SyncClose(TError(
                NRpc::EErrorCode::TransportError,
                "Socket read error")
                << TError::FromSystem(error));
        }
        return false;
    }

    return true;
}

bool TTcpConnection::AdvanceDecoder(size_t size)
{
    if (!Decoder_.Advance(size)) {
        ++Statistics_->DecoderErrors;
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

bool TTcpConnection::OnPacketReceived() throw()
{
    ++Statistics_->InPackets;
    switch (Decoder_.GetPacketType()) {
        case EPacketType::Ack:
            return OnAckPacketReceived();
        case EPacketType::Message:
            return OnMessagePacketReceived();
        default:
            Y_UNREACHABLE();
    }
}

bool TTcpConnection::OnAckPacketReceived()
{
    if (UnackedMessages_.empty()) {
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Unexpected ack received"));
        return false;
    }

    auto& unackedMessage = UnackedMessages_.front();

    if (Decoder_.GetPacketId() != unackedMessage.PacketId) {
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Ack for invalid packet ID received: expected %v, found %v",
            unackedMessage.PacketId,
            Decoder_.GetPacketId()));
        return false;
    }

    LOG_DEBUG("Ack received (PacketId: %v)", Decoder_.GetPacketId());

    if (unackedMessage.Promise) {
        unackedMessage.Promise.Set(TError());
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
    Handler_->HandleMessage(std::move(message), this);

    return true;
}

TTcpConnection::TPacket* TTcpConnection::EnqueuePacket(
    EPacketType type,
    EPacketFlags flags,
    const TPacketId& packetId,
    TSharedRefArray message)
{
    size_t size = TPacketEncoder::GetPacketSize(type, message);
    auto* packet = new TPacket(type, flags, packetId, std::move(message), size);
    QueuedPackets_.push(packet);
    UpdatePendingOut(+1, +size);
    return packet;
}

void TTcpConnection::OnSocketWrite()
{
    if (State_ == EState::Closed) {
        return;
    }

    // For client sockets the first write notification means that
    // connection was established (either successfully or not).
    if (ConnectionType_ == EConnectionType::Client && State_ == EState::Opening) {
        // Check if connection was established successfully.
        int error = GetSocketError();
        if (error != 0) {
            // We're currently in event loop context, so calling |SyncClose| is safe.
            SyncClose(TError(
                NRpc::EErrorCode::TransportError,
                "Error connecting to %v",
                EndpointDescription_)
                << TError::FromSystem(error));
            return;
        }
        SyncOpen();
    }

    LOG_TRACE("Started serving write request");

    LastWriteScheduleTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();

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
        item.buf = const_cast<char*>(fragment.Begin());
        item.len = static_cast<ULONG>(size);
        SendVector_.push_back(item);
#else
        struct iovec item;
        item.iov_base = const_cast<char*>(fragment.Begin());
        item.iov_len = size;
        SendVector_.push_back(item);
#endif
        EncodedFragments_.move_forward(fragmentIt);
        bytesAvailable -= size;
    }

    ssize_t result;
#ifdef _win_
    DWORD bytesWritten_ = 0;
    result = WSASend(Socket_, SendVector_.data(), SendVector_.size(), &bytesWritten_, 0, nullptr, nullptr);
    *bytesWritten = static_cast<size_t>(bytesWritten_);
#else
    result = HandleEintr(::writev, Socket_, SendVector_.data(), SendVector_.size());
    *bytesWritten = result >= 0 ? result : 0;
#endif
    bool isOK = CheckWriteError(result);
    if (isOK) {
        Statistics_->OutBytes += *bytesWritten;
        LOG_TRACE("%v bytes written", *bytesWritten);
    }
    return isOK;
}

void TTcpConnection::FlushWrittenFragments(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    LOG_TRACE("Flushing fragments, %v bytes written", bytesWritten);

    while (bytesToFlush != 0) {
        Y_ASSERT(!EncodedFragments_.empty());
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
        Y_ASSERT(!EncodedPacketSizes_.empty());
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
            WriteBuffers_.push_back(std::make_unique<TBlob>(TTcpConnectionWriteBufferTag()));
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
            EnableChecksums_,
            packet->PacketId,
            packet->Message);
        if (!encodeResult) {
            ++Statistics_->EncoderErrors;
            SyncClose(TError(
                NRpc::EErrorCode::TransportError,
                "Error encoding outcoming packet"));
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
            ++Statistics_->WriteErrors;
            SyncClose(TError(
                NRpc::EErrorCode::TransportError,
                "Socket write error")
                << TError::FromSystem(error));
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
            Y_UNREACHABLE();
    }


    UpdatePendingOut(-1, -packet->Size);
    ++Statistics_->OutPackets;

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
    LOG_DEBUG("Outcoming message sent (PacketId: %v)",
        packet.PacketId);
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

    switch (State_.load()) {
        case EState::Resolving:
        case EState::Opening:
            // Do nothing.
            break;

        case EState::Open:
            ProcessOutcomingMessages();
            UpdateSocketWatcher();
            break;

        case EState::Closed:
            DiscardOutcomingMessages(TError(
                NRpc::EErrorCode::TransportError,
                "Connection closed"));
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TTcpConnection::ProcessOutcomingMessages()
{
    auto messages = QueuedMessages_.DequeueAll();

    for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
        auto& queuedMessage = *it;

        const auto& packetId = queuedMessage.PacketId;
        auto flags = queuedMessage.Level == EDeliveryTrackingLevel::Full
            ? EPacketFlags::RequestAck
            : EPacketFlags::None;

        auto* packet = EnqueuePacket(
            EPacketType::Message,
            flags,
            packetId,
            std::move(queuedMessage.Message));

        LOG_DEBUG("Outcoming message dequeued (PacketId: %v, PacketSize: %v, Flags: %v)",
            packetId,
            packet->Size,
            flags);

        if (Any(flags & EPacketFlags::RequestAck)) {
            UnackedMessages_.push(TUnackedMessage(packetId, std::move(queuedMessage.Promise)));
        } else if (queuedMessage.Promise) {
            queuedMessage.Promise.Set();
        }
    }
}

void TTcpConnection::DiscardOutcomingMessages(const TError& error)
{
    TQueuedMessage queuedMessage;
    while (QueuedMessages_.Dequeue(&queuedMessage)) {
        LOG_DEBUG("Outcoming message discarded (PacketId: %v)",
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
    if (State_ != EState::Open) {
        return;
    }

    int events = ev::READ;
    if (HasUnsentData()) {
        LastWriteScheduleTime_ = NProfiling::GetCpuInstant();
        events |= ev::WRITE;
    }
    SocketWatcher_->set(events);
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

