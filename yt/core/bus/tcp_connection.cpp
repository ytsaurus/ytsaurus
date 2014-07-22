#include "stdafx.h"
#include "tcp_connection.h"
#include "tcp_dispatcher_impl.h"
#include "server.h"
#include "config.h"

#include <core/misc/string.h>

#include <core/rpc/public.h>

#include <core/profiling/profiling_manager.h>

#include <util/system/error.h>
#include <util/folder/dirut.h>

#include <errno.h>

#ifndef _WIN32
    #include <netinet/tcp.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static const size_t MinBatchReadSize =  4 * 1024;
static const size_t MaxBatchReadSize = 64 * 1024;

static const size_t MaxFragmentsPerWrite = 256;
static const size_t MaxBatchWriteSize = 64 * 1024;

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
    : Config(std::move(config))
    , DispatcherThread(std::move(dispatcherThread))
    , ConnectionType(connectionType)
    , InterfaceType(interfaceType)
    , Id(id)
    , Socket(socket)
    , Fd(INVALID_SOCKET)
    , Address(address)
#ifdef _linux_
    , Priority(priority)
#endif
    , Handler(handler)
    , Logger(BusLogger)
    , Profiler(BusProfiler)
    , Port(0)
    , MessageEnqueuedSent(false)
    , ReadBuffer(MinBatchReadSize)
    , TerminatedPromise(NewPromise<TError>())
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(handler);

    Logger.AddTag(Sprintf("ConnectionId: %s, Address: %s",
        ~ToString(id),
        ~Address));

    auto tagId = NProfiling::TProfilingManager::Get()->RegisterTag("interface", FormatEnum(InterfaceType));
    Profiler.TagIds().push_back(tagId);

    switch (ConnectionType) {
        case EConnectionType::Client:
            YCHECK(Socket == INVALID_SOCKET);
            AtomicSet(State, EState::Resolving);
            break;

        case EConnectionType::Server:
            YCHECK(Socket != INVALID_SOCKET);
            AtomicSet(State, EState::Opening);
            break;

        default:
            YUNREACHABLE();
    }

    UpdateConnectionCount(+1);
}

TTcpConnection::~TTcpConnection()
{
    CloseSocket();
    Cleanup();
}

void TTcpConnection::Cleanup()
{
    while (!QueuedPackets.empty()) {
        auto* packet = QueuedPackets.front();
        UpdatePendingOut(-1, -packet->Size);
        delete packet;
        QueuedPackets.pop();
    }

    while (!EncodedPackets.empty()) {
        auto* packet = EncodedPackets.front();
        UpdatePendingOut(-1, -packet->Packet->Size);
        delete packet->Packet;
        delete packet;
        EncodedPackets.pop();
    }

    EncodedFragments.clear();
}

void TTcpConnection::SyncInitialize()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    switch (ConnectionType) {
        case EConnectionType::Client:
            SyncResolve();
            break;

        case EConnectionType::Server:
            InitFd();
            InitWatcher();
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

    return Sprintf("ConnectionId: %s", ~ToString(Id));
}

TTcpDispatcherStatistics& TTcpConnection::Statistics()
{
    return DispatcherThread->Statistics(InterfaceType);
}

void TTcpConnection::UpdateConnectionCount(int delta)
{
    switch (ConnectionType) {
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

    return Id;
}

void TTcpConnection::SyncOpen()
{
    AtomicSet(State, EState::Open);

    LOG_DEBUG("Connection established");

    // Flush messages that were enqueued when the connection was still opening.
    ProcessOutcomingMessages();
    
    // Simulate read-write notification.
    OnSocket(*SocketWatcher, ev::READ|ev::WRITE);
}

void TTcpConnection::SyncResolve()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TStringBuf hostName;
    try {
        ParseServiceAddress(Address, &hostName, &Port);
    } catch (const std::exception& ex) {
        SyncClose(TError(ex).SetCode(NRpc::EErrorCode::TransportError));
        return;
    }

    if (InterfaceType == ETcpInterfaceType::Local) {
        LOG_DEBUG("Address resolved as local, connecting");

        auto netAddress = GetLocalBusAddress(Port);
        OnAddressResolved(netAddress);
    } else {
        AsyncAddress = TAddressResolver::Get()->Resolve(Stroka(hostName));

        auto this_ = MakeStrong(this);
        AsyncAddress.Subscribe(BIND([this, this_] (TErrorOr<TNetworkAddress>) {
            DispatcherThread->AsyncPostEvent(this_, EConnectionEvent::AddressResolved);
        }));
    }
}

void TTcpConnection::OnAddressResolved()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    auto result = AsyncAddress.Get();
    if (!result.IsOK()) {
        SyncClose(result);
        return;
    }

    LOG_DEBUG("Address resolved, connecting");

    TNetworkAddress netAddress(result.Value(), Port);
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

    InitWatcher();

    AtomicSet(State, EState::Opening);
}

void TTcpConnection::SyncClose(const TError& error)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YCHECK(!error.IsOK());

    // Check for second close attempt.
    if (State == EState::Closed) {
        return;
    }

    AtomicSet(State, EState::Closed);

    // Stop all watchers.
    SocketWatcher.reset();

    // Close the socket.
    CloseSocket();

    // Mark all unacked messages as failed.
    DiscardUnackedMessages(error);

    // Mark all queued messages as failed.
    DiscardOutcomingMessages(error);

    // Release memory.
    Cleanup();

    // Invoke user callback.
    PROFILE_TIMING ("/terminate_handler_time") {
        TerminatedPromise.Set(error);
    }

    LOG_DEBUG(error, "Connection closed");

    UpdateConnectionCount(-1);

    DispatcherThread->AsyncUnregister(this);
}

void TTcpConnection::InitFd()
{
#ifdef _WIN32
    Fd = _open_osfhandle(Socket, 0);
#else
    Fd = Socket;
#endif
}

void TTcpConnection::InitWatcher()
{
    SocketWatcher.reset(new ev::io(DispatcherThread->GetEventLoop()));
    SocketWatcher->set<TTcpConnection, &TTcpConnection::OnSocket>(this);
    SocketWatcher->start(Fd, ev::READ|ev::WRITE);
}

void TTcpConnection::CloseSocket()
{
    if (Fd != INVALID_SOCKET) {
        close(Fd);
    }
    Socket = INVALID_SOCKET;
    Fd = INVALID_SOCKET;
}

void TTcpConnection::ConnectSocket(const TNetworkAddress& netAddress)
{
    int family = netAddress.GetSockAddr()->sa_family;
    int protocol = family == AF_UNIX ? 0 : IPPROTO_TCP;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
#endif

    Socket = socket(family, type, protocol);
    if (Socket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION("Failed to create client socket")
            << TError::FromSystem();
    }

    InitFd();

    if (family == AF_INET6) {
        int value = 0;
        if (setsockopt(Socket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &value, sizeof(value)) != 0) {
            THROW_ERROR_EXCEPTION("Failed to configure IPv6 protocol")
                << TError::FromSystem();
        }
    }

    if (Config->EnableNoDelay && family != AF_UNIX) {
        if (Config->EnableNoDelay) {
            int value = 1;
            if (setsockopt(Socket, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to enable socket NODELAY mode")
                    << TError::FromSystem();
            }
        }
#ifdef _linux_
        {
            if (setsockopt(Socket, SOL_SOCKET, SO_PRIORITY, (const char*) &Priority, sizeof(Priority)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to set socket priority")
                    << TError::FromSystem();
            }
        }
#endif
        {
            int value = 1;
            if (setsockopt(Socket, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value)) != 0) {
                THROW_ERROR_EXCEPTION("Failed to enable keep alive")
                    << TError::FromSystem();
            }
        }
    }

    {
#ifdef _WIN32
        unsigned long value = 1;
        int result = ioctlsocket(Socket, FIONBIO, &value);
#else
        int result = fcntl(Socket, F_SETFL, O_NONBLOCK);
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
                result = connect(Socket, netAddress.GetSockAddr(), netAddress.GetLength());
            } while (result < 0 && errno == EINTR);
        }

        if (result != 0) {
            int error = LastSystemError();
            if (IsSocketError(error)) {
                THROW_ERROR_EXCEPTION("Error connecting to %s", ~Address)
                    << TError::FromSystem(error);
            }
        }
    }
}

TAsyncError TTcpConnection::Send(TSharedRefArray message)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueuedMessage queuedMessage(message);

    // NB: Log first to avoid producing weird traces.
    LOG_DEBUG("Outcoming message enqueued (PacketId: %s)", ~ToString(queuedMessage.PacketId));

    QueuedMessages.Enqueue(queuedMessage);

    auto state = AtomicGet(State);
    bool sent = AtomicGet(MessageEnqueuedSent);
    if (!sent && state != EState::Resolving && state != EState::Opening)  {
        AtomicSet(MessageEnqueuedSent, true);
        DispatcherThread->AsyncPostEvent(this, EConnectionEvent::MessageEnqueued);
    }

    return queuedMessage.Promise;
}

void TTcpConnection::Terminate(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!error.IsOK());

    {
        // Check if another termination request is already in progress.
        TGuard<TSpinLock> guard(TerminationSpinLock);
        if (!TerminationError.IsOK()) {
            return;
        }
        TerminationError = error;
    }

    DispatcherThread->AsyncPostEvent(this, EConnectionEvent::Terminated);
    
    LOG_DEBUG("Bus termination requested");
}

void TTcpConnection::SyncProcessEvent(EConnectionEvent event)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    switch (event) {
        case EConnectionEvent::AddressResolved:
            OnAddressResolved();
            break;
        case EConnectionEvent::Terminated:
            OnTerminated();
            break;
        case EConnectionEvent::MessageEnqueued:
            OnMessageEnqueued();
            break;
        default:
            YUNREACHABLE();
    }
}

void TTcpConnection::SubscribeTerminated(const TCallback<void(TError)>& callback)
{
    TerminatedPromise.Subscribe(callback);
}

void TTcpConnection::UnsubscribeTerminated(const TCallback<void(TError)>& callback)
{
    YUNREACHABLE();
}

void TTcpConnection::OnSocket(ev::io&, int revents)
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    YASSERT(State != EState::Closed);

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
    if (State == EState::Closed) {
        return;
    }

    LOG_TRACE("Started serving read request");
    size_t bytesReadTotal = 0;

    while (true) {
        // Check if the decoder is expecting a chunk of large enough size.
        auto decoderChunk = Decoder.GetChunk();
        size_t decoderChunkSize = decoderChunk.Size();
        LOG_TRACE("Decoder needs %" PRISZT " bytes", decoderChunkSize);

        if (decoderChunkSize >= MinBatchReadSize) {
            // Read directly into the decoder buffer.
            size_t bytesToRead = std::min(decoderChunkSize, MaxBatchReadSize);
            LOG_TRACE("Reading %" PRISZT " bytes into decoder", bytesToRead);
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
            LOG_TRACE("Reading %" PRISZT " bytes into buffer", ReadBuffer.Size());
            size_t bytesRead;
            if (!ReadSocket(ReadBuffer.Begin(), ReadBuffer.Size(), &bytesRead)) {
                break;
            }
            bytesReadTotal += bytesRead;

            // Feed the read buffer to the decoder.
            const char* recvBegin = ReadBuffer.Begin();
            size_t recvRemaining = bytesRead;
            while (recvRemaining != 0) {
                decoderChunk = Decoder.GetChunk();
                decoderChunkSize = decoderChunk.Size();
                size_t bytesToCopy = std::min(recvRemaining, decoderChunkSize);
                LOG_TRACE("Decoder chunk size is %" PRISZT " bytes, copying %" PRISZT " bytes",
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

    LOG_TRACE("Finished serving read request, %" PRISZT " bytes read total", bytesReadTotal);
}

bool TTcpConnection::ReadSocket(char* buffer, size_t size, size_t* bytesRead)
{
    ssize_t result;
    PROFILE_AGGREGATED_TIMING (ReceiveTime) {
        do {
            result = recv(Socket, buffer, size, 0);
        } while (result < 0 && errno == EINTR);
    }

    if (!CheckReadError(result)) {
        *bytesRead = 0;
        return false;
    }

    *bytesRead = result;

    Profiler.Increment(InThroughputCounter, *bytesRead);
    Profiler.Aggregate(ReceiveSize, *bytesRead);

    LOG_TRACE("%" PRISZT " bytes read", *bytesRead);

#if !defined(_win_) && !defined(__APPLE__)
    if (Config->EnableQuickAck) {
        int value = 1;
        setsockopt(Socket, IPPROTO_TCP, TCP_QUICKACK, (const char*) &value, sizeof(value));
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
            LOG_WARNING(wrappedError);
            SyncClose(wrappedError);
        }
        return false;
    }

    return true;
}

bool TTcpConnection::AdvanceDecoder(size_t size)
{
    if (!Decoder.Advance(size)) {
        SyncClose(TError(NRpc::EErrorCode::TransportError, "Error decoding incoming packet"));
        return false;
    }

    if (Decoder.IsFinished()) {
        bool result = OnPacketReceived();
        Decoder.Restart();
        return result;
    }

    return true;
}

bool TTcpConnection::OnPacketReceived()
{
    Profiler.Increment(InCounter);
    switch (Decoder.GetPacketType()) {
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
    if (UnackedMessages.empty()) {
        LOG_ERROR("Unexpected ack received");
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Unexpected ack received"));
        return false;
    }

    auto& unackedMessage = UnackedMessages.front();

    if (Decoder.GetPacketId() != unackedMessage.PacketId) {
        LOG_ERROR("Ack for invalid packet ID received: expected %s, found %s",
            ~ToString(unackedMessage.PacketId),
            ~ToString(Decoder.GetPacketId()));
        SyncClose(TError(
            NRpc::EErrorCode::TransportError,
            "Ack for invalid packet ID received"));
        return false;
    }

    LOG_DEBUG("Ack received (PacketId: %s)", ~ToString(Decoder.GetPacketId()));

    PROFILE_AGGREGATED_TIMING (OutHandlerTime) {
        unackedMessage.Promise.Set(TError());
    }

    UnackedMessages.pop();

    return true;
}

bool TTcpConnection::OnMessagePacketReceived()
{
    LOG_DEBUG("Incoming message received (PacketId: %s, PacketSize: %" PRISZT ")",
        ~ToString(Decoder.GetPacketId()),
        Decoder.GetPacketSize());

    EnqueuePacket(EPacketType::Ack, Decoder.GetPacketId());

    auto message = Decoder.GetMessage();
    PROFILE_AGGREGATED_TIMING (InHandlerTime) {
        Handler->OnMessage(message, this);
    }

    return true;
}

void TTcpConnection::EnqueuePacket(EPacketType type, const TPacketId& packetId, TSharedRefArray message)
{
    i64 size = TPacketEncoder::GetPacketSize(type, message);
    QueuedPackets.push(new TQueuedPacket(type, packetId, message, size));
    UpdatePendingOut(+1, +size);
    EncodeMoreFragments();
}

void TTcpConnection::OnSocketWrite()
{
    if (State == EState::Closed) {
        return;
    }

    // For client sockets the first write notification means that
    // connection was established (either successfully or not).
    if (ConnectionType == EConnectionType::Client && State == EState::Opening) {
        // Check if connection was established successfully.
        int error = GetSocketError();
        if (error != 0) {
            auto wrappedErrror = TError(
                NRpc::EErrorCode::TransportError,
                "Failed to connect to %s",
                ~Address)
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
        size_t bytesWritten;
        bool success = WriteFragments(&bytesWritten);
        bytesWrittenTotal += bytesWritten;
        FlushWrittenFragments(bytesWritten);
        EncodeMoreFragments();
        if (!success) {
            break;
        }
    }

    LOG_TRACE("Finished serving write request, %" PRISZT " bytes written total", bytesWrittenTotal);
}

bool TTcpConnection::HasUnsentData() const
{
    return !EncodedFragments.empty();
}

bool TTcpConnection::WriteFragments(size_t* bytesWritten)
{
    LOG_TRACE("Writing fragments, %" PRISZT " encoded", EncodedFragments.size());

    auto fragmentIt = EncodedFragments.begin();
    auto fragmentEnd = EncodedFragments.end();

    SendVector.clear();
    size_t vacantBytes = MaxBatchWriteSize;

    while (fragmentIt != fragmentEnd &&
           SendVector.size() < MaxFragmentsPerWrite &&
           vacantBytes > 0)
    {
        const auto& chunk = fragmentIt->Chunk;
        char* buffer = chunk.Begin();
        size_t size = std::min(chunk.Size(), vacantBytes);
#ifdef _WIN32
        WSABUF item;
        item.buf = buffer;
        item.len = static_cast<ULONG>(size);
        SendVector.push_back(item);
#else
        struct iovec item;
        item.iov_base = buffer;
        item.iov_len = size;
        SendVector.push_back(item);
#endif
        EncodedFragments.move_forward(fragmentIt);
        vacantBytes -= size;
    }

    ssize_t result;
#ifdef _WIN32
    DWORD bytesWritten_ = 0;
    PROFILE_AGGREGATED_TIMING (SendTime) {
        result = WSASend(Socket, SendVector.data(), SendVector.size(), &bytesWritten_, 0, NULL, NULL);
    }
    *bytesWritten = static_cast<size_t>(bytesWritten_);
#else
    PROFILE_AGGREGATED_TIMING (SendTime) {
        do {
            result = writev(Socket, SendVector.data(), SendVector.size());
        } while (result < 0 && errno == EINTR);
    }
    *bytesWritten = result >= 0 ? result : 0;
#endif

    Profiler.Increment(OutThroughputCounter, *bytesWritten);
    Profiler.Aggregate(SendSize, *bytesWritten);

    LOG_TRACE("%" PRISZT " bytes written", *bytesWritten);

    return CheckWriteError(result);
}

void TTcpConnection::FlushWrittenFragments(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    LOG_TRACE("Flushing %" PRISZT " written bytes", bytesWritten);

    while (bytesToFlush != 0) {
        YASSERT(!EncodedFragments.empty());
        auto& fragment = EncodedFragments.front();

        auto& data = fragment.Chunk;
        if (data.Size() > bytesToFlush) {
            size_t bytesRemaining = data.Size() - bytesToFlush;
            LOG_TRACE("Partial write (Size: %" PRISZT ", RemainingSize: %" PRISZT ")",
                data.Size(),
                bytesRemaining);
            fragment.Chunk = TRef(data.End() - bytesRemaining, bytesRemaining);
            break;
        }

        LOG_TRACE("Full write (Size: %" PRISZT ")", data.Size());

        if (fragment.IsLastInPacket) {
            OnPacketSent();
        }

        bytesToFlush -= data.Size();
        EncodedFragments.pop();
    }
}

bool TTcpConnection::EncodeMoreFragments()
{
    while (EncodedFragments.size() < MaxFragmentsPerWrite && !QueuedPackets.empty()) {
        // Move the packet from queued to encoded.
        auto* queuedPacket = QueuedPackets.front();
        QueuedPackets.pop();

        auto* encodedPacket = new TEncodedPacket();
        EncodedPackets.push(encodedPacket);

        encodedPacket->Packet = queuedPacket;

        // Encode the packet.
        LOG_TRACE("Started encoding packet");

        auto& encoder = encodedPacket->Encoder;
        if (!encoder.Start(queuedPacket->Type, queuedPacket->PacketId, queuedPacket->Message)) {
            SyncClose(TError(NRpc::EErrorCode::TransportError, "Error encoding outcoming packet"));
            return false;
        }

        TEncodedFragment fragment;
        do {
            fragment.Chunk = encoder.GetChunk();
            encoder.NextChunk();
            fragment.IsLastInPacket = encoder.IsFinished();
            EncodedFragments.push(fragment);
            LOG_TRACE("Fragment encoded (Size: %" PRISZT ", IsLast: %d)",
                fragment.Chunk.Size(),
                fragment.IsLastInPacket);
        } while (!fragment.IsLastInPacket);

        LOG_TRACE("Finished encoding packet");
    }
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
    const auto* packet = EncodedPackets.front();
    switch (packet->Packet->Type) {
        case EPacketType::Ack:
            OnAckPacketSent(*packet);
            break;

        case EPacketType::Message:
            OnMessagePacketSent(*packet);
            break;

        default:
            YUNREACHABLE();
    }


    UpdatePendingOut(-1, -packet->Packet->Size);
    Profiler.Increment(OutCounter);

    delete packet->Packet;
    delete packet;
    EncodedPackets.pop();
}

void  TTcpConnection::OnAckPacketSent(const TEncodedPacket& packet)
{
    LOG_DEBUG("Ack sent (PacketId: %s)", ~ToString(packet.Packet->PacketId));
}

void TTcpConnection::OnMessagePacketSent(const TEncodedPacket& packet)
{
    LOG_DEBUG("Outcoming message sent (PacketId: %s, PacketSize: %" PRId64 ")",
        ~ToString(packet.Packet->PacketId),
        packet.Packet->Size);
}

void TTcpConnection::OnMessageEnqueued()
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    
    AtomicSet(MessageEnqueuedSent, false);
    
    if (State == EState::Closed) {
        DiscardOutcomingMessages(TError(
            NRpc::EErrorCode::TransportError,
            "Connection is closed"));
        return;
    }

    ProcessOutcomingMessages();
    UpdateSocketWatcher();
}

void TTcpConnection::ProcessOutcomingMessages()
{
    TQueuedMessage queuedMessage;
    while (QueuedMessages.Dequeue(&queuedMessage)) {
        LOG_DEBUG("Outcoming message dequeued (PacketId: %s)", ~ToString(queuedMessage.PacketId));

        EnqueuePacket(EPacketType::Message, queuedMessage.PacketId, queuedMessage.Message);

        TUnackedMessage unackedMessage(queuedMessage.PacketId, queuedMessage.Promise);
        UnackedMessages.push(unackedMessage);
    }
}

void TTcpConnection::DiscardOutcomingMessages(const TError& error)
{
    TQueuedMessage queuedMessage;
    while (QueuedMessages.Dequeue(&queuedMessage)) {
        LOG_DEBUG("Outcoming message dequeued (PacketId: %s)", ~ToString(queuedMessage.PacketId));
        queuedMessage.Promise.Set(error);
    }
}

void TTcpConnection::DiscardUnackedMessages(const TError& error)
{
    while (!UnackedMessages.empty()) {
        UnackedMessages.front().Promise.Set(error);
        UnackedMessages.pop();
    }
}

void TTcpConnection::UpdateSocketWatcher()
{
    if (State == EState::Open) {
        SocketWatcher->set(HasUnsentData() ? ev::READ|ev::WRITE : ev::READ);
    }
}

void TTcpConnection::OnTerminated()
{
    VERIFY_THREAD_AFFINITY(EventLoop);
    
    if (State == EState::Closed) {
        return;
    }

    TError error;
    {
        TGuard<TSpinLock> guard(TerminationSpinLock);
        error = TerminationError;
    }

    SyncClose(error);
}

int TTcpConnection::GetSocketError() const
{
    int error;
    socklen_t errorLen = sizeof (error);
    getsockopt(Socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &errorLen);
    return error;
}

bool TTcpConnection::IsSocketError(ssize_t result)
{
#ifdef _WIN32
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

