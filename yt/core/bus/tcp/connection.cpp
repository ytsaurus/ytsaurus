#include "connection.h"
#include "config.h"
#include "server.h"
#include "dispatcher_impl.h"

#include <yt/core/misc/enum.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/string.h>

#include <yt/core/net/socket.h>
#include <yt/core/net/dialer.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/profiling/timing.h>

#include <util/system/error.h>
#include <util/system/guard.h>

#include <cerrno>

namespace NYT {
namespace NBus {

using namespace NConcurrency;
using namespace NNet;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MinBatchReadSize = 16 * 1024;
static constexpr size_t MaxBatchReadSize = 64 * 1024;
static constexpr auto ReadTimeWarningThreshold = TDuration::MilliSeconds(100);

static constexpr size_t MaxFragmentsPerWrite = 256;
static constexpr size_t MaxBatchWriteSize    = 64 * 1024;
static constexpr size_t MaxWriteCoalesceSize = 4 * 1024;
static constexpr auto WriteTimeWarningThreshold = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

struct TTcpConnectionReadBufferTag { };
struct TTcpConnectionWriteBufferTag { };

////////////////////////////////////////////////////////////////////////////////

TTcpConnection::TTcpConnection(
    TTcpBusConfigPtr config,
    EConnectionType connectionType,
    const TString& networkName,
    const TConnectionId& id,
    int socket,
    const TString& endpointDescription,
    const IAttributeDictionary& endpointAttributes,
    const TNetworkAddress& endpointAddress,
    const std::optional<TString>& address,
    const std::optional<TString>& unixDomainName,
    IMessageHandlerPtr handler,
    IPollerPtr poller)
    : Config_(std::move(config))
    , ConnectionType_(connectionType)
    , Id_(id)
    , EndpointDescription_(endpointDescription)
    , EndpointAttributes_(endpointAttributes.Clone())
    , EndpointAddress_(endpointAddress)
    , Address_(address)
    , UnixDomainName_(unixDomainName)
    , Handler_(std::move(handler))
    , Poller_(std::move(poller))
    , Logger(NLogging::TLogger(BusLogger)
        .AddTag("ConnectionId: %v, RemoteAddress: %v",
            Id_,
            EndpointDescription_))
    , LoggingId_(Format("ConnectionId: %v", Id_))
    , NetworkName_(networkName)
    , GenerateChecksums_(Config_->GenerateChecksums)
    , Socket_(socket)
    , Decoder_(Logger, Config_->VerifyChecksums)
    , ReadStallTimeout_(NProfiling::DurationToCpuDuration(Config_->ReadStallTimeout))
    , Encoder_(Logger)
    , WriteStallTimeout_(NProfiling::DurationToCpuDuration(Config_->WriteStallTimeout))
{
    Poller_->Register(this);
    InitBuffers();
}

TTcpConnection::~TTcpConnection()
{
    Cleanup();
}

void TTcpConnection::Cleanup()
{
    if (CloseError_.IsOK()) {
        CloseError_ = TError(NBus::EErrorCode::TransportError, "Bus terminated")
            << *EndpointAttributes_;
    }

    DiscardOutcomingMessages(CloseError_);
    DiscardUnackedMessages(CloseError_);

    while (!QueuedPackets_.empty()) {
        const auto& packet = QueuedPackets_.front();
        UpdatePendingOut(-1, -packet.PacketSize);
        QueuedPackets_.pop();
    }

    while (!EncodedPackets_.empty()) {
        const auto& packet = EncodedPackets_.front();
        UpdatePendingOut(-1, -packet.PacketSize);
        EncodedPackets_.pop();
    }

    EncodedFragments_.clear();

    CloseSocket();

    UpdateConnectionCount(false);
}

void TTcpConnection::Start()
{
    switch (ConnectionType_) {
        case EConnectionType::Client:
            YCHECK(Socket_ == INVALID_SOCKET);
            State_ = EState::Resolving;
            ResolveAddress();
            break;

        case EConnectionType::Server: {
            TWriterGuard guard(ControlSpinLock_);
            YCHECK(Socket_ != INVALID_SOCKET);
            State_ = EState::Opening;
            SetupNetwork(NetworkName_);
            Open();
            UpdateConnectionCount(true);
            DoArmPoller();
            break;
        }

        default:
            Y_UNREACHABLE();
    }
}

void TTcpConnection::Check()
{
    if (State_ != EState::Open) {
        return;
    }

    auto now = NProfiling::GetCpuInstant();

    if (LastIncompleteWriteTime_.load(std::memory_order_relaxed) < now - WriteStallTimeout_) {
        Counters_->StalledWrites.fetch_add(1, std::memory_order_relaxed);
        Terminate(TError(
            NBus::EErrorCode::TransportError,
            "Socket write stalled")
            << TErrorAttribute("timeout", Config_->WriteStallTimeout));
        return;
    }

    if (LastIncompleteReadTime_.load(std::memory_order_relaxed) < now - ReadStallTimeout_) {
        Counters_->StalledReads.fetch_add(1, std::memory_order_relaxed);
        Terminate(TError(
            NBus::EErrorCode::TransportError,
            "Socket read stalled")
            << TErrorAttribute("timeout", Config_->ReadStallTimeout));
        return;
    }
}

const TString& TTcpConnection::GetLoggingId() const
{
    return LoggingId_;
}

void TTcpConnection::UpdateConnectionCount(bool increment)
{
    if (increment) {
        YCHECK(!ConnectionCounterIncremented_);
        ConnectionCounterIncremented_ = true;
    } else {
        if (!ConnectionCounterIncremented_) {
            return;
        }
        ConnectionCounterIncremented_ = false;
    }
    int delta = increment ? +1 : -1;
    switch (ConnectionType_) {
        case EConnectionType::Client:
            Counters_->ClientConnections.fetch_add(delta, std::memory_order_relaxed);
            break;

        case EConnectionType::Server:
            Counters_->ServerConnections.fetch_add(delta, std::memory_order_relaxed);
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TTcpConnection::UpdatePendingOut(int countDelta, i64 sizeDelta)
{
    Counters_->PendingOutPackets.fetch_add(countDelta, std::memory_order_relaxed);
    Counters_->PendingOutBytes.fetch_add(sizeDelta, std::memory_order_relaxed);
}

const TConnectionId& TTcpConnection::GetId() const
{
    return Id_;
}

void TTcpConnection::Open()
{
    State_ = EState::Open;

    LOG_DEBUG("Connection established (LocalPort: %v)", GetSocketPort());
}

void TTcpConnection::ResolveAddress()
{
    if (UnixDomainName_) {
        if (!IsLocalBusTransportEnabled()) {
            Abort(TError(NBus::EErrorCode::TransportError, "Local bus transport is not available"));
            return;
        }

        NetworkName_ = LocalNetworkName;
        OnAddressResolved(
            TNetworkAddress::CreateUnixDomainAddress(*UnixDomainName_));
    } else {
        TStringBuf hostName;
        try {
            ParseServiceAddress(*Address_, &hostName, &Port_);
        } catch (const std::exception& ex) {
            Abort(TError(ex).SetCode(NBus::EErrorCode::TransportError));
            return;
        }

        TAddressResolver::Get()->Resolve(TString(hostName)).Subscribe(
            BIND(&TTcpConnection::OnAddressResolveFinished, MakeStrong(this))
                .Via(Poller_->GetInvoker()));
    }
}

void TTcpConnection::OnAddressResolveFinished(const TErrorOr<TNetworkAddress>& result)
{
    if (!result.IsOK()) {
        Abort(result);
        return;
    }

    TNetworkAddress address(result.Value(), Port_);

    LOG_DEBUG("Connection network address resolved (Address: %v)",
        address);

    if (IsLocalBusTransportEnabled() && TAddressResolver::Get()->IsLocalAddress(address)) {
        address = GetLocalBusAddress(Port_);
        NetworkName_ = LocalNetworkName;
    }

    OnAddressResolved(address);
}

void TTcpConnection::OnAddressResolved(
    const TNetworkAddress& address)
{
    State_ = EState::Opening;
    SetupNetwork(NetworkName_);
    ConnectSocket(address);
}

void TTcpConnection::SetupNetwork(const TString& networkName)
{
    YCHECK(!Counters_);

    LOG_DEBUG("Using %Qv network", networkName);

    Counters_ = TTcpDispatcher::TImpl::Get()->GetCounters(networkName);

    // Suppress checksum generation for local traffic.
    if (networkName == LocalNetworkName) {
        GenerateChecksums_ = false;
    }
}

void TTcpConnection::Abort(const TError& error)
{
    if (State_ == EState::Aborted || State_ == EState::Closed) {
        return;
    }

    State_ = EState::Aborted;
    YCHECK(!error.IsOK());
    YCHECK(CloseError_.IsOK());
    CloseError_ = error << *EndpointAttributes_;

    // Construct a detailed error.
    LOG_DEBUG(CloseError_, "Connection aborted");

    UnregisterFromPoller();
}

void TTcpConnection::InitBuffers()
{
    ReadBuffer_ = TBlob(TTcpConnectionReadBufferTag(), MinBatchReadSize, false);

    WriteBuffers_.push_back(std::make_unique<TBlob>(TTcpConnectionWriteBufferTag()));
    WriteBuffers_[0]->Reserve(MaxBatchWriteSize);
}

int TTcpConnection::GetSocketPort()
{
    TNetworkAddress address;
    auto* sockAddr = address.GetSockAddr();
    socklen_t sockAddrLen = address.GetLength();
    int result = getsockname(Socket_, sockAddr, &sockAddrLen);
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

void TTcpConnection::CloseSocket()
{
    TWriterGuard guard(ControlSpinLock_);
    if (Socket_ != INVALID_SOCKET) {
        close(Socket_);
        Socket_ = INVALID_SOCKET;
    }
}

void TTcpConnection::ConnectSocket(const TNetworkAddress& address)
{
    auto dialer = CreateAsyncDialer(
        Config_,
        Poller_,
        Logger);
    DialerSession_ = dialer->CreateSession(
        address,
        BIND(&TTcpConnection::OnDialerFinished, MakeWeak(this)));
    DialerSession_->Dial();
}

void TTcpConnection::OnDialerFinished(SOCKET socket, const TError& error)
{
    if (socket != INVALID_SOCKET) {
        OnSocketConnected(socket);
    } else {
        Abort(error);
    }
    DialerSession_.Reset();
}

const TString& TTcpConnection::GetEndpointDescription() const
{
    return EndpointDescription_;
}

const IAttributeDictionary& TTcpConnection::GetEndpointAttributes() const
{
    return *EndpointAttributes_;
}

const TNetworkAddress& TTcpConnection::GetEndpointAddress() const
{
    return EndpointAddress_;
}

TTcpDispatcherStatistics TTcpConnection::GetStatistics() const
{
    return Counters_->ToStatistics();
}

TFuture<void> TTcpConnection::Send(TSharedRefArray message, const TSendOptions& options)
{
    TQueuedMessage queuedMessage(std::move(message), options);

    auto pendingOutPayloadBytes = PendingOutPayloadBytes_.fetch_add(queuedMessage.PayloadSize);

    // NB: Log first to avoid producing weird traces.
    LOG_DEBUG("Outcoming message enqueued (PacketId: %v, PendingOutPayloadBytes: %v)",
        queuedMessage.PacketId,
        pendingOutPayloadBytes);

    if (State_ == EState::Open) {
        LastIncompleteWriteTime_ = NProfiling::GetCpuInstant();
    }

    QueuedMessages_.Enqueue(queuedMessage);
    ArmPollerForWrite();

    return queuedMessage.Promise;
}

void TTcpConnection::SetTosLevel(TTosLevel tosLevel)
{
    if (TosLevel_.load() == tosLevel) {
        return;
    }

    {
        NConcurrency::TWriterGuard guard(ControlSpinLock_);
        if (Socket_ != INVALID_SOCKET) {
            InitSocketTosLevel(tosLevel);
        }
    }

    TosLevel_.store(tosLevel);
}

void TTcpConnection::Terminate(const TError& error)
{
    NConcurrency::TWriterGuard guard(ControlSpinLock_);

    if (TerminateRequested_) {
        return;
    }

    LOG_DEBUG("Sending termination request");

    YCHECK(!error.IsOK());
    YCHECK(TerminateError_.IsOK());
    TerminateError_ = error;
    TerminateRequested_ = true;

    if (State_ != EState::Open) {
        LOG_TRACE("Cannot arm poller since connection is not open yet");
        return;
    }

    DoArmPoller();
}

void TTcpConnection::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Subscribe(callback);
}

void TTcpConnection::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Terminated_.Unsubscribe(callback);
}

void TTcpConnection::OnEvent(EPollControl control)
{
    do {
        TTryGuard<TSpinLock> guard(EventHandlerSpinLock_);
        if (!guard.WasAcquired()) {
            LOG_TRACE("Event handler is already running");
            return;
        }

        if (State_ == EState::Aborted || State_ == EState::Closed) {
            LOG_TRACE("Connection is already closed");
            return;
        }

        if (TerminateRequested_) {
            OnTerminated();
            return;
        }

        // For client sockets the first write notification means that
        // connection was established.
        // This is handled here to avoid race between arming in Send() and OnSocketConnected(). 
        if (Any(control & EPollControl::Write) &&
            ConnectionType_ == EConnectionType::Client &&
            State_ == EState::Opening)
        {
            Open();
        }

        LOG_TRACE("Event processing started");

        ProcessQueuedMessages();

        // NB: Try to read from the socket before writing into it to avoid
        // getting SIGPIPE when the other party closes the connection.
        if (Any(control & EPollControl::Read)) {
            OnSocketRead();
        }

        if (Any(control & EPollControl::Write)) {
            OnSocketWrite();
        }

        HasUnsentData_ = HasUnsentData();
        LOG_TRACE("Event processing finished (HasUnsentData: %v)", HasUnsentData_.load());
    } while (ArmedForQueuedMessages_);

    RearmPoller();
}

void TTcpConnection::OnShutdown()
{
    // Perform the initial cleanup (the final one will be in dtor).
    Cleanup();

    State_ = EState::Closed;

    LOG_DEBUG(CloseError_, "Connection terminated");

    Terminated_.Fire(CloseError_);
}

void TTcpConnection::OnSocketConnected(SOCKET socket)
{
    Y_ASSERT(State_ == EState::Opening);

    Socket_ = socket;

    // Check if connection was established successfully.
    int error = GetSocketError();
    if (error != 0) {
        Abort(TError(
            NBus::EErrorCode::TransportError,
            "Error connecting to %v",
            EndpointDescription_)
            << TError::FromSystem(error));
        return;
    }

    UpdateConnectionCount(true);

    {
        NConcurrency::TWriterGuard guard(ControlSpinLock_);

        auto tosLevel = TosLevel_.load();
        if (tosLevel != DefaultTosLevel) {
            InitSocketTosLevel(tosLevel);
        }

        DoArmPoller();
    }
}

void TTcpConnection::OnSocketRead()
{
    if (State_ == EState::Closed || State_ == EState::Aborted) {
        return;
    }

    LOG_TRACE("Started serving read request");
    size_t bytesReadTotal = 0;

    while (true) {
        // Check if the decoder is expecting a chunk of large enough size.
        auto decoderChunk = Decoder_.GetFragment();
        size_t decoderChunkSize = decoderChunk.Size();

        if (decoderChunkSize >= MinBatchReadSize) {
            // Read directly into the decoder buffer.
            size_t bytesToRead = std::min(decoderChunkSize, MaxBatchReadSize);
            LOG_TRACE("Reading from socket into decoder (BytesToRead: %v)",bytesToRead);

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
            LOG_TRACE("Reading from socket into buffer (BytesToRead: %v)", ReadBuffer_.Size());

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
                LOG_TRACE("Feeding buffer into decoder (DecoderNeededBytes: %v, RemainingBufferBytes: %v, BytesToCopy: %v)",
                    decoderChunkSize,
                    recvRemaining,
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

    LastIncompleteReadTime_ = HasUnreadData()
        ? NProfiling::GetCpuInstant()
        : std::numeric_limits<NProfiling::TCpuInstant>::max();

    LOG_TRACE("Finished serving read request (BytesReadTotal: %v)",
        bytesReadTotal);
}

bool TTcpConnection::HasUnreadData() const
{
    return Decoder_.IsInProgress();
}

bool TTcpConnection::ReadSocket(char* buffer, size_t size, size_t* bytesRead)
{
    NProfiling::TWallTimer timer;
    auto result = HandleEintr(recv, Socket_, buffer, size, 0);
    auto elapsed = timer.GetElapsedTime();
    if (elapsed > ReadTimeWarningThreshold) {
        LOG_DEBUG("Socket read took too long (Elapsed: %v)",
            elapsed);
    }

    if (!CheckReadError(result)) {
        *bytesRead = 0;
        return false;
    }

    *bytesRead = static_cast<size_t>(result);
    Counters_->InBytes.fetch_add(result, std::memory_order_relaxed);

    LOG_TRACE("Socket read (BytesRead: %v)", *bytesRead);

    if (Config_->EnableQuickAck) {
        if (!TrySetSocketEnableQuickAck(Socket_)) {
            LOG_TRACE("Failed to set socket quick ack option");
        }
    }

    return true;
}

bool TTcpConnection::CheckReadError(ssize_t result)
{
    if (result == 0) {
        Abort(TError(NBus::EErrorCode::TransportError, "Socket was closed"));
        return false;
    }

    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            Counters_->ReadErrors.fetch_add(1, std::memory_order_relaxed);
            Abort(TError(NBus::EErrorCode::TransportError, "Socket read error")
                << TError::FromSystem(error));
        }
        return false;
    }

    return true;
}

bool TTcpConnection::AdvanceDecoder(size_t size)
{
    if (!Decoder_.Advance(size)) {
        Counters_->DecoderErrors.fetch_add(1, std::memory_order_relaxed);
        Abort(TError(NBus::EErrorCode::TransportError, "Error decoding incoming packet"));
        return false;
    }

    if (Decoder_.IsFinished()) {
        bool result = OnPacketReceived();
        Decoder_.Restart();
        return result;
    }

    return true;
}

bool TTcpConnection::OnPacketReceived() noexcept
{
    Counters_->InPackets.fetch_add(1, std::memory_order_relaxed);
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
        Abort(TError(NBus::EErrorCode::TransportError, "Unexpected ack received"));
        return false;
    }

    auto& unackedMessage = UnackedMessages_.front();

    if (Decoder_.GetPacketId() != unackedMessage.PacketId) {
        Abort(TError(
            NBus::EErrorCode::TransportError,
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
        EnqueuePacket(EPacketType::Ack, EPacketFlags::None, 0, Decoder_.GetPacketId());
    }

    auto message = Decoder_.GrabMessage();
    Handler_->HandleMessage(std::move(message), this);

    return true;
}

TTcpConnection::TPacket* TTcpConnection::EnqueuePacket(
    EPacketType type,
    EPacketFlags flags,
    int checksummedPartCount,
    const TPacketId& packetId,
    TSharedRefArray message,
    size_t payloadSize)
{
    size_t packetSize = TPacketEncoder::GetPacketSize(type, message, payloadSize);
    auto* packet = QueuedPackets_.emplace(
        type,
        flags,
        checksummedPartCount,
        packetId,
        std::move(message),
        payloadSize,
        packetSize);
    UpdatePendingOut(+1, +packetSize);
    return packet;
}

void TTcpConnection::OnSocketWrite()
{
    if (State_ == EState::Closed || State_ == EState::Aborted) {
        return;
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

    LOG_TRACE("Finished serving write request (BytesWrittenTotal: %v)", bytesWrittenTotal);
}

bool TTcpConnection::HasUnsentData() const
{
    return !EncodedFragments_.empty() || !QueuedPackets_.empty() || !EncodedPackets_.empty();
}

bool TTcpConnection::WriteFragments(size_t* bytesWritten)
{
    LOG_TRACE("Writing fragments (EncodedFragments: %v)", EncodedFragments_.size());

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
        struct iovec item;
        item.iov_base = const_cast<char*>(fragment.Begin());
        item.iov_len = size;
        SendVector_.push_back(item);
        EncodedFragments_.move_forward(fragmentIt);
        bytesAvailable -= size;
    }

    NProfiling::TWallTimer timer;
    auto result = HandleEintr(::writev, Socket_, SendVector_.data(), SendVector_.size());
    auto elapsed = timer.GetElapsedTime();
    if (elapsed > WriteTimeWarningThreshold) {
        LOG_DEBUG("Socket write took too long (Elapsed: %v)",
            elapsed);
    }

    *bytesWritten = result >= 0 ? static_cast<size_t>(result) : 0;
    bool isOK = CheckWriteError(result);
    if (isOK) {
        Counters_->OutBytes.fetch_add(*bytesWritten, std::memory_order_relaxed);
        LOG_TRACE("Socket written (BytesWritten: %v)", *bytesWritten);
    }
    return isOK;
}

void TTcpConnection::FlushWrittenFragments(size_t bytesWritten)
{
    size_t bytesToFlush = bytesWritten;
    LOG_TRACE("Flushing fragments (BytesWritten: %v)", bytesWritten);

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
    LOG_TRACE("Flushing packets (BytesWritten: %v)", bytesWritten);

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
        EncodedPackets_.push(std::move(QueuedPackets_.front()));
        QueuedPackets_.pop();
        const auto& packet = EncodedPackets_.back();

        // Encode the packet.
        LOG_TRACE("Starting encoding packet (PacketId: %v)", packet.PacketId);

        bool encodeResult = Encoder_.Start(
            packet.Type,
            packet.Flags,
            GenerateChecksums_,
            packet.ChecksummedPartCount,
            packet.PacketId,
            packet.Message);
        if (!encodeResult) {
            Counters_->EncoderErrors.fetch_add(1, std::memory_order_relaxed);
            Abort(TError(NBus::EErrorCode::TransportError, "Error encoding outcoming packet"));
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

        EncodedPacketSizes_.push(packet.PacketSize);
        encodedSize += packet.PacketSize;

        LOG_TRACE("Finished encoding packet (PacketId: %v)", packet.PacketId);
    }

    flushCoalesced();

    return true;
}

bool TTcpConnection::CheckWriteError(ssize_t result)
{
    if (result < 0) {
        int error = LastSystemError();
        if (IsSocketError(error)) {
            Counters_->WriteErrors.fetch_add(1, std::memory_order_relaxed);
            Abort(TError(NBus::EErrorCode::TransportError, "Socket write error")
                << TError::FromSystem(error));
        }
        return false;
    }

    return true;
}

void TTcpConnection::OnPacketSent()
{
    const auto& packet = EncodedPackets_.front();
    switch (packet.Type) {
        case EPacketType::Ack:
            OnAckPacketSent(packet);
            break;

        case EPacketType::Message:
            OnMessagePacketSent(packet);
            break;

        default:
            Y_UNREACHABLE();
    }


    UpdatePendingOut(-1, -packet.PacketSize);
    Counters_->OutPackets.fetch_add(1, std::memory_order_relaxed);

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

    PendingOutPayloadBytes_.fetch_sub(packet.PayloadSize);
}

void TTcpConnection::OnTerminated()
{
    TError error;
    {
        TReaderGuard guard(ControlSpinLock_);
        error = TerminateError_;
    }

    LOG_DEBUG("Termination request received");

    Abort(error);
}

void TTcpConnection::ProcessQueuedMessages()
{
    ArmedForQueuedMessages_ = false;
    auto messages = QueuedMessages_.DequeueAll();

    for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
        auto& queuedMessage = *it;

        auto packetId = queuedMessage.PacketId;
        auto flags = queuedMessage.Options.TrackingLevel == EDeliveryTrackingLevel::Full
            ? EPacketFlags::RequestAck
            : EPacketFlags::None;

        auto* packet = EnqueuePacket(
            EPacketType::Message,
            flags,
            GenerateChecksums_ ? queuedMessage.Options.ChecksummedPartCount : 0,
            packetId,
            std::move(queuedMessage.Message),
            queuedMessage.PayloadSize);

        LOG_DEBUG("Outcoming message dequeued (PacketId: %v, PacketSize: %v, Flags: %v)",
            packetId,
            packet->PacketSize,
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

void TTcpConnection::UnregisterFromPoller()
{
    NConcurrency::TWriterGuard guard(ControlSpinLock_);

    if (Unregistered_) {
        return;
    }
    Unregistered_ = true;

    if (Socket_ != INVALID_SOCKET) {
        Poller_->Unarm(Socket_);
    }
    Poller_->Unregister(this);
}

void TTcpConnection::ArmPollerForWrite()
{
    if (State_ != EState::Open) {
        LOG_TRACE("Cannot arm poller since connection is not open yet");
        return;
    }

    // In case the connection is already open we kick-start processing by arming the poller.
    // ArmedForQueuedMessages_ is used to batch these arm calls.
    bool expected = false;
    if (!ArmedForQueuedMessages_.compare_exchange_strong(expected, true)) {
        LOG_TRACE("Poller is already armed");
        return;
    }

    {
        NConcurrency::TReaderGuard guard(ControlSpinLock_);
        DoArmPoller();
    }
}

void TTcpConnection::DoArmPoller()
{
    if (Unregistered_) {
        LOG_TRACE("Cannot arm poller since connection is unregistered");
        return;
    }

    if (Socket_ == INVALID_SOCKET) {
        LOG_TRACE("Cannot arm poller since socket is closed");
        return;
    }

    Poller_->Arm(Socket_, this, EPollControl::Read|EPollControl::Write);

    LOG_TRACE("Poller armed");
}

void TTcpConnection::RearmPoller()
{
    NConcurrency::TReaderGuard guard(ControlSpinLock_);

    if (Unregistered_) {
        LOG_TRACE("Cannot rearm poller since connection is unregistered");
        return;
    }

    if (Socket_ == INVALID_SOCKET) {
        LOG_TRACE("Cannot rearm poller since socket is closed");
        return;
    }

    auto mustArmForWrite = [&] {
        return HasUnsentData_.load() || ArmedForQueuedMessages_.load();
    };

    // This loop is to avoid race with #TTcpConnection::Send and to prevent
    // arming the poller in read-only mode in presence of queued messages or unsent data.
    bool forWrite;
    do {
        if (HasUnsentData_.load()) {
            LastIncompleteWriteTime_ = NProfiling::GetCpuInstant();
        } else {
            LastIncompleteWriteTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
        }

        forWrite = mustArmForWrite();
        Poller_->Arm(Socket_, this, EPollControl::Read | (forWrite ? EPollControl::Write : EPollControl::None));
        LOG_TRACE("Poller rearmed (ForWrite: %v)", forWrite);
    } while (!forWrite && mustArmForWrite());
}

int TTcpConnection::GetSocketError() const
{
    return NNet::GetSocketError(Socket_);
}

bool TTcpConnection::IsSocketError(ssize_t result)
{
    return
        result != EWOULDBLOCK &&
        result != EAGAIN &&
        result != EINPROGRESS;
}

void TTcpConnection::InitSocketTosLevel(TTosLevel tosLevel)
{
    if (TrySetSocketTosLevel(Socket_, tosLevel)) {
        LOG_DEBUG("Socket TOS level set (TosLevel: %x)",
            tosLevel);
    } else {
        LOG_DEBUG("Failed to set socket TOS level");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

