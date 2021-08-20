#pragma once

#include "packet.h"
#include "dispatcher_impl.h"

#include <yt/yt/core/bus/private.h>
#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/concurrency/pollable_detail.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <util/network/init.h>

#include <atomic>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETcpConnectionState,
    (None)
    (Resolving)
    (Opening)
    (Open)
    (Closed)
    (Aborted)
);

DEFINE_ENUM(EPacketState,
    (Queued)
    (Encoded)
    (Canceled)
);


class TTcpConnection
    : public IBus
    , public NConcurrency::TPollableBase
{
public:
    TTcpConnection(
        TTcpBusConfigPtr config,
        EConnectionType connectionType,
        const TString& networkName,
        TConnectionId id,
        SOCKET socket,
        const TString& endpointDescription,
        const NYTree::IAttributeDictionary& endpointAttributes,
        const NNet::TNetworkAddress& endpointAddress,
        const std::optional<TString>& address,
        const std::optional<TString>& unixDomainSocketPath,
        IMessageHandlerPtr handler,
        NConcurrency::IPollerPtr poller);

    ~TTcpConnection();

    void Start();
    void CheckLiveness();

    TConnectionId GetId() const;

    // IPollable implementation.
    virtual const TString& GetLoggingTag() const override;
    virtual void OnEvent(NConcurrency::EPollControl control) override;
    virtual void OnShutdown() override;

    // IBus implementation.
    virtual const TString& GetEndpointDescription() const override;
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    virtual const NNet::TNetworkAddress& GetEndpointAddress() const override;
    virtual TTcpDispatcherStatistics GetStatistics() const override;
    virtual TFuture<void> GetReadyFuture() const override;
    virtual TFuture<void> Send(TSharedRefArray message, const TSendOptions& options) override;
    virtual void SetTosLevel(TTosLevel tosLevel) override;
    virtual void Terminate(const TError& error) override;

    DECLARE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

private:
    using EState = ETcpConnectionState;

    struct TQueuedMessage
    {
        TQueuedMessage() = default;

        TQueuedMessage(TSharedRefArray message, const TSendOptions& options)
            : Promise((options.TrackingLevel != EDeliveryTrackingLevel::None || options.EnableSendCancelation)
                ? NewPromise<void>()
                : std::nullopt)
            , Message(std::move(message))
            , PayloadSize(GetByteSize(Message))
            , Options(options)
            , PacketId(TPacketId::Create())
        { }

        TPromise<void> Promise;
        TSharedRefArray Message;
        size_t PayloadSize;
        TSendOptions Options;
        TPacketId PacketId;
    };

    struct TPacket final
    {
        TPacket(
            EPacketType type,
            EPacketFlags flags,
            int checksummedPartCount,
            TPacketId packetId,
            TSharedRefArray message,
            size_t payloadSize,
            size_t packetSize)
            : Type(type)
            , Flags(flags)
            , ChecksummedPartCount(checksummedPartCount)
            , PacketId(packetId)
            , Message(std::move(message))
            , PayloadSize(payloadSize)
            , PacketSize(packetSize)
        { }

        EPacketType Type;
        EPacketFlags Flags;
        int ChecksummedPartCount;
        TPacketId PacketId;

        TSharedRefArray Message;

        size_t PayloadSize;
        size_t PacketSize;

        std::atomic<EPacketState> State = EPacketState::Queued;
        TPromise<void> Promise;
        TTcpConnectionPtr Connection;

        bool MarkEncoded();
        void OnCancel(const TError& error);
        void EnableCancel(TTcpConnectionPtr connection);
    };

    using TPacketPtr = TIntrusivePtr<TPacket>;

    const TTcpBusConfigPtr Config_;
    const EConnectionType ConnectionType_;
    const TConnectionId Id_;
    const TString EndpointDescription_;
    const NYTree::IAttributeDictionaryPtr EndpointAttributes_;
    const NNet::TNetworkAddress EndpointAddress_;
    const std::optional<TString> Address_;
    const std::optional<TString> UnixDomainSocketPath_;
    const std::optional<TString> AbstractUnixDomainSocketName_;
    const IMessageHandlerPtr Handler_;
    const NConcurrency::IPollerPtr Poller_;

    const NLogging::TLogger Logger;
    const TString LoggingTag_;

    const TPromise<void> ReadyPromise_ = NewPromise<void>();

    TString NetworkName_;
    TTcpDispatcherCountersPtr Counters_;
    bool GenerateChecksums_ = true;

    // Only used by client sockets.
    int Port_ = 0;

    std::atomic<EState> State_ = EState::None;

    // Actually stores NConcurrency::EPollControl.
    std::atomic<ui64> PendingControl_ = static_cast<ui64>(NConcurrency::EPollControl::Offline);

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);

    SOCKET Socket_ = INVALID_SOCKET;

    TError CloseError_;

    NNet::IAsyncDialerSessionPtr DialerSession_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    TMpscStack<TQueuedMessage> QueuedMessages_;
    std::atomic<size_t> PendingOutPayloadBytes_ = 0;

    TPacketDecoder Decoder_;
    const NProfiling::TCpuDuration ReadStallTimeout_;
    std::atomic<NProfiling::TCpuInstant> LastIncompleteReadTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    TBlob ReadBuffer_;

    TRingQueue<TPacketPtr> QueuedPackets_;
    TRingQueue<TPacketPtr> EncodedPackets_;
    TRingQueue<TPacketPtr> UnackedPackets_;

    TPacketEncoder Encoder_;
    const NProfiling::TCpuDuration WriteStallTimeout_;
    std::atomic<NProfiling::TCpuInstant> LastIncompleteWriteTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    std::vector<std::unique_ptr<TBlob>> WriteBuffers_;
    TRingQueue<TRef> EncodedFragments_;
    TRingQueue<size_t> EncodedPacketSizes_;

    std::vector<struct iovec> SendVector_;

    std::atomic<TTosLevel> TosLevel_ = DefaultTosLevel;

    void Open();
    void Close();

    void ResolveAddress();
    void Abort(const TError& error);

    void InitBuffers();

    int GetSocketPort();

    void ConnectSocket(const NNet::TNetworkAddress& address);
    void OnDialerFinished(const TErrorOr<SOCKET>& socketOrError);

    void OnAddressResolveFinished(const TErrorOr<NNet::TNetworkAddress>& result);
    void OnAddressResolved(const NNet::TNetworkAddress& address);
    void SetupNetwork(const TString& networkName);

    int GetSocketError() const;
    bool IsSocketError(ssize_t result);

    void OnSocketRead();
    bool HasUnreadData() const;
    bool ReadSocket(char* buffer, size_t size, size_t* bytesRead);
    bool CheckReadError(ssize_t result);
    bool AdvanceDecoder(size_t size);
    bool OnPacketReceived() noexcept;
    bool OnAckPacketReceived();
    bool OnMessagePacketReceived();

    TPacket* EnqueuePacket(
        EPacketType type,
        EPacketFlags flags,
        int checksummedPartCount,
        TPacketId packetId,
        TSharedRefArray message = TSharedRefArray(),
        size_t payloadSize = 0);

    void OnSocketWrite();
    bool HasUnsentData() const;
    bool WriteFragments(size_t* bytesWritten);
    void FlushWrittenFragments(size_t bytesWritten);
    void FlushWrittenPackets(size_t bytesWritten);
    bool MaybeEncodeFragments();
    bool CheckWriteError(ssize_t result);
    void OnPacketSent();
    void OnAckPacketSent(const TPacket& packet);
    void OnMessagePacketSent(const TPacket& packet);
    void OnTerminate();
    void ProcessQueuedMessages();
    void DiscardOutcomingMessages(const TError& error);
    void DiscardUnackedMessages(const TError& error);

    void UpdateConnectionCount(int delta);
    void UpdatePendingOut(int countDelta, i64 sizeDelta);

    void InitSocketTosLevel(int tosLevel);
};

DEFINE_REFCOUNTED_TYPE(TTcpConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
