#pragma once

#include "private.h"
#include "bus.h"
#include "packet.h"
#include "tcp_dispatcher_impl.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/ring_queue.h>

#include <util/network/init.h>

#include <yt/contrib/libev/ev++.h>

#include <atomic>

#ifdef _unix_
    #include <sys/uio.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETcpConnectionState,
    (Resolving)
    (Opening)
    (Open)
    (Closed)
);

class TTcpConnection
    : public IBus
    , public IEventLoopObject
{
public:
    TTcpConnection(
        TTcpBusConfigPtr config,
        TTcpDispatcherThreadPtr dispatcherThread,
        EConnectionType connectionType,
        TNullable<ETcpInterfaceType> interfaceType,
        const TConnectionId& id,
        int socket,
        const TString& endpointDescription,
        const NYTree::IAttributeDictionary& endpointAttributes,
        const TNullable<TString>& address,
        const TNullable<TString>& unixDomainName,
        int priority,
        IMessageHandlerPtr handler);

    ~TTcpConnection();

    const TConnectionId& GetId() const;

    // IEventLoopObject implementation.
    virtual void SyncInitialize() override;
    virtual void SyncFinalize() override;
    virtual void SyncCheck() override;
    virtual TString GetLoggingId() const override;

    // IBus implementation.
    virtual const TString& GetEndpointDescription() const override;
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    virtual TFuture<void> Send(TSharedRefArray message, EDeliveryTrackingLevel level) override;
    virtual void Terminate(const TError& error) override;

    DECLARE_SIGNAL(void(const TError&), Terminated);

private:
    using EState = ETcpConnectionState;

    struct TQueuedMessage
    {
        TQueuedMessage() = default;

        TQueuedMessage(TSharedRefArray message, EDeliveryTrackingLevel level)
            : Promise(level != EDeliveryTrackingLevel::None ? NewPromise<void>() : Null)
            , Message(std::move(message))
            , Level(level)
            , PacketId(TPacketId::Create())
        { }

        TPromise<void> Promise;
        TSharedRefArray Message;
        EDeliveryTrackingLevel Level;
        TPacketId PacketId;
    };

    struct TPacket
    {
        TPacket(
            EPacketType type,
            EPacketFlags flags,
            const TPacketId& packetId,
            TSharedRefArray message,
            size_t size)
            : Type(type)
            , Flags(flags)
            , PacketId(packetId)
            , Message(std::move(message))
            , Size(size)
        { }

        EPacketType Type;
        EPacketFlags Flags;
        TPacketId PacketId;
        TSharedRefArray Message;
        size_t Size;
    };

    struct TUnackedMessage
    {
        TUnackedMessage()
        { }

        TUnackedMessage(const TPacketId& packetId, TPromise<void> promise)
            : PacketId(packetId)
            , Promise(std::move(promise))
        { }

        TPacketId PacketId;
        TPromise<void> Promise;
    };

    const TTcpBusConfigPtr Config_;
    const TTcpDispatcherThreadPtr DispatcherThread_;
    const EConnectionType ConnectionType_;
    const TConnectionId Id_;
    int Socket_;
    const TString EndpointDescription_;
    const std::unique_ptr<NYTree::IAttributeDictionary> EndpointAttributes_;
    const TNullable<TString> Address_;
    const TNullable<TString> UnixDomainName_;
#ifdef _linux_
    const int Priority_;
#endif
    const IMessageHandlerPtr Handler_;

    NLogging::TLogger Logger;

    int FD_ = INVALID_SOCKET;

    TNullable<ETcpInterfaceType> InterfaceType_;
    TTcpDispatcherStatistics* Statistics_ = nullptr;
    bool EnableChecksums_ = true;
    bool ConnectionCounterUpdated_ = false;

    // Only used by client sockets.
    int Port_ = 0;

    std::atomic<EState> State_;

    const TClosure MessageEnqueuedCallback_;
    std::atomic<bool> MessageEnqueuedCallbackPending_ = {false};
    TMultipleProducerSingleConsumerLockFreeStack<TQueuedMessage> QueuedMessages_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    std::unique_ptr<ev::io> SocketWatcher_;

    TPacketDecoder Decoder_;
    NProfiling::TCpuDuration ReadStallTimeout_;
    NProfiling::TCpuInstant LastReadTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    TBlob ReadBuffer_;

    TRingQueue<TPacket*> QueuedPackets_;
    TRingQueue<TPacket*> EncodedPackets_;

    TPacketEncoder Encoder_;
    NProfiling::TCpuDuration WriteStallTimeout_;
    NProfiling::TCpuInstant LastWriteScheduleTime_ = std::numeric_limits<NProfiling::TCpuInstant>::max();
    std::vector<std::unique_ptr<TBlob>> WriteBuffers_;
    TRingQueue<TRef> EncodedFragments_;
    TRingQueue<size_t> EncodedPacketSizes_;

#ifdef _WIN32
    std::vector<WSABUF> SendVector_;
#else
    std::vector<struct iovec> SendVector_;
#endif

    TRingQueue<TUnackedMessage> UnackedMessages_;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

    void Cleanup();

    void SyncOpen();
    void SyncResolve();
    void SyncClose(const TError& error);

    void InitBuffers();
    void InitFD();
    void InitSocketWatcher();

    int GetSocketPort();

    void ConnectSocket(const TNetworkAddress& netAddress);
    void CloseSocket();

    void OnAddressResolutionFinished(const TErrorOr<TNetworkAddress>& result);
    void OnAddressResolved(TNetworkAddress address);
    void OnInterfaceTypeEstablished(ETcpInterfaceType interfaceType);

    void OnSocket(ev::io&, int revents);

    int GetSocketError() const;
    bool IsSocketError(ssize_t result);

    void OnSocketRead();
    bool HasUnreadData() const;
    bool ReadSocket(char* buffer, size_t size, size_t* bytesRead);
    bool CheckReadError(ssize_t result);
    bool AdvanceDecoder(size_t size);
    bool OnPacketReceived() throw();
    bool OnAckPacketReceived();
    bool OnMessagePacketReceived();

    TPacket* EnqueuePacket(
        EPacketType type,
        EPacketFlags flags,
        const TPacketId& packetId,
        TSharedRefArray message = TSharedRefArray());
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
    static void OnMessageEnqueuedThunk(const TWeakPtr<TTcpConnection>& weakConnection);
    void OnMessageEnqueued();
    void ProcessOutcomingMessages();
    void DiscardOutcomingMessages(const TError& error);
    void DiscardUnackedMessages(const TError& error);
    void UpdateSocketWatcher();

    void OnTerminated(const TError& error);

    void UpdateConnectionCount(int delta);
    void UpdatePendingOut(int countDelta, i64 sizeDelta);

};

DEFINE_REFCOUNTED_TYPE(TTcpConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
