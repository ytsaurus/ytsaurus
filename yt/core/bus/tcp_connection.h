#pragma once

#include "private.h"
#include "tcp_dispatcher_impl.h"
#include "bus.h"
#include "packet.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/address.h>
#include <core/misc/ring_queue.h>
#include <core/misc/lock_free.h>

#include <core/actions/future.h>

#include <core/logging/log.h>

#include <contrib/libev/ev++.h>

#ifndef _win_
    #include <sys/uio.h>
#endif

#include <atomic>

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
        ETcpInterfaceType interfaceType,
        const TConnectionId& id,
        int socket,
        const Stroka& address,
        int priority,
        IMessageHandlerPtr handler);

    ~TTcpConnection();

    const TConnectionId& GetId() const;

    // IEventLoopObject implementation.
    virtual void SyncInitialize() override;
    virtual void SyncFinalize() override;
    virtual Stroka GetLoggingId() const override;

    // IBus implementation.
    virtual NYTree::TYsonString GetEndpointDescription() const override;
    virtual TFuture<void> Send(TSharedRefArray message, EDeliveryTrackingLevel level) override;
    virtual void Terminate(const TError& error) override;

    DECLARE_SIGNAL(void(const TError&), Terminated);

private:
    struct TQueuedMessage
    {
        TQueuedMessage()
        { }

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
            i64 size)
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
        i64 Size;
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

    TTcpBusConfigPtr Config_;
    TTcpDispatcherThreadPtr DispatcherThread_;
    EConnectionType ConnectionType_;
    ETcpInterfaceType InterfaceType_;
    TConnectionId Id_;
    int Socket_;
    int Fd_;
    Stroka Address_;
#ifdef _linux_
    int Priority_;
#endif
    IMessageHandlerPtr Handler_;

    NLog::TLogger Logger;
    NProfiling::TProfiler Profiler;
    
    // Only used by client sockets.
    int Port_ = 0;

    // ETcpConnectionState actually, managed by GetState and SetState.
    std::atomic<ETcpConnectionState> State_;

    TClosure MessageEnqueuedCallback_;
    std::atomic<bool> MessageEnqueuedCallbackPending_;
    TMultipleProducerSingleConsumerLockFreeStack<TQueuedMessage> QueuedMessages_;

    TSpinLock TerminateSpinLock_;
    TError TerminateError_;
    TCallbackList<void(const TError&)> Terminated_;

    std::unique_ptr<ev::io> SocketWatcher_;

    TPacketDecoder Decoder_;
    TBlob ReadBuffer_;

    TRingQueue<TPacket*> QueuedPackets_;
    TRingQueue<TPacket*> EncodedPackets_;

    TPacketEncoder Encoder_;
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

    void InitFd();
    void InitSocketWatcher();
    
    void ConnectSocket(const TNetworkAddress& netAddress);
    void CloseSocket();

    void OnAddressResolutionFinished(TErrorOr<TNetworkAddress> result);
    void OnAddressResolved(const TNetworkAddress& netAddress);

    void OnSocket(ev::io&, int revents);

    int GetSocketError() const;
    bool IsSocketError(ssize_t result);

    void OnSocketRead();
    bool ReadSocket(char* buffer, size_t size, size_t* bytesRead);
    bool CheckReadError(ssize_t result);
    bool AdvanceDecoder(size_t size);
    bool OnPacketReceived();
    bool OnAckPacketReceived();
    bool OnMessagePacketReceived();

    void EnqueuePacket(
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

    TTcpDispatcherStatistics& Statistics();
    void UpdateConnectionCount(int delta);
    void UpdatePendingOut(int countDelta, i64 sizeDelta);

};

DEFINE_REFCOUNTED_TYPE(TTcpConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
