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

#include <core/logging/tagged_logger.h>

#include <contrib/libev/ev++.h>

#ifndef _WIN32
    #include <sys/uio.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

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
    virtual TAsyncError Send(TSharedRefArray message, EDeliveryTrackingLevel level) override;
    virtual void Terminate(const TError& error) override;

    void SyncProcessEvent(EConnectionEvent event);

    DECLARE_SIGNAL(void(TError), Terminated);

private:
    struct TQueuedMessage
    {
        TQueuedMessage()
        { }

        TQueuedMessage(TSharedRefArray message, EDeliveryTrackingLevel level)
            : Promise(level != EDeliveryTrackingLevel::None ? NewPromise<TError>() : Null)
            , Message(std::move(message))
            , Level(level)
            , PacketId(TPacketId::Create())
        { }

        TAsyncErrorPromise Promise;
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

        TUnackedMessage(const TPacketId& packetId, TAsyncErrorPromise promise)
            : PacketId(packetId)
            , Promise(std::move(promise))
        { }

        TPacketId PacketId;
        TAsyncErrorPromise Promise;
    };

    DECLARE_ENUM(EState,
        (Resolving)
        (Opening)
        (Open)
        (Closed)
    );

    TTcpBusConfigPtr Config;
    TTcpDispatcherThreadPtr DispatcherThread;
    EConnectionType ConnectionType;
    ETcpInterfaceType InterfaceType;
    TConnectionId Id;
    int Socket;
    int Fd;
    Stroka Address;
#ifdef _linux_
    int Priority;
#endif
    IMessageHandlerPtr Handler;

    NLog::TTaggedLogger Logger;
    NProfiling::TProfiler Profiler;

    // Only used for client sockets.
    int Port;
    TFuture< TErrorOr<TNetworkAddress> > AsyncAddress;

    TAtomic State;

    TAtomic MessageEnqueuedSent;

    TSpinLock TerminationSpinLock;
    TError TerminationError;

    std::unique_ptr<ev::io> SocketWatcher;

    TPacketDecoder Decoder;
    TBlob ReadBuffer;

    TPromise<TError> TerminatedPromise;

    TMultipleProducerSingleConsumerLockFreeStack<TQueuedMessage> QueuedMessages;
    
    TRingQueue<TPacket*> QueuedPackets;
    TRingQueue<TPacket*> EncodedPackets;

    TPacketEncoder Encoder;
    std::vector<std::unique_ptr<TBlob>> WriteBuffers;
    TRingQueue<TRef> EncodedFragments;
    TRingQueue<size_t> EncodedPacketSizes;

#ifdef _WIN32
    std::vector<WSABUF> SendVector;
#else
    std::vector<struct iovec> SendVector;
#endif

    TRingQueue<TUnackedMessage> UnackedMessages;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

    void Cleanup();

    void SyncOpen();
    void SyncResolve();
    void SyncClose(const TError& error);

    void InitFd();
    void ConnectSocket(const TNetworkAddress& netAddress);
    void CloseSocket();

    void OnAddressResolved();
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
    void OnMessageEnqueued();
    void ProcessOutcomingMessages();
    void DiscardOutcomingMessages(const TError& error);
    void DiscardUnackedMessages(const TError& error);
    void UpdateSocketWatcher();

    void OnTerminated();

    TTcpDispatcherStatistics& Statistics();
    void UpdateConnectionCount(int delta);
    void UpdatePendingOut(int countDelta, i64 sizeDelta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
