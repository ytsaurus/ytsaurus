#pragma once

#include "private.h"
#include "tcp_dispatcher_impl.h"
#include "bus.h"
#include "packet.h"
#include "message.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/actions/future.h>

#include <queue>
#include <deque>

#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <contrib/libuv/src/unix/ev/ev++.h>

#ifndef _WIN32
#include <sys/uio.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EConnectionType,
    (Client)
    (Server)
);

class TTcpConnection
    : public IBus
    , public IEventLoopObject
{
public:
    TTcpConnection(
        EConnectionType type,
        const TConnectionId& id,
        int socket,
        const Stroka& address,
        IMessageHandlerPtr handler);

    ~TTcpConnection();

    const TConnectionId& GetId() const;

    // IEventLoopObject implementation.
    virtual void SyncInitialize();
    virtual void SyncFinalize();
    virtual Stroka GetLoggingId() const;

    // IBus implementation.
    virtual TSendResult Send(IMessagePtr message);
    virtual void Terminate(const TError& error);

    DECLARE_SIGNAL(void(TError), Terminated);

private:
    TPromise<TError> TerminatedPromise;

    struct TQueuedMessage
    {
        TQueuedMessage()
            : Promise(Null)
        { }

        explicit TQueuedMessage(IMessagePtr message)
            : Promise(NewPromise<ESendResult>())
            , Message(MoveRV(message))
            , PacketId(TPacketId::Create())
        { }

        IBus::TSendPromise Promise;
        IMessagePtr Message;
        TPacketId PacketId;
    };

    struct TQueuedPacket
    {
        TQueuedPacket(EPacketType type, const TPacketId& packetId, IMessagePtr message, i64 size)
            : Type(type)
            , PacketId(packetId)
            , Message(MoveRV(message))
            , Size(size)
        { }

        EPacketType Type;
        TPacketId PacketId;
        IMessagePtr Message;
        i64 Size;
    };

    struct TUnackedMessage
    {
        TUnackedMessage()
            : Promise(Null)
        { }

        TUnackedMessage(const TPacketId& packetId, IBus::TSendPromise promise)
            : PacketId(packetId)
            , Promise(MoveRV(promise))
        { }

        TPacketId PacketId;
        IBus::TSendPromise Promise;
    };

    struct TEncodedPacket
    {
        TPacketEncoder Encoder;
        TQueuedPacket* Packet;
    };

    struct TEncodedFragment
    {
        TRef Data;
        bool IsLastInPacket;
    };

    DECLARE_ENUM(EState,
        (Opening)
        (Open)
        (Closed)
    );

    
    EConnectionType Type;
    TConnectionId Id;
    int Socket;
    int Fd;
    Stroka Address;
    IMessageHandlerPtr Handler;

    TSpinLock SpinLock;
    EState State;
    TError TerminationError;

    THolder<ev::async> TerminationWatcher;
    THolder<ev::io> SocketWatcher;

    TBlob ReadBuffer;
    TPacketDecoder Decoder;

    TLockFreeQueue<TQueuedMessage> QueuedMessages;
    std::queue<TQueuedPacket*> QueuedPackets;
    std::queue<TEncodedPacket*> EncodedPackets;
    std::deque<TEncodedFragment> EncodedFragments;
#ifdef _WIN32
    std::vector<WSABUF> SendVector;
#else
    std::vector<struct iovec> SendVector;
#endif

    std::queue<TUnackedMessage> UnackedMessages;
    THolder<ev::async> OutcomingMessageWatcher;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

    void Cleanup();

    void SyncOpen();
    void SyncClose(const TError& error);
    void CloseSocket();

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

    void EnqueuePacket(EPacketType type, const TPacketId& packetId, IMessagePtr message = NULL);
    void OnSocketWrite();
    bool HasUnsentData() const;
    bool WriteFragments(size_t* bytesWritten);
    void FlushWrittenFragments(size_t bytesWritten);
    bool EncodeMoreFragments();
    bool CheckWriteError(ssize_t result);
    void OnPacketSent();
    void OnAckPacketSent(const TEncodedPacket& packet);
    void OnMessagePacketSent(const TEncodedPacket& packet);
    void OnOutcomingMessage(ev::async&, int);
    void UpdateSocketWatcher();

    void OnTerminated(ev::async&, int);

    TTcpDispatcherStatistics& Statistics();
    void UpdateConnectionCount(int delta);
    void UpdatePendingOut(int countDelta, i64 sizeDelta);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
