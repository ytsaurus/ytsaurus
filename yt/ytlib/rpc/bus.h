#pragma once

#include "common.h"
#include "message.h"

#include "../misc/lease_manager.h"

#include <util/system/thread.h>
#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IBus
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBus> TPtr;

    enum ESendResult
    {
        OK,
        Failed
    };

    typedef TAsyncResult<ESendResult> TSendResult;

    virtual ~IBus() {}
    virtual TSendResult::TPtr Send(IMessage::TPtr message) = 0;
    virtual void Terminate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMessageHandler
{
    virtual ~IMessageHandler() {}
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TPacketHeader;

class TBusServer
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBusServer> TPtr;

    TBusServer(
        int port,
        IMessageHandler* handler);
    virtual ~TBusServer();
    
    void Terminate();
    Stroka GetDebugInfo();

private:
    class TReplyBus;
    class TSession;
    struct TReply;

    friend class TSession;

    typedef yhash_map<TSessionId, TIntrusivePtr<TSession>, TGUIDHash> TSessionMap;
    typedef yhash_map<TGUID, TIntrusivePtr<TSession>, TGUIDHash> TPingMap;

    IMessageHandler* Handler;
    volatile bool Terminated;
    TIntrusivePtr<IRequester> Requester;
    TThread Thread;
    TSessionMap SessionMap;
    TPingMap PingMap;
    TLockFreeQueue< TIntrusivePtr<TReply> > ReplyQueue;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    Event& GetEvent();
    
    bool ProcessNLRequests();
    void ProcessNLRequest(TUdpHttpRequest* nlRequest);

    bool ProcessNLResponses();
    void ProcessNLResponse(TUdpHttpResponse* nlResponse);
    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse);

    void EnqueueReply(TIntrusivePtr<TReply> reply);
    bool ProcessReplies();
    void ProcessReply(TIntrusivePtr<TReply> reply);

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse);
    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse);

    void DoProcessMessage(
        TPacketHeader* header,
        const TGUID& requestId,
        const TUdpAddress& address,
        TBlob& data,
        bool isRequest);

    TIntrusivePtr<TSession> RegisterSession(
        const TSessionId& sessionId,
        const TUdpAddress& clientAddress);
    void UnregisterSession(TIntrusivePtr<TSession> session);
};

////////////////////////////////////////////////////////////////////////////////

class TClientDispatcher;

class TBusClient
    : public TThrRefBase
{
public:
    typedef TIntrusivePtr<TBusClient> TPtr;

    TBusClient(Stroka address);

    IBus::TPtr CreateBus(IMessageHandler* handler);

private:
    class TBus;
    friend class TClientDispatcher;

    TUdpAddress ServerAddress;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
