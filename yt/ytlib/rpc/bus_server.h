#pragma once

#include "common.h"
#include "message.h"
#include "bus.h"
#include "packet.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

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
    TLockFreeQueue< TIntrusivePtr<TSession> > PendingReplySessions;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    Event& GetEvent();

    bool ProcessNLRequests();
    void ProcessNLRequest(TUdpHttpRequest* nlRequest);

    bool ProcessNLResponses();
    void ProcessNLResponse(TUdpHttpResponse* nlResponse);
    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse);

    void EnqueueReply(TIntrusivePtr<TSession> session, TIntrusivePtr<TReply> reply);
    bool ProcessReplies();
    void ProcessReply(TIntrusivePtr<TSession> session, TIntrusivePtr<TReply> reply);

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse);
    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse);

    TIntrusivePtr<TSession> DoProcessMessage(
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

} // namespace NRpc
} // namespace NYT
