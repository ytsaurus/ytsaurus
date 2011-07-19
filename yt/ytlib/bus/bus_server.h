#pragma once

#include "common.h"
#include "message.h"
#include "bus.h"
#include "packet.h"

#include <quality/NetLiba/UdpHttp.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TBusServer
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBusServer> TPtr;

    TBusServer(
        int port,
        IMessageHandler::TPtr handler);
    virtual ~TBusServer();

    void Terminate();
    Stroka GetDebugInfo();

private:
    class TSession;
    struct TReply;

    friend class TSession;

    typedef yhash_map<TSessionId, TIntrusivePtr<TSession>, TGuidHash> TSessionMap;
    typedef yhash_map<TGuid, TIntrusivePtr<TSession>, TGuidHash> TPingMap;

    IMessageHandler::TPtr Handler;
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
        const TGuid& requestId,
        const TUdpAddress& address,
        TBlob& data,
        bool isRequest);

    TIntrusivePtr<TSession> RegisterSession(
        const TSessionId& sessionId,
        const TUdpAddress& clientAddress);
    void UnregisterSession(TIntrusivePtr<TSession> session);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
