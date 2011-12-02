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

    TBusServer(int port, IMessageHandler* handler);

    virtual ~TBusServer();

    void Terminate();
    Stroka GetDebugInfo();

private:
    class TSession;
    struct TOutcomingResponse;

    friend class TSession;

    typedef yhash_map<TSessionId, TIntrusivePtr<TSession> > TSessionMap;
    typedef yhash_map<TGuid, TIntrusivePtr<TSession> > TPingMap;

    IMessageHandler::TPtr Handler;
    volatile bool Terminated;
    TIntrusivePtr<IRequester> Requester;
    TThread Thread;
    TSessionMap SessionMap;
    TPingMap PingMap;
    TLockFreeQueue< TIntrusivePtr<TSession> > SessionsWithPendingResponses;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    Event& GetEvent();

    bool ProcessIncomingNLRequests();
    void ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest);

    bool ProcessIncomingNLResponses();
    void ProcessIncomingNLResponse(TUdpHttpResponse* nlResponse);
    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse);

    void EnqueueOutcomingResponse(TSession* session, TOutcomingResponse* response);
    bool ProcessOutcomingResponses();
    void ProcessOutcomingResponse(TSession* session, TOutcomingResponse* response);

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse);
    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse);

    TIntrusivePtr<TSession> DoProcessMessage(
        TPacketHeader* header,
        const TGuid& requestId,
        const TUdpAddress& address,
        TBlob&& data,
        bool isRequest);

    TIntrusivePtr<TSession> RegisterSession(
        const TSessionId& sessionId,
        const TUdpAddress& clientAddress);
    void UnregisterSession(TIntrusivePtr<TSession> session);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
