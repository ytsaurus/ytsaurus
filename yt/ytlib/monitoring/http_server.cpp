#include "stdafx.h"
#include "http_server.h"

#include <ytlib/misc/id_generator.h>

#include <ytlib/actions/bind.h>
#include <ytlib/actions/future.h>

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

#include <util/server/http.h>
#include <util/string/http.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("HTTP");

#if USE_NETLIBA_HTTP

////////////////////////////////////////////////////////////////////////////////

namespace {

static inline bool SendAndClose(SOCKET s, const Stroka& data)
{
    bool result = SendRetry(s, data.c_str(), data.size());
    if (result) {
        CloseConnection(s);
    }
    return result;
}

static inline bool SendAndClose(SOCKET s, const yvector<char>& data)
{
    bool result = SendRetry(s, &data[0], data.ysize());
    if (result) {
        // close connection here, because SendRetry closes socket only if failed
        CloseConnection(s);
    }
    return result;
}

static inline double SecondsSince(i64 start) {
    return NHPTimer::GetSeconds(GetCycleCount() - start);
}

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

class TServer::TImpl
{
private:
    typedef yhash_map<Stroka, TSyncHandler::TPtr> TSyncHandlerMap;
    typedef yhash_map<Stroka, TAsyncHandler::TPtr> TAsyncHandlerMap;

    struct TPendingRequest
    {
        i64 Id;
        SOCKET Socket;
        Stroka Path;
        i64 StartTime;
        TFuture<Stroka>::TPtr Future;
    };

private:
    int Port;

    int InputConnectionWaitTime;
    int MaxAsyncRequestTime;

    TSyncHandlerMap SyncHandlers;
    TAsyncHandlerMap AsyncHandlers;

public:
    TImpl(int port, int inputConnectionWaitTime, int maxAsyncRequestTime)
        : Port(port)
        , InputConnectionWaitTime(inputConnectionWaitTime)
        , MaxAsyncRequestTime(maxAsyncRequestTime)
        , Thread(ThreadFunc, (void*) this)
    { }

    void Run() const;
    void Start();
    void Stop();

    void Register(const Stroka& prefix, TSyncHandler* handler)
    {
        YVERIFY(SyncHandlers.insert(MakePair(prefix, handler)).second);
    }

    void Register(const Stroka& prefix, TAsyncHandler* handler)
    {
        YVERIFY(AsyncHandlers.insert(MakePair(prefix, handler)).second);
    }

private:
    void SendResponse(const TPendingRequest& request, const Stroka& result) const;

    bool HandlePendingRequest(TPendingRequest& request) const;
    bool HandleNewRequest(TPendingRequest& request) const;

private:
    TThread Thread;
    static void* ThreadFunc(void* param);
};

////////////////////////////////////////////////////////////////////////////////

void TServer::TImpl::Run() const
{
    TIdGenerator<i64> idGenerator;
    ylist<TPendingRequest> pendingRequests;
    TNLHttpServer httpServer(Port);

    for (;;) {
        THttpRequest httpRequest;
        SOCKET clientSocket = httpServer.Accept(&httpRequest, InputConnectionWaitTime);

        if (clientSocket == INVALID_SOCKET) {
            // This block of code is, in fact, kinda "on idle".
            auto it = pendingRequests.begin();
            while (it != pendingRequests.end()) {
                bool responseWasSent = HandlePendingRequest(*it);
                if (responseWasSent) {
                    auto itToBeErased = it;
                    ++it;
                    pendingRequests.erase(itToBeErased);
                }

                ++it;
            }
        } else {
            // This block handles all new incoming requests.
            TPendingRequest request;
            request.Id = idGenerator.Next();
            request.Socket = clientSocket;
            request.Path = httpRequest.GetUrl();
            request.StartTime = GetCycleCount();

            bool responseWasSent = HandleNewRequest(request);
            if (!responseWasSent) {
                pendingRequests.push_back(request);
            }
        }
    }    
}

bool TServer::TImpl::HandlePendingRequest(TPendingRequest& request) const
{
    Stroka result;
    double timeElapsed = SecondsSince(request.StartTime);

    if (request.Future->TryGet(&result)) {
        SendResponse(request, result);
        return true;
    }

    if (timeElapsed > MaxAsyncRequestTime) {
        LOG_WARNING("Timed out HTTP request (RequestId: %lld, Path: %s, TimeElapsed: %.3lfs)",
            request.Id,
            ~request.Path,
            timeElapsed);
        SendResponse(request, FormatGatewayTimeoutResponse());

        return true;
    }

    return false;
}

bool TServer::TImpl::HandleNewRequest(TPendingRequest& request) const
{
    LOG_INFO("Started to serve HTTP request (RequestId: %lld, Path: %s)",
        request.Id,
        ~request.Path);

    // See http://www.w3.org/Protocols/rfc2616/rfc2616.html for HTTP RFC.

    // TODO(sandello): Implement proper request parsing here.
    const Stroka& path = request.Path;
    if (!path.empty() && path[0] == '/') {
        auto slashPosition = path.find('/', 1);
        if (slashPosition == Stroka::npos) {
            slashPosition = path.length();
        }

        Stroka prefix = path.substr(0, slashPosition);
        Stroka suffix = path.substr(slashPosition);

        {
            auto it = SyncHandlers.find(prefix);
            if (it != SyncHandlers.end()) {
                Stroka result = it->second->Do(suffix);
                SendResponse(request, result);
                return true;
            }
        }

        {
            auto it = AsyncHandlers.find(prefix);
            if (it != AsyncHandlers.end()) {
                request.Future = it->second->Do(suffix);
                return false;
            }
        }
    }

    LOG_WARNING("Cannot find a handler for HTTP request (RequestId: %lld, Path: %s)",
        request.Id,
        ~request.Path);
    SendResponse(request, FormatNotFoundResponse());

    return true;
}

void TServer::TImpl::Start()
{
    LOG_INFO("Starting server on port %d", Port);

    Thread.Start();
    Thread.Detach();
}

void TServer::TImpl::Stop()
{
    // TODO(sandello): Implement proper stopping.
    LOG_INFO("Stopping server")
}

void TServer::TImpl::SendResponse(const TPendingRequest& request, const Stroka& result) const
{
    double timeElapsed = SecondsSince(request.StartTime);

    if (SendAndClose(request.Socket, result)) {
        LOG_INFO("Served HTTP request (RequestId: %lld, Path: %s, TimeElapsed: %.3lfs)",
            request.Id,
            ~request.Path,
            timeElapsed);
    } else {
        LOG_WARNING("Failed to send HTTP response (RequestId: %lld, Path: %s, TimeElapsed: %.3lfs)",
            request.Id,
            ~request.Path,
            timeElapsed);
    }
}

void* TServer::TImpl::ThreadFunc(void* param)
{
    auto* impl = (TServer::TImpl*) param;
    impl->Run();
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

#else // USE_NETLIBA_HTTP

////////////////////////////////////////////////////////////////////////////////

class TServer::TImpl
{
private:
    class TClient
        : public ::TClientRequest
    {
    public:
        TClient()
        { }

        virtual bool Reply(void* param)
        {
            auto impl = (TImpl*) param;
            TParsedHttpRequest request(Headers[0]);

            LOG_INFO("Started to serve HTTP request (Method: %s, Path: %s)",
                ~request.Method.ToString(),
                ~request.Request.ToString());

            // See http://www.w3.org/Protocols/rfc2616/rfc2616.html for HTTP RFC.

            if (request.Method != "GET") {
                Output() << FormatNotImplementedResponse();
                return true;
            }

            if (!request.Request.empty() && request.Request[0] == '/') {
                auto slashPosition = request.Request.find('/', 1);
                if (slashPosition == Stroka::npos) {
                    slashPosition = request.Request.length();
                }

                Stroka prefix = request.Request.substr(0, slashPosition).ToString();
                Stroka suffix = request.Request.substr(slashPosition).ToString();
                
                {
                    auto it = impl->SyncHandlers.find(prefix);
                    if (it != impl->SyncHandlers.end()) {
                        Output() << it->second.Run(suffix);
                        return true;
                    }
                }

                {
                    auto it = impl->AsyncHandlers.find(prefix);
                    if (it != impl->AsyncHandlers.end()) {
                        Output() << it->second.Run(suffix)->Get();
                        return true;
                    }
                }
            }

            LOG_WARNING("Cannot find a handler for HTTP request (Method; %s, Path: %s)",
                ~request.Method.ToString(),
                ~request.Request.ToString());
            Output() << FormatNotFoundResponse();
            return true;
        }
    };

    class TCallback
        : public THttpServer::ICallBack
    {
    public:
        TCallback(const TImpl& impl)
            : Impl(impl)
        { }

        virtual TClientRequest* CreateClient()
        {
            return new TClient();
        }

        virtual void* CreateThreadSpecificResource()
        {
            return (void*) &Impl;
        }
        
    private:
        const TImpl& Impl;
    };

private:
    typedef yhash_map<Stroka, TSyncHandler> TSyncHandlerMap;
    typedef yhash_map<Stroka, TAsyncHandler> TAsyncHandlerMap;

private:
    THolder<TCallback> Callback;
    THolder<THttpServer> Server;
    
    TSyncHandlerMap SyncHandlers;
    TAsyncHandlerMap AsyncHandlers;

public:
    TImpl(int port)
    {
        Callback.Reset(new TCallback(*this));
        Server.Reset(new THttpServer(~Callback,
            THttpServerOptions(static_cast<ui16>(port))
        ));
    }

    void Start()
    {
        Server->Start();
    }

    void Stop()
    {
        Server->Stop();
    }

    void Register(const Stroka& prefix, TSyncHandler handler)
    {
        YVERIFY(SyncHandlers.insert(MakePair(prefix, MoveRV(handler))).second);
    }

    void Register(const Stroka& prefix, TAsyncHandler handler)
    {
        YVERIFY(AsyncHandlers.insert(MakePair(prefix, MoveRV(handler))).second);
    }
};

////////////////////////////////////////////////////////////////////////////////

#endif // USE_NETLIBA_HTTP

Stroka FormatInternalServerErrorResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 500 Internal Server Error\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatNotImplementedResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 501 Not Implemented\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatBadGatewayResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 502 Bad Gateway\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatServiceUnavailableResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatGatewayTimeoutResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 504 Gateway Timeout\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatBadRequestResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 400 Bad Request\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatNotFoundResponse(const Stroka& body)
{
    return Sprintf(
        "HTTP/1.1 404 Not Found\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

Stroka FormatRedirectResponse(const Stroka& location)
{
    return Sprintf(
        "HTTP/1.1 303 See Other\r\n"
        "Connection: close\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 0\r\n"
        "Location: %s\r\n"
        "\r\n",
        ~location);
}

Stroka FormatOKResponse(const Stroka& body)
{
    // TODO(sandello): Unify headers across all these methods; also implement CRYT-61.
    return Sprintf(
        "HTTP/1.1 200 OK\r\n"
        "Server: YT\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Connection: close\r\n"
        "Cache-Control: no-cache, max-age=0\r\n"
        "Expires: Thu, 01 Jan 1970 00:00:01 GMT\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        body.length(),
        ~body);
}

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(int port)
    : Impl(new TImpl(port))
{ }

TServer::~TServer()
{ }

void TServer::Register(const Stroka& prefix, TSyncHandler handler)
{
    Impl->Register(prefix, handler);
}

void TServer::Register(const Stroka& prefix, TAsyncHandler handler)
{
    Impl->Register(prefix, handler);
}

void TServer::Start()
{
    Impl->Start();
}

void TServer::Stop()
{
    Impl->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
