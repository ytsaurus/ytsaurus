#include "http_server.h"

#include <yt/core/actions/bind.h>
#include <yt/core/actions/future.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/id_generator.h>

#include <yt/core/profiling/profiler.h>

#include <library/http/misc/parsed_request.h>
#include <library/http/server/http.h>

namespace NYT {
namespace NXHttp {

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("HTTP");

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
            TParsedHttpRequest request(RequestString);

            LOG_DEBUG("Started serving HTTP request (Method: %v, Path: %v)",
                request.Method.ToString(),
                request.Request.ToString());

            // See http://www.w3.org/Protocols/rfc2616/rfc2616.html for HTTP RFC.

            if (request.Method != "GET") {
                Output() << FormatNotImplementedResponse();
                return true;
            }

            if (!request.Request.empty() && request.Request[0] == '/') {
                auto slashPosition = request.Request.find('/', 1);
                if (slashPosition == TString::npos) {
                    slashPosition = request.Request.length();
                }

                auto prefix = request.Request.substr(0, slashPosition).ToString();
                auto suffix = request.Request.substr(slashPosition).ToString();

                {
                    auto it = impl->SyncHandlers.find(prefix);
                    if (it != impl->SyncHandlers.end()) {
                        Output() << it->second.Run(suffix);
                        LOG_DEBUG("Request served");
                        return true;
                    }
                }

                {
                    auto it = impl->AsyncHandlers.find(prefix);
                    if (it != impl->AsyncHandlers.end()) {
                        Output() << it->second.Run(suffix)
                            .Get()
                            .ValueOrThrow();
                        LOG_DEBUG("Request served");
                        return true;
                    }
                }
            }

            LOG_WARNING("Cannot find a handler for HTTP request (Method; %v, Path: %v)",
                request.Method.ToString(),
                request.Request.ToString());
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
    typedef THashMap<TString, TSyncHandler> TSyncHandlerMap;
    typedef THashMap<TString, TAsyncHandler> TAsyncHandlerMap;

private:
    std::unique_ptr<TCallback> Callback;
    std::unique_ptr<THttpServer> Server;

    TSyncHandlerMap SyncHandlers;
    TAsyncHandlerMap AsyncHandlers;

public:
    TImpl(int port)
    {
        Callback.reset(new TCallback(*this));
        Server.reset(new THttpServer(
            Callback.get(),
            THttpServerOptions(static_cast<ui16>(port))));
    }

    TImpl(int port, int bindRetryCount, TDuration bindRetryBackoff)
    {
        Callback.reset(new TCallback(*this));
        auto options = THttpServerOptions(static_cast<ui16>(port));
#ifndef YT_IN_ARCADIA
        options.BindRetryCount = bindRetryCount;
        options.BindRetryBackoff = bindRetryBackoff;
#endif
        Server.reset(new THttpServer(Callback.get(), options));
    }

    void Start()
    {
        if (!Server->Start()) {
            THROW_ERROR_EXCEPTION("Failed to start HTTP server on port %v",
                Server->Options().Port)
                << TError::FromSystem(Server->GetErrorCode());
        }
    }

    void Stop()
    {
        Server->Stop();
    }

    void Register(const TString& prefix, TSyncHandler handler)
    {
        YCHECK(SyncHandlers.insert(std::make_pair(prefix, std::move(handler))).second);
    }

    void Register(const TString& prefix, TAsyncHandler handler)
    {
        YCHECK(AsyncHandlers.insert(std::make_pair(prefix, std::move(handler))).second);
    }
};

////////////////////////////////////////////////////////////////////////////////

TString FormatInternalServerErrorResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 500 Internal Server Error\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatNotImplementedResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 501 Not Implemented\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatBadGatewayResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 502 Bad Gateway\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatServiceUnavailableResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatGatewayTimeoutResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 504 Gateway Timeout\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatBadRequestResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 400 Bad Request\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatNotFoundResponse(const TString& body, const TString& type)
{
    return Format(
        "HTTP/1.1 404 Not Found\r\n"
        "Connection: close\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

TString FormatRedirectResponse(const TString& location)
{
    return Format(
        "HTTP/1.1 303 See Other\r\n"
        "Connection: close\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 0\r\n"
        "Location: %s\r\n"
        "\r\n",
        ~location);
}

TString FormatOKResponse(const TString& body, const TString& type)
{
    // TODO(sandello): Unify headers across all these methods; also implement CRYT-61.
    return Format(
        "HTTP/1.1 200 OK\r\n"
        "Server: YT\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Connection: close\r\n"
        "Cache-Control: no-cache, max-age=0\r\n"
        "Expires: Thu, 01 Jan 1970 00:00:01 GMT\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %" PRISZT "\r\n"
        "\r\n"
        "%s",
        ~type,
        body.length(),
        ~body);
}

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(int port)
    : Impl(new TImpl(port))
{ }

TServer::TServer(int port, int bindRetryCount, TDuration bindRetryBackoff)
    : Impl(new TImpl(port, bindRetryCount, bindRetryBackoff))
{ }

TServer::~TServer()
{ }

void TServer::Register(const TString& prefix, TSyncHandler handler)
{
    Impl->Register(prefix, handler);
}

void TServer::Register(const TString& prefix, TAsyncHandler handler)
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

} // namespace NXHttp
} // namespace NYT
