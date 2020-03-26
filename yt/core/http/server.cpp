#include "server.h"
#include "http.h"
#include "config.h"
#include "stream.h"
#include "private.h"
#include "helpers.h"

#include <yt/core/net/listener.h>
#include <yt/core/net/connection.h>

#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/misc/finally.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/tracing/trace_context.h>

namespace NYT::NHttp {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

static const auto& Logger = HttpLogger;

////////////////////////////////////////////////////////////////////////////////

TCallbackHandler::TCallbackHandler(TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
    : Handler_(std::move(handler))
{ }

void TCallbackHandler::HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    Handler_(req, rsp);
}

////////////////////////////////////////////////////////////////////////////////

void IServer::AddHandler(
    const TString& pattern,
    TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
{
    AddHandler(pattern, New<TCallbackHandler>(handler));
}

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        const TServerConfigPtr& config,
        const IListenerPtr& listener,
        const IPollerPtr& poller,
        const IPollerPtr& acceptor)
        : Config_(config)
        , Listener_(listener)
        , Poller_(poller)
        , Acceptor_(acceptor)
    { }

    virtual void AddHandler(const TString& path, const IHttpHandlerPtr& handler) override
    {
        YT_VERIFY(!Started_);
        Handlers_.Add(path, handler);
    }

    virtual const TNetworkAddress& GetAddress() const override
    {
        return Listener_->GetAddress();
    }

    virtual void Start() override
    {
        YT_VERIFY(!Started_);
        Started_ = true;

        YT_LOG_INFO("Server started");

        AsyncAcceptConnection();
    }

    virtual void Stop() override
    {
        Stopped_.store(true);

        YT_LOG_INFO("Server stopped");
    }

private:
    const TServerConfigPtr Config_;
    const IListenerPtr Listener_;
    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;

    bool Started_ = false;
    std::atomic<bool> Stopped_ = {false};

    TRequestPathMatcher Handlers_;

    TSimpleGauge ConnectionsActiveGauge_{"/connections_active"};
    TMonotonicCounter ConnectionsAcceptedCounter_{"/connections_accepted"};
    TMonotonicCounter ConnectionsDroppedCounter_{"/connections_dropped"};


    void AsyncAcceptConnection()
    {
        Listener_->Accept().Subscribe(
            BIND(&TServer::OnConnectionAccepted, MakeWeak(this))
                .Via(Acceptor_->GetInvoker()));
    }

    void OnConnectionAccepted(const TErrorOr<IConnectionPtr>& connectionOrError)
    {
        if (Stopped_.load()) {
            return;
        }

        AsyncAcceptConnection();

        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        auto connection = connectionOrError.ValueOrThrow();

        HttpProfiler.Increment(ConnectionsAcceptedCounter_);
        if (HttpProfiler.Increment(ConnectionsActiveGauge_, +1) >= Config_->MaxSimultaneousConnections) {
            HttpProfiler.Increment(ConnectionsDroppedCounter_);
            HttpProfiler.Increment(ConnectionsActiveGauge_, -1);
            YT_LOG_WARNING("Server is over max active connection limit (RemoteAddress: %v)",
                connection->RemoteAddress());
            return;
        }

        auto connectionId = TGuid::Create();
        YT_LOG_DEBUG("Connection accepted (ConnectionId: %v, RemoteAddress: %v, LocalAddress: %v)",
            connectionId,
            connection->RemoteAddress(),
            connection->LocalAddress());

        Poller_->GetInvoker()->Invoke(
            BIND(&TServer::HandleConnection, MakeStrong(this), std::move(connection), connectionId));
    }

    bool HandleRequest(const THttpInputPtr& request, const THttpOutputPtr& response)
    {
        response->SetStatus(EStatusCode::InternalServerError);

        bool closeResponse = true;
        try {
            if (!request->ReceiveHeaders()) {
                return false;
            }

            const auto& path = request->GetUrl().Path;

            YT_LOG_DEBUG("Received HTTP request (ConnectionId: %v, RequestId: %v, Method: %v, Path: %v, L7RequestId: %v, L7RealIP: %v, UserAgent: %v)",
                request->GetConnectionId(),
                request->GetRequestId(),
                request->GetMethod(),
                path,
                GetBalancerRequestId(request),
                GetBalancerRealIP(request),
                GetUserAgent(request));

            auto handler = Handlers_.Match(path);
            if (handler) {
                closeResponse = false;

                if (request->IsExpecting100Continue()) {
                    response->Flush100Continue();
                }

                auto trace = GetOrCreateTraceContext(request);
                NTracing::TTraceContextGuard guard(trace);
                NTracing::TTraceContextFinishGuard finishGuard(trace);
                SetTraceId(response, trace->GetTraceId());

                handler->HandleRequest(request, response);

                YT_LOG_DEBUG("Finished handling HTTP request (RequestId: %v)",
                    request->GetRequestId());
            } else {
                YT_LOG_DEBUG("Missing HTTP handler for given URL (RequestId: %v, Path: %v)",
                    request->GetRequestId(),
                    path);

                response->SetStatus(EStatusCode::NotFound);
            }
        } catch (const std::exception& ex) {
            closeResponse = true;
            YT_LOG_DEBUG(ex, "Error handling HTTP request (RequestId: %v)",
                request->GetRequestId());

            if (!response->IsHeadersFlushed()) {
                response->SetStatus(EStatusCode::InternalServerError);
            }
        }

        if (closeResponse) {
            auto responseResult = WaitFor(response->Close());
            if (!responseResult.IsOK()) {
                YT_LOG_DEBUG(responseResult, "Error flushing HTTP response stream (RequestId: %v)",
                    request->GetRequestId());
            }
        }

        return true;
    }

    void HandleConnection(const IConnectionPtr& connection, TGuid connectionId)
    {
        auto finally = Finally([&] {
            HttpProfiler.Increment(ConnectionsActiveGauge_, -1);
        });

        auto request = New<THttpInput>(
            connection,
            connection->RemoteAddress(),
            GetCurrentInvoker(),
            EMessageType::Request,
            Config_);

        if (Config_->IsHttps) {
            request->SetHttps();
        }

        auto response = New<THttpOutput>(
            connection,
            EMessageType::Response,
            Config_);

        request->SetConnectionId(connectionId);
        response->SetConnectionId(connectionId);

        while (true) {
            auto requestId = TGuid::Create();
            request->SetRequestId(requestId);
            response->SetRequestId(requestId);

            bool ok = HandleRequest(request, response);
            if (!ok) {
                break;
            }

            auto logDrop = [&] (auto reason) {
                YT_LOG_DEBUG("Dropping HTTP connection (ConnectionId: %v, Reason: %v)",
                    connectionId,
                    reason);
            };

            if (!Config_->EnableKeepAlive) {
                break;
            }

            // Arcadia decompressors might return eof earlier than
            // underlying stream. From HTTP server standpoint that
            // looks like request that wasn't fully consumed, even if
            // next Read() on that request would have returned eof.
            //
            // So we perform one last Read() here and check that
            // there is no data left inside stream.
            bool bodyConsumed = false;
            try {
                auto chunk = WaitFor(request->Read())
                    .ValueOrThrow();
                bodyConsumed = chunk.Empty();
            } catch (const std::exception& ) { }
            if (!bodyConsumed) {
                logDrop("Body is not fully consumed by the handler");
                break;
            }

            if (request->IsSafeToReuse()) {
                request->Reset();
            } else {
                logDrop("Request is not safe to reuse");
                break;
            }

            if (response->IsSafeToReuse()) {
                response->Reset();
            } else {
                logDrop("Response is not safe to reuse");
                break;
            }

            if (!connection->IsIdle()) {
                logDrop("Connection not idle");
                break;
            }
        }

        auto connectionResult = WaitFor(connection->Close());
        if (connectionResult.IsOK()) {
            YT_LOG_DEBUG("HTTP connection closed (ConnectionId: %v)",
                connectionId);
        } else {
            YT_LOG_DEBUG(connectionResult, "Error closing HTTP connection (ConnectionId: %v)",
                connectionId);
        }
    }
};

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller)
{
    return New<TServer>(config, listener, poller, poller);
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    return New<TServer>(config, listener, poller, acceptor);
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller, const IPollerPtr& acceptor)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int i = 0;; ++i) {
        try {
            auto listener = CreateListener(address, poller, acceptor, config->MaxBacklogSize);
            return New<TServer>(config, listener, poller, acceptor);
        } catch (const std::exception& ex) {
            if (i + 1 == config->BindRetryCount) {
                throw;
            } else {
                YT_LOG_ERROR(ex, "HTTP server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(config, poller, poller);
}

IServerPtr CreateServer(int port, const IPollerPtr& poller)
{
    auto config = New<TServerConfig>();
    config->Port = port;
    return CreateServer(config, poller);
}

IServerPtr CreateServer(const TServerConfigPtr& config)
{
    auto threadName = config->ServerName
        ? "Http:" + config->ServerName
        : "Http";
    auto poller = CreateThreadPoolPoller(1, threadName);
    return CreateServer(config, poller);
}

////////////////////////////////////////////////////////////////////////////////

void TRequestPathMatcher::Add(const TString& pattern, const IHttpHandlerPtr& handler)
{
    if (pattern.empty()) {
        THROW_ERROR_EXCEPTION("Empty pattern is invalid");
    }

    if (pattern.back() == '/') {
        Subtrees_[pattern] = handler;

        auto withoutSlash = pattern.substr(0, pattern.size() - 1);
        Subtrees_[withoutSlash] = handler;
    } else {
        Exact_[pattern] = handler;
    }
}

IHttpHandlerPtr TRequestPathMatcher::Match(TStringBuf path)
{
    {
        auto it = Exact_.find(path);
        if (it != Exact_.end()) {
            return it->second;
        }
    }

    while (true) {
        auto it = Subtrees_.find(path);
        if (it != Subtrees_.end()) {
            return it->second;
        }

        if (path.empty()) {
            break;
        }

        path.Chop(1);
        while (!path.empty() && path.back() != '/') {
            path.Chop(1);
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
