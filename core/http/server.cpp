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

namespace NYT {
namespace NHttp {

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
        const IPollerPtr& poller)
        : Config_(config)
        , Listener_(listener)
        , Poller_(poller)
    { }

    virtual void AddHandler(const TString& path, const IHttpHandlerPtr& handler) override
    {
        YCHECK(!Started_.load());
        Handlers_.Add(path, handler);
    }

    virtual const TNetworkAddress& GetAddress() const override
    {
        return Listener_->GetAddress();
    }

    virtual void Start() override
    {
        if (Started_) {
            return;
        }

        Started_ = true;
        MainLoopFuture_ = BIND(&TServer::MainLoop, MakeStrong(this))
            .AsyncVia(Poller_->GetInvoker())
            .Run();
    }

    virtual void Stop() override
    {
        if (!Started_) {
            return;
        }

        Started_ = false;
        MainLoopFuture_.Cancel();
        MainLoopFuture_.Reset();
    }

private:
    const TServerConfigPtr Config_;
    const IListenerPtr Listener_;
    const IPollerPtr Poller_;

    TFuture<void> MainLoopFuture_;

    std::atomic<int> ActiveClients_ = {0};

    std::atomic<bool> Started_ = {false};
    TRequestPathMatcher Handlers_;

    TMonotonicCounter ConnectionsAccepted_{"/connections_accepted"};
    TMonotonicCounter ConnectionsDropped_{"/connections_dropped"};


    void MainLoop()
    {
        LOG_INFO("Server started");
        auto logStop = Finally([] {
            LOG_INFO("Server stopped");
        });

        while (true) {
            auto client = WaitFor(Listener_->Accept())
                .ValueOrThrow();

            HttpProfiler.Increment(ConnectionsAccepted_);
            if (++ActiveClients_ >= Config_->MaxSimultaneousConnections) {
                HttpProfiler.Increment(ConnectionsDropped_);
                --ActiveClients_;
                LOG_WARNING("Server is over max active connection limit (RemoteAddress: %v)",
                    client->RemoteAddress());
                continue;
            }

            auto connectionId = TGuid::Create();
            LOG_DEBUG("Client accepted (ConnectionId: %v, RemoteAddress: %v, LocalAddress: %v)",
                connectionId,
                client->RemoteAddress(),
                client->LocalAddress());
            Poller_->GetInvoker()->Invoke(
                BIND(&TServer::HandleClient, MakeStrong(this), std::move(client), connectionId));
        }
    }

    bool HandleRequest(const THttpInputPtr& request, THttpOutputPtr& response)
    {
        response->SetStatus(EStatusCode::InternalServerError);

        bool closeResponse = true;
        try {
            if (!request->ReceiveHeaders()) {
                return false;
            }

            const auto& path = request->GetUrl().Path;

            LOG_DEBUG("Received HTTP request (ConnectionId: %v, RequestId: %v, Method: %v, Path: %v, L7ReqId: %v, L7RealIP: %v, UserAgent: %Qv)",
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
                handler->HandleRequest(request, response);

                LOG_DEBUG("Finished handling HTTP request (RequestId: %v)",
                    request->GetRequestId());
            } else {
                LOG_DEBUG("Missing HTTP handler for given URL (RequestId: %v, Path: %v)",
                    request->GetRequestId(),
                    path);

                response->SetStatus(EStatusCode::NotFound);
            }
        } catch (const std::exception& ex) {
            closeResponse = true;
            LOG_DEBUG(ex, "Error handling HTTP request (RequestId: %v)",
                request->GetRequestId());

            if (!response->IsHeadersFlushed()) {
                response->SetStatus(EStatusCode::InternalServerError);
            }
        }

        if (closeResponse) {
            auto responseResult = WaitFor(response->Close());
            if (!responseResult.IsOK()) {
                LOG_DEBUG(responseResult, "Error flushing HTTP response stream (RequestId: %v)",
                    request->GetRequestId());
            }
        }

        return true;
    }

    void HandleClient(const IConnectionPtr& connection, const TGuid connectionId)
    {
        auto finally = Finally([&] {
            --ActiveClients_;
        });

        auto request = New<THttpInput>(
            connection,
            connection->RemoteAddress(),
            Poller_->GetInvoker(),
            EMessageType::Request,
            Config_);

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
                LOG_DEBUG("Dropping HTTP connection (ConnectionId: %v, Reason: %Qv)",
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
            LOG_DEBUG("HTTP connection closed (ConnectionId: %v)",
                connectionId);
        } else {
            LOG_DEBUG(connectionResult, "Error closing HTTP connection (ConnectionId: %v)",
                connectionId);
        }
    }
};

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller)
{
    return New<TServer>(config, listener, poller);
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int i = 0;; ++i) {
        try {
            auto listener = CreateListener(address, poller);
            return New<TServer>(config, listener, poller);
        } catch (const std::exception& ex) {
            if (i + 1 == config->BindRetryCount) {
                throw;
            } else {
                LOG_ERROR(ex, "HTTP server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

IServerPtr CreateServer(int port, const IPollerPtr& poller)
{
    auto config = New<TServerConfig>();
    config->Port = port;
    return CreateServer(config, poller);
}

IServerPtr CreateServer(const TServerConfigPtr& config)
{
    auto poller = CreateThreadPoolPoller(1, "Http");
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

} // namespace NHttp
} // namespace NYT
