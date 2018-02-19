#include "server.h"
#include "http.h"
#include "config.h"
#include "stream.h"
#include "private.h"

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

class TCallbackHandler
    : public IHttpHandler
{
public:
    explicit TCallbackHandler(TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
        : Handler_(handler)
    { }

    virtual void HandleHttp(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        Handler_(req, rsp);
    }

private:
    TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> Handler_;
};

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

    virtual TFuture<void> Start() override
    {
        Started_ = true;
        return BIND(&TServer::MainLoop, MakeStrong(this))
            .AsyncVia(Poller_->GetInvoker())
            .Run();
    }

private:
    const TServerConfigPtr Config_;
    const IListenerPtr Listener_;
    const IPollerPtr Poller_;

    std::atomic<int> ActiveClients_ = {0};

    std::atomic<bool> Started_ = {false};
    TRequestPathMatcher Handlers_;

    TSimpleCounter ConnectionsAccepted_{"/connections_accepted"};
    TSimpleCounter ConnectionsDropped_{"/connections_dropped"};


    void MainLoop()
    {
        LOG_INFO("Server started");
        auto logStop = Finally([] {
            LOG_INFO("Server stopped");
        });

        while (true) {
            auto client = WaitFor(Listener_->Accept()).ValueOrThrow();
            HttpProfiler.Increment(ConnectionsAccepted_);
            if (++ActiveClients_ >= Config_->MaxSimultaneousConnections) {
                HttpProfiler.Increment(ConnectionsDropped_);
                --ActiveClients_;
                LOG_WARNING("Server is over max active connection limit (RemoteAddress: %v)",
                    client->RemoteAddress());
                continue;
            }

            LOG_DEBUG("Accepted client (RemoteAddress: %v)", client->RemoteAddress());
            BIND(&TServer::HandleClient, MakeStrong(this), std::move(client))
                .AsyncVia(Poller_->GetInvoker())
                .Run();
        }
    }

    void HandleClient(const IConnectionPtr& connection)
    {
        auto finally = Finally([&] {
            --ActiveClients_;
        });

        auto request = New<THttpInput>(
            connection,
            Poller_->GetInvoker(),
            EMessageType::Request,
            Config_->ReadBufferSize);
        auto response = New<THttpOutput>(
            connection,
            EMessageType::Response,
            Config_->WriteBufferSize);

        response->SendConnectionCloseHeader();
        response->WriteHeaders(EStatusCode::InternalServerError);

        try {
            auto requestId = TGuid::Create();
            LOG_INFO("Received HTTP request (RequestId: %v, Method: %v, Path: %v)",
                requestId,
                request->GetMethod(),
                request->GetUrl().Path);

            auto handler = Handlers_.Match(request->GetUrl().Path);
            if (!handler) {
                response->WriteHeaders(EStatusCode::NotFound);
                WaitFor(response->Close()).ThrowOnError();
                return;
            }

            handler->HandleHttp(request, response);
            LOG_INFO("Finished handling HTTP request (RequestId: %v, Method: %v, Path: %v)",
                requestId,
                request->GetMethod(),
                request->GetUrl().Path);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error while handling HTTP request");

            if (!response->IsHeadersFlushed()) {
                response->WriteHeaders(EStatusCode::InternalServerError);
                WaitFor(response->Close()).ThrowOnError();
            }
        }

        WaitFor(connection->Close()).ThrowOnError();
        LOG_DEBUG("Client connection closed (RemoteAddress: %v)",
            connection->RemoteAddress());
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
