#include "mock_http_server.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NTests {

////////////////////////////////////////////////////////////////////////////////

TString HttpResponse(int code, TString body)
{
    TString result;
    result += "HTTP/1.1 " + ToString(code) + " ";
    switch (code) {
        case 200: result += "Found"; break;
        case 404: result += "Not Found"; break;
        case 500: result += "Internal Server Error"; break;
        default: YT_ABORT();
    }
    result += "\r\n";
    result += "Connection: close\r\n";
    result += "Content-Length: " + ToString(body.length()) + "\r\n";
    result += "\r\n";
    result += body;
    return result;
}

TString CollectMessages(const TError& error)
{
    TString result;
    std::function<void(const TError&)> impl = [&] (const TError& e) {
        result += e.GetMessage();
        for (const auto& ie : e.InnerErrors()) {
            result += "\n";
            impl(ie);
        }
    };
    impl(error);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TMockHttpServer::SetCallback(TCallback callback)
{
    Callback_ = std::move(callback);
    if (ServerImpl_) {
        ServerImpl_->SetCallback(Callback_);
    }
}

void TMockHttpServer::Start()
{
    YT_VERIFY(!IsStarted());

    ServerImpl_ = std::make_unique<THttpServerImpl>();
    ServerImpl_->SetCallback(Callback_);

    Port_ = NTesting::GetFreePort();

    Server_ = std::make_unique<THttpServer>(
        ServerImpl_.get(),
        THttpServerOptions()
            .SetHost("localhost")
            .SetPort(Port_));
    Server_->Start();
}

void TMockHttpServer::Stop()
{
    YT_VERIFY(IsStarted());

    Server_->Stop();
    Server_.reset();

    ServerImpl_.reset();
}

bool TMockHttpServer::IsStarted() const
{
    return Server_.operator bool();
}

TString TMockHttpServer::GetHost() const
{
    return Server_->Options().Host;
}

int TMockHttpServer::GetPort() const
{
    return Server_->Options().Port;
}

////////////////////////////////////////////////////////////////////////////////

TMockHttpServer::TCallback TMockHttpServer::THttpServerImpl::GetCallback()
{
    auto guard = Guard(Lock_);
    return Callback_;
}

void TMockHttpServer::THttpServerImpl::SetCallback(TCallback callback)
{
    auto guard = Guard(Lock_);
    Callback_ = std::move(callback);
}

TClientRequest* TMockHttpServer::THttpServerImpl::CreateClient()
{
    return new TRequest(this);
}

////////////////////////////////////////////////////////////////////////////////

TMockHttpServer::THttpServerImpl::TRequest::TRequest(TMockHttpServer::THttpServerImpl* owner)
    : Owner_(owner)
{ }

bool TMockHttpServer::THttpServerImpl::TRequest::Reply(void* /*opaque*/)
{
    TCallback callback;
    if (Owner_) {
        callback = Owner_->GetCallback();
    }
    if (!callback) {
        Output() << "HTTP/1.0 501 Not Implemented\r\n\r\n";
    } else {
        callback(this);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTests
