#include "client.h"
#include "http.h"
#include "config.h"
#include "stream.h"
#include "private.h"

#include <yt/core/net/dialer.h>
#include <yt/core/net/config.h>
#include <yt/core/net/connection.h>

#include <yt/core/concurrency/poller.h>

namespace NYT::NHttp {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        const TClientConfigPtr& config,
        const IDialerPtr& dialer,
        const IInvokerPtr& invoker)
        : Config_(config)
        , Dialer_(dialer)
        , Invoker_(invoker)
    { }

    virtual TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return WrapError(url, BIND([=, this_ = MakeStrong(this)] {
            THttpOutputPtr request;
            THttpInputPtr response;

            auto urlRef = ParseUrl(url);
            auto address = GetAddress(urlRef);
            std::tie(request, response) = OpenHttp(address);

            request->SetHost(urlRef.Host, urlRef.PortStr);
            if (headers) {
                request->SetHeaders(headers);
            }

            auto requestPath = Format("%v?%v", urlRef.Path, urlRef.RawQuery);
            request->WriteRequest(EMethod::Get, requestPath);
            WaitFor(request->Close())
                .ThrowOnError();

            // Waits for response headers internally.
            response->GetStatusCode();

            return IResponsePtr(response);
        }));
    }

    virtual TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return WrapError(url, BIND([=, this_ = MakeStrong(this)] {
            THttpOutputPtr request;
            THttpInputPtr response;

            auto urlRef = ParseUrl(url);
            auto address = GetAddress(urlRef);
            std::tie(request, response) = OpenHttp(address);

            request->SetHost(urlRef.Host, urlRef.PortStr);
            if (headers) {
                request->SetHeaders(headers);
            }

            auto requestPath = Format("%v?%v", urlRef.Path, urlRef.RawQuery);
            request->WriteRequest(EMethod::Post, requestPath);

            WaitFor(request->Write(body))
                .ThrowOnError();
            WaitFor(request->Close())
                .ThrowOnError();

            // Waits for response headers internally.
            response->GetStatusCode();

            return IResponsePtr(response);
        }));
    }
    
private:
    const TClientConfigPtr Config_;
    const IDialerPtr Dialer_;
    const IInvokerPtr Invoker_;

    TNetworkAddress GetAddress(const TUrlRef& parsedUrl)
    {
        constexpr int DefaultHttpPort = 80;

        auto host = parsedUrl.Host;
        TNetworkAddress address;

        auto tryIP = TNetworkAddress::TryParse(host);
        if (tryIP.IsOK()) {
            address = tryIP.Value();
        } else {
            auto asyncAddress = TAddressResolver::Get()->Resolve(ToString(host));
            address = WaitFor(asyncAddress)
                .ValueOrThrow();
        }

        return TNetworkAddress(address, parsedUrl.Port.value_or(DefaultHttpPort));
    }

    std::pair<THttpOutputPtr, THttpInputPtr> OpenHttp(const TNetworkAddress& address)
    {
        auto conn = WaitFor(Dialer_->Dial(address)).ValueOrThrow();
        auto input = New<THttpInput>(
            conn,
            address,
            Invoker_,
            EMessageType::Response,
            Config_);
        auto output = New<THttpOutput>(
            conn,
            EMessageType::Request,
            Config_);

        return std::make_pair(std::move(output), std::move(input));
    }

    TFuture<IResponsePtr> WrapError(const TString& url, TCallback<IResponsePtr()> action)
    {
        return BIND([=] {
            try {
                return action();
            } catch(const TErrorException& ex) {
                THROW_ERROR_EXCEPTION("HTTP request failed")
                    << TErrorAttribute("url", url)
                    << ex;
                throw;
            }
        })
            .AsyncVia(Invoker_)
            .Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IDialerPtr& dialer,
    const IInvokerPtr& invoker)
{
    return New<TClient>(config, dialer, invoker);
}

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IPollerPtr& poller)
{
    return CreateClient(
        config,
        CreateDialer(New<TDialerConfig>(), poller, HttpLogger),
        poller->GetInvoker());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
