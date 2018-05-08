#include "client.h"
#include "config.h"

#include <yt/core/http/client.h>
#include <yt/core/http/private.h>

#include <yt/core/net/config.h>
#include <yt/core/net/address.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/concurrency/poller.h>

namespace NYT {
namespace NHttps {

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    explicit TClient(IClientPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return Underlying_->Get(url, headers);
    }

    virtual TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return Underlying_->Post(url, body, headers);
    }

private:
    const IClientPtr Underlying_;
    // Initialize SSL.
    const NRpc::NGrpc::TGrpcLibraryLockPtr LibraryLock_ = NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();
};

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IPollerPtr& poller)
{
    auto sslContext =  New<TSslContext>();
    // configure

    auto tlsDialer = sslContext->CreateDialer(
        New<TDialerConfig>(),
        poller,
        HttpLogger);

    auto httpClient = NHttp::CreateClient(
        config,
        tlsDialer,
        poller->GetInvoker());

    return New<TClient>(std::move(httpClient));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT
