#include "server.h"
#include "config.h"

#include <yt/core/http/server.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/net/address.h>

#include <yt/core/concurrency/poller.h>

namespace NYT {
namespace NHttps {

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    explicit TServer(IServerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual void AddHandler(
        const TString& pattern,
        const IHttpHandlerPtr& handler) override
    {
        Underlying_->AddHandler(pattern, handler);
    }

    //! Starts the server.
    virtual void Start() override
    {
        Underlying_->Start();
    }

    //! Stops the server.
    virtual void Stop() override
    {
        Underlying_->Stop();
    }

private:
    const IServerPtr Underlying_;
    // Initialize SSL.
    const NRpc::NGrpc::TGrpcLibraryLockPtr LibraryLock_ = NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();
};

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IPollerPtr& poller)
{
    auto sslContext =  New<TSslContext>();
    // configure

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller);

    auto httpServer = NHttp::CreateServer(config, tlsListener, poller);

    return New<TServer>(std::move(httpServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT
