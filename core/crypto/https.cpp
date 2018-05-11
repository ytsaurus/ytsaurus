#include "https.h"

#include "tls.h"

#include <yt/core/http/server.h>
#include <yt/core/http/client.h>
#include <yt/core/http/config.h>

#include <yt/core/http/private.h>

#include <yt/core/net/address.h>
#include <yt/core/net/config.h>

#include <yt/core/concurrency/poller.h>

namespace NYT {
namespace NCrypto {

using namespace NNet;
using namespace NHttp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateHttpsServer(
    const TSslContextPtr& sslContext,
    const TServerConfigPtr& config,
    const IPollerPtr& poller)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller);
    return CreateServer(config, tlsListener, poller);
}

IServerPtr CreateHttpsServer(
    const TSslContextPtr& sslContext,
    const IListenerPtr& listener,
    const IPollerPtr& poller)
{
    auto tlsListener = sslContext->CreateListener(listener, poller);
    return CreateServer(New<TServerConfig>(), tlsListener, poller);
}

IClientPtr CreateHttpsClient(
    const TSslContextPtr& sslContext,
    const TClientConfigPtr& config,
    const IPollerPtr& poller)
{
    auto tlsDialer = sslContext->CreateDialer(
        New<TDialerConfig>(),
        poller,
        HttpLogger);
    return CreateClient(config, tlsDialer, poller->GetInvoker());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT
