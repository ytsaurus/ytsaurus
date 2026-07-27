#include "discovery.h"

#include <yt/yt/library/discovery_client/discovery.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NClickHouseServer {

using namespace NApi::NNative;
using namespace NDiscoveryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

TDiscoveryConnectionConfigPtr GetDiscoveryConnection(const IConnectionPtr& connection)
{
    auto connectionConfig = connection->GetConfig();
    if (!connectionConfig->DiscoveryConnection) {
        THROW_ERROR_EXCEPTION("Missing \"discovery_connection\" parameter in connection configuration");
    }
    return connectionConfig->DiscoveryConnection;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryFromNativeConnection(
    TDiscoveryConfigPtr config,
    IConnectionPtr connection,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<std::string> extraAttributes,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    return NDiscoveryClient::CreateDiscovery(
        std::move(config),
        GetDiscoveryConnection(connection),
        std::move(channelFactory),
        std::move(invoker),
        std::move(extraAttributes),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
