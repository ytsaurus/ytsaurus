// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "connection.h"

#include "config.h"
#include "discovery_service_proxy.h"
#include "object_service_proxy.h"
#include "private.h"

#include <yt/yt/orm/client/native/connection_impl.h>

namespace NYT::NOrm::NExample::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NNative::IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return NYT::NOrm::NClient::NNative::CreateConnection<TObjectServiceProxy, TDiscoveryServiceProxy>(
        std::move(config),
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NNative
