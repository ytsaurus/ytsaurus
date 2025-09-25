#pragma once

#include <yt/yt/orm/client/objects/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TClusterTag;

using NClient::NObjects::TMasterInstanceTag;
using NClient::NObjects::UndefinedMasterInstanceTag;

using NClient::NObjects::TTimestamp;
using NClient::NObjects::TTransactionId;

struct IBootstrap;

class TServiceBase;

DECLARE_REFCOUNTED_STRUCT(IYTConnector)
DECLARE_REFCOUNTED_STRUCT(IHealthCheck)
DECLARE_REFCOUNTED_CLASS(TEventLogger)

DECLARE_REFCOUNTED_STRUCT(TMasterConfigBase)
DECLARE_REFCOUNTED_STRUCT(TMasterBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TYTConnectorConfig)
DECLARE_REFCOUNTED_STRUCT(TEventLogManagerConfig)
DECLARE_REFCOUNTED_STRUCT(THealthCheckConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IServiceDirectory)

////////////////////////////////////////////////////////////////////////////////

}; // namespace NYT::NFusion
