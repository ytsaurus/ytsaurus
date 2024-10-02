#pragma once

#include <yt/yt/orm/client/objects/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TClusterTag;

using NClient::NObjects::TMasterInstanceTag;
using NClient::NObjects::UndefinedMasterInstanceTag;

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(TYTConnector)
DECLARE_REFCOUNTED_CLASS(TEventLogger)

DECLARE_REFCOUNTED_CLASS(TMasterConfigBase)
DECLARE_REFCOUNTED_CLASS(TMasterConfig)
DECLARE_REFCOUNTED_CLASS(TMasterDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TRpcProxyCollocationConfig)
DECLARE_REFCOUNTED_CLASS(TYTConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TEventLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
