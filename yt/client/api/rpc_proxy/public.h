#pragma once

#include <yt/client/api/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

extern const TString RpcProxiesPath;
extern const TString GrpcProxiesPath;
extern const TString DefaultProxyRole;
extern const TString BannedAttributeName;
extern const TString RoleAttributeName;
extern const TString AliveNodeName;
extern const TString ApiServiceName;
extern const TString DiscoveryServiceName;

////////////////////////////////////////////////////////////////////////////////

const int MaxInFlightModifyRowsRequestCount = 256;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((ProxyBanned) (2100))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
