#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

extern const TString RpcProxiesPath;
extern const TString DefaultProxyRole;
extern const TString BannedAttributeName;
extern const TString RoleAttributeName;
extern const TString AliveNodeName;
extern const TString ApiServiceName;
extern const TString DiscoveryServiceName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
