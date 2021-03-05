#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IRowStreamEncoder)
DECLARE_REFCOUNTED_STRUCT(IRowStreamDecoder)

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

extern const TString RpcProxiesPath;
extern const TString GrpcProxiesPath;
extern const TString DefaultProxyRole;
extern const TString BannedAttributeName;
extern const TString RoleAttributeName;
extern const TString AliveNodeName;
extern const TString ApiServiceName;
extern const TString DiscoveryServiceName;

constexpr int CurrentWireFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(babenko): get rid of this in favor of NRpc::EErrorCode::PeerBanned
DEFINE_ENUM(EErrorCode,
    ((ProxyBanned) (2100))
);

DEFINE_ENUM(ERpcProxyFeature,
    ((GetInSyncWithoutKeys)(0))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
