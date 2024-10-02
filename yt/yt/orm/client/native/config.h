#pragma once

#include "request.h"

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveBatchSizeOptionsConfig
    : public NYTree::TYsonStruct
    , public TAdaptiveBatchSizeOptions
{
public:
    TAdaptiveBatchSizeOptions ToPlainStruct() const;

    REGISTER_YSON_STRUCT(TAdaptiveBatchSizeOptionsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAdaptiveBatchSizeOptionsConfig);

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    // Connection should be configured through exactly one of the two following ways.

    // First option: balancer and static list of master addresses.

    // Balancer grpc address, which is used only to discover masters.
    // Connection to balancer is never used to serve user requests.
    std::optional<TString> DiscoveryAddress;

    // Balancer ip6 grpc address. If provided master connections are
    // forced to use ip6 addresses too.
    std::optional<TString> DiscoveryIP6Address;

    // Static list of master addresses. Used for both discovery and request.
    std::vector<TString> Addresses;

    // Second option: service discovery endpoints.

    // Provided endpoint sets are resolved into master addresses through
    // service discovery. Obtained addresses are used for future rediscovery.
    NRpc::TServiceDiscoveryEndpointsConfigPtr Endpoints;

    bool Secure;

    NRpc::TDynamicChannelPoolConfigPtr DynamicChannelPool;
    TDuration ChannelTtl;

    // Common options for all grpc channels.
    NRpc::NGrpc::TChannelConfigTemplatePtr GrpcChannelTemplate;
    NRpc::NGrpc::TChannelConfigPtr GrpcChannel;

    TDuration RequestTimeout;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);

protected:
    static void SetDefaultDomain(
        TRegistrar registrar,
        TStringBuf defaultDomain,
        ui16 defaultGrpcPort);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
