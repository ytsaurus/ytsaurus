#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_DISCOVERY_SERVICE_PROTO_MODULE_FROM_NAMESPACE(ns) \
    struct TDiscoveryServiceProtoModule \
    { \
        using TReqGetMasters = ns::TReqGetMasters; \
        using TRspGetMasters = ns::TRspGetMasters; \
    };

////////////////////////////////////////////////////////////////////////////////

//! Discovery service proto module incorporates all required proto message types.
//! Master info addresses converter converts master info addresses from the ORM to the protobuf format.
template <
    class TDiscoveryServiceProtoModule,
    class TMasterInfoAddressesConverter>
NRpc::IServicePtr CreateDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    TString interfaceName);

////////////////////////////////////////////////////////////////////////////////

template <class TDiscoveryServiceProtoModule>
NRpc::IServicePtr CreateClientDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor);

template <class TDiscoveryServiceProtoModule>
NRpc::IServicePtr CreateSecureClientDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi

#define DISCOVERY_SERVICE_INL_H_
#include "discovery_service-inl.h"
#undef DISCOVERY_SERVICE_INL_H_
