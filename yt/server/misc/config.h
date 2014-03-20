#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/misc/address.h>

#include <core/rpc/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual TYsonSerializable
{
public:
    TAddressResolverConfigPtr AddressResolver;
    NRpc::TServerConfigPtr RpcServer;

    TServerConfig()
    {
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("rpc_server", RpcServer)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
