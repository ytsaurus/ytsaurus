#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/misc/address.h>

#include <ytlib/rpc/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TYsonSerializable
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
