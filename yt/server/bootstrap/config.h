#pragma once

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/misc/address.h>

#include <ytlib/rpc/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public TYsonSerializable
{
    TAddressResolverConfigPtr AddressResolver;
    NRpc::TServerConfigPtr RpcServer;

    TServerConfig()
    {
        Register("address_resolver", AddressResolver)
            .DefaultNew();
        Register("rpc_server", RpcServer)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
