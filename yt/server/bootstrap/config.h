#pragma once

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
        Register("address_resolver", AddressResolver)
            .DefaultNew();
        Register("rpc_server", RpcServer)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
